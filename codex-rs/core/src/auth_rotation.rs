use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use codex_api::SharedAuthProvider;
use codex_login::AuthCredentialsStoreMode;
use codex_login::AuthDotJson;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_login::save_auth;
use codex_model_provider::auth_provider_from_auth;
use codex_model_provider_info::ModelProviderInfo;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use reqwest::StatusCode;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::OnceCell;

pub(crate) const CODEX_AUTH_ROTATION_JSON_ENV: &str = "CODEX_AUTH_ROTATION_JSON";
const COORDINATOR_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug)]
pub(crate) struct AuthRotation {
    accounts: Vec<AuthRotationAccount>,
    rotation_id: String,
    coordinator_url: String,
    coordinator_token: String,
    http_client: reqwest::Client,
    chatgpt_base_url: Option<String>,
}

#[derive(Debug)]
struct AuthRotationAccount {
    home: PathBuf,
    manager: OnceCell<Arc<AuthManager>>,
}

#[derive(Clone)]
pub(crate) struct AuthRotationClientSetup {
    pub(crate) account_index: usize,
    pub(crate) generation: u64,
    pub(crate) auth: CodexAuth,
    pub(crate) api_provider: codex_api::Provider,
    pub(crate) api_auth: SharedAuthProvider,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub(crate) struct AuthRotationState {
    pub(crate) rotation_id: String,
    pub(crate) account_count: usize,
    pub(crate) active_index: usize,
    pub(crate) generation: u64,
}

#[derive(Debug, Deserialize)]
struct AuthRotationEnvConfig {
    account_homes: Vec<PathBuf>,
    rotation_id: String,
    coordinator_url: String,
    coordinator_token: String,
}

#[derive(Debug, Serialize)]
struct AuthRotationAdvanceRequest {
    account_index: usize,
    generation: u64,
    reason: &'static str,
}

#[derive(Debug, Serialize)]
struct AuthRotationAccountAuthRequest {
    account_index: usize,
    generation: u64,
}

#[derive(Debug, Deserialize)]
struct AuthRotationAccountAuthResponse {
    rotation_id: String,
    account_count: usize,
    account_index: usize,
    generation: u64,
    auth: AuthDotJson,
}

impl AuthRotation {
    pub(crate) fn from_env(chatgpt_base_url: Option<String>) -> Result<Option<Self>> {
        let Some(raw_config) = std::env::var(CODEX_AUTH_ROTATION_JSON_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
        else {
            return Ok(None);
        };

        let config: AuthRotationEnvConfig = serde_json::from_str(&raw_config)?;
        Ok(Some(Self::from_config(config, chatgpt_base_url)?))
    }

    pub(crate) fn account_count(&self) -> usize {
        self.accounts.len()
    }

    pub(crate) async fn active_state(&self) -> Result<AuthRotationState> {
        let url = self.endpoint_url("/state");
        let response = self
            .http_client
            .get(url)
            .bearer_auth(&self.coordinator_token)
            .send()
            .await
            .map_err(coordinator_error)?;
        self.decode_state_response(response).await
    }

    pub(crate) async fn advance_after_usage_limit(
        &self,
        account_index: usize,
        generation: u64,
    ) -> Result<AuthRotationState> {
        let url = self.endpoint_url("/advance");
        let response = self
            .http_client
            .post(url)
            .bearer_auth(&self.coordinator_token)
            .json(&AuthRotationAdvanceRequest {
                account_index,
                generation,
                reason: "usage_limit_reached",
            })
            .send()
            .await
            .map_err(coordinator_error)?;
        self.decode_state_response(response).await
    }

    pub(crate) async fn refresh_account_auth(
        &self,
        account_index: usize,
        generation: u64,
    ) -> Result<()> {
        let account = self.accounts.get(account_index).ok_or_else(|| {
            CodexErr::InvalidRequest(format!(
                "auth rotation account index {account_index} is out of range"
            ))
        })?;
        let url = self.endpoint_url("/account-auth");
        let response = self
            .http_client
            .post(url)
            .bearer_auth(&self.coordinator_token)
            .json(&AuthRotationAccountAuthRequest {
                account_index,
                generation,
            })
            .send()
            .await
            .map_err(coordinator_error)?;
        let auth_response = self.decode_account_auth_response(response).await?;
        if auth_response.account_index != account_index {
            return Err(CodexErr::InvalidRequest(
                "Codex auth rotation coordinator returned a different account_index".to_string(),
            ));
        }
        if auth_response.generation != generation {
            return Err(CodexErr::InvalidRequest(
                "Codex auth rotation coordinator returned a different generation".to_string(),
            ));
        }
        save_auth(
            &account.home,
            &auth_response.auth,
            AuthCredentialsStoreMode::File,
        )?;
        if let Some(manager) = account.manager.get() {
            manager.reload().await;
        }
        Ok(())
    }

    pub(crate) async fn client_setup_for_state(
        &self,
        state: &AuthRotationState,
        provider_info: &ModelProviderInfo,
    ) -> Result<AuthRotationClientSetup> {
        self.validate_state(state)?;
        self.client_setup_for_account_generation(
            state.active_index,
            state.generation,
            provider_info,
        )
        .await
    }

    pub(crate) async fn client_setup_for_account_generation(
        &self,
        account_index: usize,
        generation: u64,
        provider_info: &ModelProviderInfo,
    ) -> Result<AuthRotationClientSetup> {
        let account = self.accounts.get(account_index).ok_or_else(|| {
            CodexErr::InvalidRequest(format!(
                "auth rotation account index {account_index} is out of range"
            ))
        })?;
        let manager = account.manager(self.chatgpt_base_url.clone()).await;
        let auth = manager.auth_cached().ok_or_else(|| {
            CodexErr::InvalidRequest(format!(
                "auth rotation account {account_index} has no usable auth at {}",
                account.home.display()
            ))
        })?;
        let api_provider = provider_info.to_api_provider(Some(auth.auth_mode()))?;
        let api_auth = auth_provider_from_auth(&auth);
        Ok(AuthRotationClientSetup {
            account_index,
            generation,
            auth,
            api_provider,
            api_auth,
        })
    }

    fn from_config(
        config: AuthRotationEnvConfig,
        chatgpt_base_url: Option<String>,
    ) -> Result<Self> {
        let account_count = config.account_homes.len();
        if account_count < 2 {
            return Err(CodexErr::InvalidRequest(
                "CODEX_AUTH_ROTATION_JSON account_homes must contain at least two entries"
                    .to_string(),
            ));
        }
        if config.rotation_id.trim().is_empty() {
            return Err(CodexErr::InvalidRequest(
                "CODEX_AUTH_ROTATION_JSON rotation_id must not be empty".to_string(),
            ));
        }
        if config.coordinator_url.trim().is_empty() {
            return Err(CodexErr::InvalidRequest(
                "CODEX_AUTH_ROTATION_JSON coordinator_url must not be empty".to_string(),
            ));
        }
        if config.coordinator_token.trim().is_empty() {
            return Err(CodexErr::InvalidRequest(
                "CODEX_AUTH_ROTATION_JSON coordinator_token must not be empty".to_string(),
            ));
        }
        for home in &config.account_homes {
            if !home.is_absolute() {
                return Err(CodexErr::InvalidRequest(format!(
                    "CODEX_AUTH_ROTATION_JSON account home must be absolute: {}",
                    home.display()
                )));
            }
        }
        let http_client = reqwest::Client::builder()
            .timeout(COORDINATOR_REQUEST_TIMEOUT)
            .build()
            .map_err(coordinator_error)?;

        Ok(Self {
            accounts: config
                .account_homes
                .into_iter()
                .map(|home| AuthRotationAccount {
                    home,
                    manager: OnceCell::new(),
                })
                .collect(),
            rotation_id: config.rotation_id,
            coordinator_url: config.coordinator_url.trim_end_matches('/').to_string(),
            coordinator_token: config.coordinator_token,
            http_client,
            chatgpt_base_url,
        })
    }

    fn validate_state(&self, state: &AuthRotationState) -> Result<()> {
        if state.rotation_id != self.rotation_id {
            return Err(CodexErr::InvalidRequest(
                "Codex auth rotation coordinator returned a different rotation_id".to_string(),
            ));
        }
        if state.account_count != self.accounts.len() {
            return Err(CodexErr::InvalidRequest(
                "Codex auth rotation coordinator returned a different account_count".to_string(),
            ));
        }
        if state.active_index >= self.accounts.len() {
            return Err(CodexErr::InvalidRequest(format!(
                "Codex auth rotation coordinator returned out-of-range active_index {}",
                state.active_index
            )));
        }
        Ok(())
    }

    fn endpoint_url(&self, path: &str) -> String {
        format!("{}{}", self.coordinator_url, path)
    }

    async fn decode_state_response(
        &self,
        response: reqwest::Response,
    ) -> Result<AuthRotationState> {
        let status = response.status();
        let body = response.text().await.map_err(coordinator_error)?;
        if status != StatusCode::OK {
            return Err(CodexErr::Stream(
                format!("Codex auth rotation coordinator returned HTTP {status}: {body}"),
                None,
            ));
        }
        let state: AuthRotationState = serde_json::from_str(&body)?;
        self.validate_state(&state)?;
        Ok(state)
    }

    async fn decode_account_auth_response(
        &self,
        response: reqwest::Response,
    ) -> Result<AuthRotationAccountAuthResponse> {
        let status = response.status();
        let body = response.text().await.map_err(coordinator_error)?;
        if status != StatusCode::OK {
            return Err(CodexErr::Stream(
                format!("Codex auth rotation coordinator returned HTTP {status}: {body}"),
                None,
            ));
        }
        let auth_response: AuthRotationAccountAuthResponse = serde_json::from_str(&body)?;
        if auth_response.rotation_id != self.rotation_id {
            return Err(CodexErr::InvalidRequest(
                "Codex auth rotation coordinator returned a different rotation_id".to_string(),
            ));
        }
        if auth_response.account_count != self.accounts.len() {
            return Err(CodexErr::InvalidRequest(
                "Codex auth rotation coordinator returned a different account_count".to_string(),
            ));
        }
        if auth_response.account_index >= self.accounts.len() {
            return Err(CodexErr::InvalidRequest(format!(
                "Codex auth rotation coordinator returned out-of-range account_index {}",
                auth_response.account_index
            )));
        }
        Ok(auth_response)
    }
}

impl AuthRotationAccount {
    async fn manager(&self, chatgpt_base_url: Option<String>) -> Arc<AuthManager> {
        Arc::clone(
            self.manager
                .get_or_init(|| async {
                    Arc::new(
                        AuthManager::new(
                            self.home.clone(),
                            /*enable_codex_api_key_env*/ false,
                            AuthCredentialsStoreMode::File,
                            chatgpt_base_url,
                        )
                        .await,
                    )
                })
                .await,
        )
    }
}

fn coordinator_error(error: reqwest::Error) -> CodexErr {
    CodexErr::Stream(
        format!("Codex auth rotation coordinator request failed: {error}"),
        None,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(account_homes: Vec<PathBuf>) -> AuthRotationEnvConfig {
        AuthRotationEnvConfig {
            account_homes,
            rotation_id: "rotation-1".to_string(),
            coordinator_url: "http://127.0.0.1:12345".to_string(),
            coordinator_token: "token".to_string(),
        }
    }

    #[test]
    fn parses_valid_config() {
        let rotation = AuthRotation::from_config(
            config(vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")]),
            None,
        )
        .expect("rotation config should parse");

        assert_eq!(rotation.account_count(), 2);
        assert_eq!(rotation.rotation_id, "rotation-1");
    }

    #[test]
    fn rejects_fewer_than_two_accounts() {
        let err = AuthRotation::from_config(config(vec![PathBuf::from("/tmp/a")]), None)
            .expect_err("one account should fail");

        assert!(err.to_string().contains("at least two"));
    }

    #[test]
    fn rejects_non_absolute_account_home() {
        let err = AuthRotation::from_config(
            config(vec![PathBuf::from("/tmp/a"), PathBuf::from("relative")]),
            None,
        )
        .expect_err("relative account home should fail");

        assert!(err.to_string().contains("must be absolute"));
    }

    #[test]
    fn rejects_empty_coordinator_fields() {
        let mut config = config(vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")]);
        config.coordinator_token = String::new();

        let err = AuthRotation::from_config(config, None).expect_err("empty token should fail");

        assert!(err.to_string().contains("coordinator_token"));
    }

    #[test]
    fn rejects_mismatched_coordinator_state() {
        let rotation = AuthRotation::from_config(
            config(vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")]),
            None,
        )
        .expect("rotation config should parse");

        let err = rotation
            .validate_state(&AuthRotationState {
                rotation_id: "other".to_string(),
                account_count: 2,
                active_index: 0,
                generation: 0,
            })
            .expect_err("mismatched state should fail");

        assert!(err.to_string().contains("rotation_id"));
    }
}
