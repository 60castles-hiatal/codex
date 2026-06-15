use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use codex_api::ApiError;
use codex_api::SharedAuthProvider;
use codex_login::AuthCredentialsStoreMode;
use codex_login::AuthManager;
use codex_login::CodexAuth;
use codex_model_provider::auth_provider_from_auth;
use codex_model_provider_info::ModelProviderInfo;
use codex_protocol::error::CodexErr;
use codex_protocol::error::Result;
use serde::Deserialize;
use tokio::sync::OnceCell;
use tracing::warn;

pub(crate) const CODEX_AUTH_ROTATION_JSON_ENV: &str = "CODEX_AUTH_ROTATION_JSON";
pub(crate) const DEFAULT_SLOT_SECONDS: u64 = 2250;
pub(crate) const DEFAULT_PRECONNECT_LEAD_SECONDS: u64 = 120;
pub(crate) const DEFAULT_STOP_NEW_AFTER_CONNECTED_SECONDS: u64 = 2400;
pub(crate) const DEFAULT_HARD_CAP_SECONDS: u64 = 2500;
const DEFAULT_FAILURE_COOLDOWN_SECONDS: u64 = 600;

#[derive(Debug)]
pub(crate) struct AuthRotation {
    accounts: Vec<AuthRotationAccount>,
    timings: AuthRotationTimings,
    epoch_seconds: u64,
    failure_cooldown: Duration,
    failures: StdMutex<HashMap<usize, Instant>>,
    chatgpt_base_url: Option<String>,
}

#[derive(Debug)]
struct AuthRotationAccount {
    home: PathBuf,
    manager: OnceCell<Arc<AuthManager>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AuthRotationTimings {
    pub(crate) slot: Duration,
    pub(crate) preconnect_lead: Duration,
    pub(crate) stop_new_after_connected: Duration,
    pub(crate) hard_cap: Duration,
}

#[derive(Clone)]
pub(crate) struct AuthRotationClientSetup {
    pub(crate) account_index: usize,
    pub(crate) auth: CodexAuth,
    pub(crate) api_provider: codex_api::Provider,
    pub(crate) api_auth: SharedAuthProvider,
    pub(crate) auth_manager: Arc<AuthManager>,
}

#[derive(Debug, Deserialize)]
struct AuthRotationEnvConfig {
    account_homes: Vec<PathBuf>,
    #[serde(default)]
    slot_seconds: Option<u64>,
    #[serde(default)]
    preconnect_lead_seconds: Option<u64>,
    #[serde(default)]
    stop_new_after_connected_seconds: Option<u64>,
    #[serde(default)]
    hard_cap_seconds: Option<u64>,
    #[serde(default)]
    epoch_seconds: Option<u64>,
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

    #[cfg(test)]
    pub(crate) fn timings(&self) -> AuthRotationTimings {
        self.timings
    }

    pub(crate) fn account_count(&self) -> usize {
        self.accounts.len()
    }

    pub(crate) fn should_preconnect(&self, connected_at: Instant) -> bool {
        connected_at.elapsed()
            >= self
                .timings
                .slot
                .saturating_sub(self.timings.preconnect_lead)
    }

    pub(crate) fn should_promote(&self, connected_at: Instant) -> bool {
        let age = connected_at.elapsed();
        age >= self.timings.slot || age >= self.timings.stop_new_after_connected
    }

    pub(crate) fn initial_account_index(&self) -> usize {
        self.slot_index_for_time(SystemTime::now())
    }

    pub(crate) fn next_account_after(&self, account_index: usize) -> Option<usize> {
        self.next_usable_account((account_index + 1) % self.accounts.len())
    }

    pub(crate) fn next_initial_account(&self) -> Option<usize> {
        self.next_usable_account(self.initial_account_index())
    }

    pub(crate) fn mark_failure(&self, account_index: usize, error: &ApiError) {
        warn!(
            "Codex auth rotation account {account_index} failed; cooling down for {}s: {error}",
            self.failure_cooldown.as_secs()
        );
        self.failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(account_index, Instant::now() + self.failure_cooldown);
    }

    pub(crate) fn clear_failure(&self, account_index: usize) {
        self.failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&account_index);
    }

    pub(crate) async fn client_setup_for_account(
        &self,
        account_index: usize,
        provider_info: &ModelProviderInfo,
    ) -> Result<AuthRotationClientSetup> {
        let account = self.accounts.get(account_index).ok_or_else(|| {
            CodexErr::InvalidRequest(format!(
                "auth rotation account index {account_index} is out of range"
            ))
        })?;
        let manager = account.manager(self.chatgpt_base_url.clone()).await;
        let auth = manager.auth().await.ok_or_else(|| {
            CodexErr::InvalidRequest(format!(
                "auth rotation account {account_index} has no usable auth at {}",
                account.home.display()
            ))
        })?;
        let api_provider = provider_info.to_api_provider(Some(auth.auth_mode()))?;
        let api_auth = auth_provider_from_auth(&auth);
        Ok(AuthRotationClientSetup {
            account_index,
            auth,
            api_provider,
            api_auth,
            auth_manager: manager,
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
        for home in &config.account_homes {
            if !home.is_absolute() {
                return Err(CodexErr::InvalidRequest(format!(
                    "CODEX_AUTH_ROTATION_JSON account home must be absolute: {}",
                    home.display()
                )));
            }
        }

        let slot_seconds = config.slot_seconds.unwrap_or(DEFAULT_SLOT_SECONDS);
        let preconnect_lead_seconds = config
            .preconnect_lead_seconds
            .unwrap_or(DEFAULT_PRECONNECT_LEAD_SECONDS);
        let stop_new_after_connected_seconds = config
            .stop_new_after_connected_seconds
            .unwrap_or(DEFAULT_STOP_NEW_AFTER_CONNECTED_SECONDS);
        let hard_cap_seconds = config.hard_cap_seconds.unwrap_or(DEFAULT_HARD_CAP_SECONDS);
        validate_positive("slot_seconds", slot_seconds)?;
        validate_positive("preconnect_lead_seconds", preconnect_lead_seconds)?;
        validate_positive(
            "stop_new_after_connected_seconds",
            stop_new_after_connected_seconds,
        )?;
        validate_positive("hard_cap_seconds", hard_cap_seconds)?;
        if preconnect_lead_seconds >= slot_seconds {
            return Err(CodexErr::InvalidRequest(
                "CODEX_AUTH_ROTATION_JSON preconnect_lead_seconds must be smaller than slot_seconds"
                    .to_string(),
            ));
        }
        if slot_seconds
            .checked_add(preconnect_lead_seconds)
            .is_none_or(|preconnect_end_seconds| preconnect_end_seconds >= hard_cap_seconds)
        {
            return Err(CodexErr::InvalidRequest(
                "CODEX_AUTH_ROTATION_JSON slot_seconds + preconnect_lead_seconds must be smaller than hard_cap_seconds"
                    .to_string(),
            ));
        }
        if stop_new_after_connected_seconds >= hard_cap_seconds {
            return Err(CodexErr::InvalidRequest(
                "CODEX_AUTH_ROTATION_JSON stop_new_after_connected_seconds must be smaller than hard_cap_seconds"
                    .to_string(),
            ));
        }

        Ok(Self {
            accounts: config
                .account_homes
                .into_iter()
                .map(|home| AuthRotationAccount {
                    home,
                    manager: OnceCell::new(),
                })
                .collect(),
            timings: AuthRotationTimings {
                slot: Duration::from_secs(slot_seconds),
                preconnect_lead: Duration::from_secs(preconnect_lead_seconds),
                stop_new_after_connected: Duration::from_secs(stop_new_after_connected_seconds),
                hard_cap: Duration::from_secs(hard_cap_seconds),
            },
            epoch_seconds: config.epoch_seconds.unwrap_or_default(),
            failure_cooldown: Duration::from_secs(DEFAULT_FAILURE_COOLDOWN_SECONDS),
            failures: StdMutex::new(HashMap::new()),
            chatgpt_base_url,
        })
    }

    fn slot_index_for_time(&self, time: SystemTime) -> usize {
        let now_seconds = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let elapsed = now_seconds.saturating_sub(self.epoch_seconds);
        ((elapsed / self.timings.slot.as_secs()) as usize) % self.accounts.len()
    }

    fn next_usable_account(&self, start_index: usize) -> Option<usize> {
        let mut failures = self
            .failures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        failures.retain(|_, retry_at| *retry_at > now);
        for offset in 0..self.accounts.len() {
            let account_index = (start_index + offset) % self.accounts.len();
            if !failures.contains_key(&account_index) {
                return Some(account_index);
            }
        }
        Some(start_index)
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

fn validate_positive(name: &str, value: u64) -> Result<()> {
    if value == 0 {
        return Err(CodexErr::InvalidRequest(format!(
            "CODEX_AUTH_ROTATION_JSON {name} must be positive"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(account_homes: Vec<PathBuf>) -> AuthRotationEnvConfig {
        AuthRotationEnvConfig {
            account_homes,
            slot_seconds: Some(DEFAULT_SLOT_SECONDS),
            preconnect_lead_seconds: Some(DEFAULT_PRECONNECT_LEAD_SECONDS),
            stop_new_after_connected_seconds: Some(DEFAULT_STOP_NEW_AFTER_CONNECTED_SECONDS),
            hard_cap_seconds: Some(DEFAULT_HARD_CAP_SECONDS),
            epoch_seconds: Some(0),
        }
    }

    #[test]
    fn parses_valid_config() {
        let rotation = AuthRotation::from_config(
            config(vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")]),
            None,
        )
        .expect("rotation config should parse");

        assert_eq!(
            rotation.timings(),
            AuthRotationTimings {
                slot: Duration::from_secs(DEFAULT_SLOT_SECONDS),
                preconnect_lead: Duration::from_secs(DEFAULT_PRECONNECT_LEAD_SECONDS),
                stop_new_after_connected: Duration::from_secs(
                    DEFAULT_STOP_NEW_AFTER_CONNECTED_SECONDS
                ),
                hard_cap: Duration::from_secs(DEFAULT_HARD_CAP_SECONDS),
            }
        );
    }

    #[test]
    fn rejects_fewer_than_two_accounts() {
        let err = AuthRotation::from_config(config(vec![PathBuf::from("/tmp/a")]), None)
            .expect_err("one account should fail");

        assert!(err.to_string().contains("at least two"));
    }

    #[test]
    fn rejects_non_positive_timings() {
        let mut config = config(vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")]);
        config.slot_seconds = Some(0);

        let err = AuthRotation::from_config(config, None).expect_err("zero slot should fail");

        assert!(err.to_string().contains("slot_seconds must be positive"));
    }

    #[test]
    fn rejects_preconnect_window_that_reaches_hard_cap() {
        let mut config = config(vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")]);
        config.slot_seconds = Some(2400);
        config.preconnect_lead_seconds = Some(100);
        config.hard_cap_seconds = Some(2500);

        let err = AuthRotation::from_config(config, None)
            .expect_err("preconnect window should not reach hard cap");

        assert!(err.to_string().contains("hard_cap_seconds"));
    }

    #[test]
    fn skips_failed_account_while_in_cooldown() {
        let rotation = AuthRotation::from_config(
            config(vec![PathBuf::from("/tmp/a"), PathBuf::from("/tmp/b")]),
            None,
        )
        .expect("rotation config should parse");

        rotation.mark_failure(0, &ApiError::Stream("failed account".to_string()));

        assert_eq!(rotation.next_initial_account(), Some(1));
    }
}
