use anyhow::Context;
use anyhow::Result;
use codex_protocol::models::ContentItem;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::RolloutLine;
use core_test_support::responses;
use core_test_support::responses::ev_assistant_message;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;

fn configure_minimal_prompt_context(config: &mut codex_core::config::Config) {
    config.include_permissions_instructions = false;
    config.include_apps_instructions = false;
    config.include_collaboration_mode_instructions = false;
    config.include_skill_instructions = false;
    config.include_environment_context = false;
}

fn rollout_has_user_message(path: &std::path::Path, text: &str) -> Result<bool> {
    let rollout_text = std::fs::read_to_string(path)
        .with_context(|| format!("read rollout file {}", path.display()))?;
    for line in rollout_text.lines().filter(|line| !line.trim().is_empty()) {
        let rollout: RolloutLine = serde_json::from_str(line).context("parse rollout line")?;
        let RolloutItem::ResponseItem(ResponseItem::Message { role, content, .. }) = rollout.item
        else {
            continue;
        };
        if role == "user"
            && content.iter().any(|item| {
                matches!(
                    item,
                    ContentItem::InputText { text: item_text } if item_text == text
                )
            })
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[test]
fn current_turn_user_message_is_developer_only_in_request() -> Result<()> {
    skip_if_no_network!(Ok(()));

    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .thread_stack_size(16 * 1024 * 1024)
        .enable_all()
        .build()?
        .block_on(async {
            let server = start_mock_server().await;
            let first_mock = responses::mount_sse_once(
                &server,
                sse(vec![
                    ev_response_created("resp-1"),
                    ev_assistant_message("msg-1", "first done"),
                    ev_completed("resp-1"),
                ]),
            )
            .await;
            let test = test_codex()
                .with_config(configure_minimal_prompt_context)
                .build(&server)
                .await?;

            test.submit_turn("first ordinary turn").await?;

            let first_request = first_mock.single_request();
            let first_developer_texts = first_request.message_input_texts("developer");
            let first_user_texts = first_request.message_input_texts("user");
            assert!(
                first_developer_texts
                    .iter()
                    .any(|text| text == "first ordinary turn"),
                "current turn should be developer-role in request"
            );
            assert!(
                !first_user_texts
                    .iter()
                    .any(|text| text == "first ordinary turn"),
                "current turn should not remain user-role in same request"
            );

            test.codex.flush_rollout().await?;
            let rollout_path = test.codex.rollout_path().context("rollout path")?;
            assert!(
                rollout_has_user_message(&rollout_path, "first ordinary turn")?,
                "rollout should persist the first turn as a user message"
            );

            let second_mock = responses::mount_sse_once(
                &server,
                sse(vec![
                    ev_response_created("resp-2"),
                    ev_assistant_message("msg-2", "second done"),
                    ev_completed("resp-2"),
                ]),
            )
            .await;

            test.submit_turn("second ordinary turn").await?;

            let second_request = second_mock.single_request();
            let second_developer_texts = second_request.message_input_texts("developer");
            let second_user_texts = second_request.message_input_texts("user");
            assert!(
                second_developer_texts
                    .iter()
                    .any(|text| text == "second ordinary turn"),
                "current second turn should be developer-role in request"
            );
            assert!(
                second_user_texts
                    .iter()
                    .any(|text| text == "first ordinary turn"),
                "previous turn should return to user-role in later request"
            );
            assert!(
                !second_developer_texts
                    .iter()
                    .any(|text| text == "first ordinary turn"),
                "previous turn should not stay developer-role"
            );

            Ok(())
        })
}
