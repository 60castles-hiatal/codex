use super::*;
use codex_extension_api::ExtensionData;
use codex_extension_api::TurnItemContributor;
use codex_protocol::items::AgentMessageContent;
use pretty_assertions::assert_eq;
use std::sync::Arc;

struct RewriteAgentMessageContributor;

impl TurnItemContributor for RewriteAgentMessageContributor {
    fn contribute<'a>(
        &'a self,
        _thread_store: &'a ExtensionData,
        _turn_store: &'a ExtensionData,
        item: &'a mut TurnItem,
    ) -> codex_extension_api::ExtensionFuture<'a, Result<(), String>> {
        Box::pin(async move {
            if let TurnItem::AgentMessage(agent_message) = item {
                agent_message.content = vec![AgentMessageContent::Text {
                    text: "plan contributed assistant text".to_string(),
                }];
            }
            Ok(())
        })
    }
}

fn assistant_output_text(text: &str) -> ResponseItem {
    ResponseItem::Message {
        id: Some("msg-1".to_string()),
        role: "assistant".to_string(),
        content: vec![ContentItem::OutputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: None,
    }
}

fn input_text_message(role: &str, text: &str, turn_id: Option<&str>) -> ResponseItem {
    ResponseItem::Message {
        id: None,
        role: role.to_string(),
        content: vec![ContentItem::InputText {
            text: text.to_string(),
        }],
        phase: None,
        internal_chat_message_metadata_passthrough: Some(
            codex_protocol::models::InternalChatMessageMetadataPassthrough {
                turn_id: turn_id.map(str::to_string),
            },
        ),
    }
}

#[test]
fn current_turn_user_messages_are_rewritten_as_developer_for_prompt() {
    let input = vec![
        input_text_message("user", "current user", Some("turn-current")),
        input_text_message("user", "previous user", Some("turn-previous")),
        input_text_message("user", "unstamped user", None),
        input_text_message("developer", "current developer", Some("turn-current")),
        assistant_output_text("current assistant"),
    ];

    let rewritten = rewrite_current_turn_user_messages_as_developer(input, "turn-current");

    let roles = rewritten
        .iter()
        .filter_map(|item| match item {
            ResponseItem::Message { role, .. } => Some(role.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        roles,
        vec!["developer", "user", "user", "developer", "assistant"]
    );
}

#[test]
fn current_turn_role_rewrite_leaves_non_message_items_unchanged() {
    let output = ResponseItem::FunctionCallOutput {
        id: None,
        call_id: "call-1".to_string(),
        output: codex_protocol::models::FunctionCallOutputPayload::from_text(
            "tool output".to_string(),
        ),
        internal_chat_message_metadata_passthrough: Some(
            codex_protocol::models::InternalChatMessageMetadataPassthrough {
                turn_id: Some("turn-current".to_string()),
            },
        ),
    };

    let rewritten = rewrite_current_turn_user_messages_as_developer(
        vec![
            output.clone(),
            input_text_message("user", "current user", Some("turn-current")),
        ],
        "turn-current",
    );

    assert_eq!(rewritten[0], output);
    assert!(matches!(
        &rewritten[1],
        ResponseItem::Message { role, .. } if role == "developer"
    ));
}

#[tokio::test]
async fn plan_mode_uses_contributed_turn_item_for_last_agent_message() {
    let (mut session, turn_context) = crate::session::tests::make_session_and_context().await;
    let mut builder = codex_extension_api::ExtensionRegistryBuilder::new();
    builder.turn_item_contributor(Arc::new(RewriteAgentMessageContributor));
    session.services.extensions = Arc::new(builder.build());
    let turn_store = ExtensionData::new(turn_context.sub_id.clone());
    let mut state = PlanModeStreamState::new(&turn_context.sub_id);
    let mut last_agent_message = None;
    let item = assistant_output_text("original assistant text");

    let handled = handle_assistant_item_done_in_plan_mode(
        &session,
        &turn_context,
        &turn_store,
        &item,
        &mut state,
        /*previously_active_item*/ None,
        &mut last_agent_message,
    )
    .await;

    assert!(handled);
    assert_eq!(
        last_agent_message.as_deref(),
        Some("plan contributed assistant text")
    );
}
