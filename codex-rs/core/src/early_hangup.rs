use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use codex_protocol::models::ResponseItem;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;

pub(crate) const FINAL_ANSWER_TOOL_NAME: &str = "final_answer";

pub(crate) fn final_answer_tool() -> ToolSpec {
    let properties = BTreeMap::from([(
        "answer".to_string(),
        JsonSchema::string(Some(
            "The exact final answer to show to the user. Only provide this when no further tool calls are needed."
                .to_string(),
        )),
    )]);

    ToolSpec::Function(ResponsesApiTool {
        name: FINAL_ANSWER_TOOL_NAME.to_string(),
        description: "Always call this to finish the turn. Call it only after all required non-final tools have already been called and their results have been received. Never call this in the same response as any other tool. The arguments must contain the exact final answer to show to the user."
            .to_string(),
        strict: true,
        defer_loading: None,
        parameters: JsonSchema::object(
            properties,
            Some(vec!["answer".to_string()]),
            Some(false.into()),
        ),
        output_schema: None,
    })
}

pub(crate) fn is_final_answer_call(item: &ResponseItem) -> bool {
    matches!(
        item,
        ResponseItem::FunctionCall {
            name,
            namespace: None,
            ..
        } if name == FINAL_ANSWER_TOOL_NAME
    )
}

fn is_non_final_tool_call(item: &ResponseItem) -> bool {
    match item {
        ResponseItem::FunctionCall { .. } => !is_final_answer_call(item),
        ResponseItem::LocalShellCall { .. }
        | ResponseItem::ToolSearchCall { .. }
        | ResponseItem::CustomToolCall { .. }
        | ResponseItem::WebSearchCall { .. }
        | ResponseItem::ImageGenerationCall { .. } => true,
        ResponseItem::Message { .. }
        | ResponseItem::AgentMessage { .. }
        | ResponseItem::Reasoning { .. }
        | ResponseItem::FunctionCallOutput { .. }
        | ResponseItem::CustomToolCallOutput { .. }
        | ResponseItem::ToolSearchOutput { .. }
        | ResponseItem::Compaction { .. }
        | ResponseItem::CompactionTrigger { .. }
        | ResponseItem::ContextCompaction { .. }
        | ResponseItem::Other => false,
    }
}

#[derive(Debug, Default)]
pub(crate) struct EarlyHangupState {
    final_answer_item_ids: HashSet<String>,
    argument_buffers: HashMap<String, String>,
    saw_non_final_tool_call: bool,
}

impl EarlyHangupState {
    pub(crate) fn observe_output_item(&mut self, item: &ResponseItem) {
        if is_final_answer_call(item) {
            if let ResponseItem::FunctionCall { id, call_id, .. } = item {
                if let Some(id) = id {
                    self.final_answer_item_ids.insert(id.clone());
                }
                self.final_answer_item_ids.insert(call_id.clone());
            }
        } else if is_non_final_tool_call(item) {
            self.saw_non_final_tool_call = true;
        }
    }

    pub(crate) fn observe_function_call_arguments_delta(
        &mut self,
        item_id: &str,
        delta: &str,
    ) -> Option<String> {
        if !self.final_answer_item_ids.contains(item_id) {
            return None;
        }
        let arguments = self
            .argument_buffers
            .entry(item_id.to_string())
            .or_default();
        arguments.push_str(delta);
        if self.saw_non_final_tool_call {
            return None;
        }
        final_answer_from_arguments(arguments)
    }

    pub(crate) fn final_answer_from_done_item(&self, item: &ResponseItem) -> Option<String> {
        if self.saw_non_final_tool_call || !is_final_answer_call(item) {
            return None;
        }
        let ResponseItem::FunctionCall { arguments, .. } = item else {
            return None;
        };
        final_answer_from_arguments(arguments)
    }
}

fn final_answer_from_arguments(arguments: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(arguments).ok()?;
    let answer = value.get("answer")?.as_str()?;
    Some(answer.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn final_answer_item() -> ResponseItem {
        ResponseItem::FunctionCall {
            id: Some("fc-final".to_string()),
            name: FINAL_ANSWER_TOOL_NAME.to_string(),
            namespace: None,
            arguments: String::new(),
            call_id: "call-final".to_string(),
            internal_chat_message_metadata_passthrough: None,
        }
    }

    #[test]
    fn parses_final_answer_from_incremental_arguments() {
        let mut state = EarlyHangupState::default();
        state.observe_output_item(&final_answer_item());

        assert_eq!(
            state.observe_function_call_arguments_delta("fc-final", r#"{"answer":"done"#),
            None
        );
        assert_eq!(
            state.observe_function_call_arguments_delta("fc-final", r#""}"#),
            Some("done".to_string())
        );
    }

    #[test]
    fn suppresses_early_answer_when_real_tool_was_seen() {
        let mut state = EarlyHangupState::default();
        state.observe_output_item(&ResponseItem::FunctionCall {
            id: Some("fc-real".to_string()),
            name: "lookup".to_string(),
            namespace: None,
            arguments: String::new(),
            call_id: "call-real".to_string(),
            internal_chat_message_metadata_passthrough: None,
        });
        state.observe_output_item(&final_answer_item());

        assert_eq!(
            state.observe_function_call_arguments_delta("fc-final", r#"{"answer":"done"}"#),
            None
        );
    }

    #[test]
    fn suppresses_done_item_answer_when_real_tool_was_seen_without_added_event() {
        let mut state = EarlyHangupState::default();
        state.observe_output_item(&ResponseItem::FunctionCall {
            id: Some("fc-real".to_string()),
            name: "update_goal".to_string(),
            namespace: None,
            arguments: r#"{"status":"complete"}"#.to_string(),
            call_id: "call-real".to_string(),
            internal_chat_message_metadata_passthrough: None,
        });

        let mut final_answer = final_answer_item();
        if let ResponseItem::FunctionCall { arguments, .. } = &mut final_answer {
            *arguments = r#"{"answer":"done"}"#.to_string();
        }

        assert_eq!(state.final_answer_from_done_item(&final_answer), None);
    }
}
