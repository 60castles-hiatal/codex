use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;

use codex_protocol::models::ResponseItem;
use codex_tools::JsonSchema;
use codex_tools::ResponsesApiTool;
use codex_tools::ToolSpec;

pub(crate) const FINAL_ANSWER_TOOL_NAME: &str = "final_answer";
const FINAL_ANSWER_EXTRA_VALUE: &str = concat!(
    "f6d79a07: This is the final answer, there are no more answers after this. ",
    "All content should be included in the prior message. This field doesn't do much, and should probably be removed in the long term. This is just a generic field with a generic enum."
);
const FINAL_ANSWER_EXTRA_TRIGGER: &str = "\n\nf6d79a07:";
const FINAL_ANSWER_EXTRA_TRIGGER_ESCAPED: &str = "\\n\\nf6d79a07:";

pub(crate) fn final_answer_tool() -> ToolSpec {
    let properties = BTreeMap::from([(
        "answer".to_string(),
        JsonSchema::string(Some(format!(
            "The exact final answer to show to the user, followed by exactly two newlines and then `{FINAL_ANSWER_EXTRA_VALUE}`. Only provide this when no further tool calls are needed."
        ))),
    )]);

    ToolSpec::Function(ResponsesApiTool {
        name: FINAL_ANSWER_TOOL_NAME.to_string(),
        description: format!(
            "Always call this to finish the turn. Call it only after all required non-final tools have already been called and their results have been received. Never call this in the same response as any other tool. Put the exact final answer in `answer`, then append exactly two newlines followed by `{FINAL_ANSWER_EXTRA_VALUE}`."
        ),
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
    json_string_field_prefix_before_marker(arguments, "answer")
        .or_else(|| json_string_field(arguments, "answer").map(strip_final_answer_extra))
}

fn strip_final_answer_extra(answer: String) -> String {
    answer
        .find(FINAL_ANSWER_EXTRA_TRIGGER)
        .map(|index| answer[..index].trim_end().to_string())
        .unwrap_or(answer)
}

fn json_string_field_prefix_before_marker(input: &str, field_name: &str) -> Option<String> {
    let value_start = json_string_field_value_start(input, field_name)?;
    json_string_prefix_before_marker(input, value_start)
}

fn json_string_field(input: &str, field_name: &str) -> Option<String> {
    let value_start = json_string_field_value_start(input, field_name)?;
    let value_end = json_string_end(input, value_start)?;
    serde_json::from_str(&input[value_start..value_end]).ok()
}

fn json_string_field_value_start(input: &str, field_name: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut cursor = skip_json_whitespace(bytes, 0);
    if bytes.get(cursor) != Some(&b'{') {
        return None;
    }
    cursor += 1;

    loop {
        cursor = skip_json_whitespace(bytes, cursor);
        match bytes.get(cursor) {
            Some(b'}') | None => return None,
            Some(b'"') => {}
            _ => return None,
        }

        let key_end = json_string_end(input, cursor)?;
        let key: String = serde_json::from_str(&input[cursor..key_end]).ok()?;
        cursor = skip_json_whitespace(bytes, key_end);
        if bytes.get(cursor) != Some(&b':') {
            return None;
        }
        cursor = skip_json_whitespace(bytes, cursor + 1);

        if key == field_name {
            if bytes.get(cursor) != Some(&b'"') {
                return None;
            }
            return Some(cursor);
        }

        cursor = json_value_end(input, cursor)?;
        cursor = skip_json_whitespace(bytes, cursor);
        match bytes.get(cursor) {
            Some(b',') => cursor += 1,
            _ => return None,
        }
    }
}

fn json_string_prefix_before_marker(input: &str, value_start: usize) -> Option<String> {
    let bytes = input.as_bytes();
    if bytes.get(value_start) != Some(&b'"') {
        return None;
    }

    let search_start = value_start + 1;
    let search_end = json_string_end(input, value_start)
        .map(|end| end.saturating_sub(1))
        .unwrap_or(input.len());
    let value_tail = input.get(search_start..search_end)?;
    let marker_index = [
        FINAL_ANSWER_EXTRA_TRIGGER_ESCAPED,
        FINAL_ANSWER_EXTRA_TRIGGER,
    ]
    .iter()
    .filter_map(|marker| value_tail.find(marker))
    .min()?;
    let raw_answer = &input[value_start..search_start + marker_index];
    let candidate = format!("{raw_answer}\"");
    serde_json::from_str::<String>(&candidate)
        .ok()
        .map(|answer| answer.trim_end().to_string())
}

fn skip_json_whitespace(bytes: &[u8], mut cursor: usize) -> usize {
    while matches!(bytes.get(cursor), Some(b' ' | b'\n' | b'\r' | b'\t')) {
        cursor += 1;
    }
    cursor
}

fn json_string_end(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    if bytes.get(start) != Some(&b'"') {
        return None;
    }

    let mut cursor = start + 1;
    let mut escaped = false;
    while cursor < bytes.len() {
        let byte = bytes[cursor];
        if escaped {
            escaped = false;
            cursor += 1;
            continue;
        }

        match byte {
            b'\\' => {
                escaped = true;
                cursor += 1;
            }
            b'"' => return Some(cursor + 1),
            _ => cursor += 1,
        }
    }

    None
}

fn json_value_end(input: &str, start: usize) -> Option<usize> {
    let bytes = input.as_bytes();
    match bytes.get(start)? {
        b'"' => json_string_end(input, start),
        b'{' => json_container_end(input, start, b'{', b'}'),
        b'[' => json_container_end(input, start, b'[', b']'),
        b't' if input[start..].starts_with("true") => Some(start + 4),
        b'f' if input[start..].starts_with("false") => Some(start + 5),
        b'n' if input[start..].starts_with("null") => Some(start + 4),
        b'-' | b'0'..=b'9' => Some(json_number_end(bytes, start)),
        _ => None,
    }
}

fn json_container_end(input: &str, start: usize, open: u8, close: u8) -> Option<usize> {
    let bytes = input.as_bytes();
    if bytes.get(start) != Some(&open) {
        return None;
    }

    let mut stack = vec![close];
    let mut cursor = start + 1;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'"' => cursor = json_string_end(input, cursor)?,
            b'{' => {
                stack.push(b'}');
                cursor += 1;
            }
            b'[' => {
                stack.push(b']');
                cursor += 1;
            }
            b'}' | b']' => {
                if stack.pop() != Some(bytes[cursor]) {
                    return None;
                }
                cursor += 1;
                if stack.is_empty() {
                    return Some(cursor);
                }
            }
            _ => cursor += 1,
        }
    }

    None
}

fn json_number_end(bytes: &[u8], start: usize) -> usize {
    let mut cursor = start;
    while !matches!(
        bytes.get(cursor),
        None | Some(b' ' | b'\n' | b'\r' | b'\t' | b',' | b'}' | b']')
    ) {
        cursor += 1;
    }
    cursor
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
    fn final_answer_tool_requires_answer_with_fixed_trailing_marker() {
        let ToolSpec::Function(tool) = final_answer_tool() else {
            panic!("final_answer should be a function tool");
        };
        assert!(tool.description.contains(FINAL_ANSWER_EXTRA_VALUE));
        let parameters = tool.parameters;
        assert_eq!(parameters.required, Some(vec!["answer".to_string()]));
        assert_eq!(
            parameters
                .properties
                .as_ref()
                .map(|properties| properties.contains_key("extra")),
            Some(false)
        );
        let answer = parameters
            .properties
            .as_ref()
            .and_then(|properties| properties.get("answer"))
            .expect("answer schema should be present");
        assert!(
            answer
                .description
                .as_deref()
                .is_some_and(|description| description.contains(FINAL_ANSWER_EXTRA_VALUE))
        );
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
            state.observe_function_call_arguments_delta(
                "fc-final",
                r#"\n\nf6d79a07: This is the final answer, there are no more answers after this. All content should be included"#
            ),
            Some("done".to_string())
        );
    }

    #[test]
    fn parses_final_answer_before_arguments_object_is_complete() {
        assert_eq!(
            final_answer_from_arguments(
                r#"{"answer":"done\n\nf6d79a07: This is the final answer, there are no more answers after this. All content should be included"#
            ),
            Some("done".to_string())
        );
    }

    #[test]
    fn parses_escaped_final_answer_before_extra_is_complete() {
        assert_eq!(
            final_answer_from_arguments(
                r#"{"answer":"line\n\"quoted\"\n\nf6d79a07: This is the final answer"#
            ),
            Some("line\n\"quoted\"".to_string())
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
