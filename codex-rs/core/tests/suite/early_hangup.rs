use codex_features::Feature;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::Op;
use codex_protocol::user_input::UserInput;
use core_test_support::responses;
use core_test_support::responses::ev_completed;
use core_test_support::responses::ev_response_created;
use core_test_support::responses::mount_sse_sequence;
use core_test_support::responses::sse;
use core_test_support::responses::start_mock_server;
use core_test_support::skip_if_no_network;
use core_test_support::test_codex::test_codex;
use core_test_support::wait_for_event;
use pretty_assertions::assert_eq;
use serde_json::Value;
use serde_json::json;

fn final_answer_delta_sse(answer: &str) -> String {
    sse(vec![
        ev_response_created("resp-early"),
        json!({
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {
                "type": "function_call",
                "id": "fc-final",
                "name": "final_answer",
                "arguments": "",
                "call_id": "call-final",
                "status": "in_progress"
            }
        }),
        json!({
            "type": "response.function_call_arguments.delta",
            "item_id": "fc-final",
            "output_index": 0,
            "delta": serde_json::to_string(&json!({ "answer": answer }))
                .expect("final_answer arguments should serialise")
        }),
    ])
}

async fn submit_first_turn_and_capture_answer(
    test: &core_test_support::test_codex::TestCodex,
) -> anyhow::Result<Option<String>> {
    test.codex
        .submit(Op::UserInput {
            items: vec![UserInput::Text {
                text: "finish with final_answer".to_string(),
                text_elements: Vec::new(),
            }],
            final_output_json_schema: None,
            responsesapi_client_metadata: None,
            additional_context: Default::default(),
            thread_settings: Default::default(),
        })
        .await?;

    let mut agent_message = None;
    loop {
        let event = wait_for_event(&test.codex, |event| {
            matches!(event, EventMsg::AgentMessage(_) | EventMsg::TurnComplete(_))
        })
        .await;
        match event {
            EventMsg::AgentMessage(message) => {
                agent_message = Some(message.message);
            }
            EventMsg::TurnComplete(_) => break,
            _ => {}
        }
    }
    Ok(agent_message)
}

fn assistant_output_texts(request: &responses::ResponsesRequest) -> Vec<String> {
    request
        .input()
        .into_iter()
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("message"))
        .filter(|item| item.get("role").and_then(Value::as_str) == Some("assistant"))
        .filter_map(|item| item.get("content").and_then(Value::as_array).cloned())
        .flatten()
        .filter(|content| content.get("type").and_then(Value::as_str) == Some("output_text"))
        .filter_map(|content| {
            content
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_answer_delta_completes_turn_without_response_completed() -> anyhow::Result<()> {
    skip_if_no_network!(Ok(()));

    let server = start_mock_server().await;
    let response_mock = mount_sse_sequence(
        &server,
        vec![
            final_answer_delta_sse("early hangup final"),
            sse(vec![
                ev_response_created("resp-next"),
                ev_completed("resp-next"),
            ]),
        ],
    )
    .await;

    let test = test_codex()
        .with_config(|config| {
            let _ = config.features.enable(Feature::EarlyHangup);
        })
        .build(&server)
        .await?;

    let agent_message = submit_first_turn_and_capture_answer(&test).await?;
    assert_eq!(agent_message.as_deref(), Some("early hangup final"));

    test.submit_turn("next turn").await?;

    let requests = response_mock.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].body_json()["tool_choice"], json!("required"));
    let final_answer_tool = requests[0]
        .body_json()
        .get("tools")
        .and_then(Value::as_array)
        .and_then(|tools| {
            tools
                .iter()
                .find(|tool| tool.get("name").and_then(Value::as_str) == Some("final_answer"))
        })
        .cloned()
        .expect("final_answer tool should be present");
    assert_eq!(final_answer_tool["strict"], json!(true));
    assert_eq!(
        assistant_output_texts(&requests[1]),
        vec!["early hangup final".to_string()]
    );
    let assistant_history_item = requests[1]
        .input()
        .into_iter()
        .find(|item| {
            item.get("type").and_then(Value::as_str) == Some("message")
                && item.get("role").and_then(Value::as_str) == Some("assistant")
        })
        .expect("assistant history item should be present");
    assert_eq!(assistant_history_item.get("phase"), None);

    Ok(())
}
