//! LLM access via `rig` against OpenRouter, with native tool-calling.

use octo_rig::OctoDispatchTool;
use rig::client::CompletionClient;
use rig::completion::{Message, Prompt};
use rig::providers::openrouter;

use crate::error::Result;

/// Run a prompt with the Octo dispatch tool attached and prior `history`,
/// letting rig drive its own tool-calling loop (up to `max_turns` rounds). The
/// model reaches Octo connectors natively; we don't parse JSON actions.
pub async fn chat_with_tool(
    api_key: &str,
    model: &str,
    preamble: &str,
    user: &str,
    history: Vec<Message>,
    tool: OctoDispatchTool,
    max_turns: usize,
) -> Result<String> {
    let client = openrouter::Client::new(api_key)?;
    let agent = client.agent(model).preamble(preamble).tool(tool).build();
    let answer = agent
        .prompt(user)
        .max_turns(max_turns)
        .with_history(history)
        .await?;
    Ok(answer)
}
