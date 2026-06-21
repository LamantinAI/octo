//! LLM access via `rig` against OpenRouter, with native tool-calling.
//!
//! Supports a multimodal prompt: when an image [`Blob`] is present it is sent as
//! a base64 data-URI alongside the (optional) caption text, so a vision-capable
//! model can actually see it. OpenRouter accepts only base64/URL images (not raw
//! bytes), so we encode here.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use octo_core::Blob;
use octo_rig::OctoDispatchTool;
use rig::client::CompletionClient;
use rig::completion::message::{DocumentSourceKind, Image, ImageMediaType, Text, UserContent};
use rig::completion::{Message, Prompt};
use rig::providers::openrouter;
use rig::OneOrMany;

use crate::error::Result;

/// Run a prompt with the Octo dispatch tool attached and prior `history`,
/// letting rig drive its own tool-calling loop (up to `max_turns` rounds). The
/// model reaches Octo connectors natively; we don't parse JSON actions. When
/// `image` is set, the prompt is multimodal (caption text + the image).
pub async fn chat_with_tool(
    api_key: &str,
    model: &str,
    preamble: &str,
    text: &str,
    image: Option<&Blob>,
    history: Vec<Message>,
    tool: OctoDispatchTool,
    max_turns: usize,
) -> Result<String> {
    let client = openrouter::Client::new(api_key)?;
    let agent = client.agent(model).preamble(preamble).tool(tool).build();
    let answer = agent
        .prompt(build_prompt(text, image))
        .max_turns(max_turns)
        .with_history(history)
        .await?;
    Ok(answer)
}

/// Build the user prompt message: plain text, or text + image when a `Blob` is
/// present (image encoded as a base64 data-URI).
fn build_prompt(text: &str, image: Option<&Blob>) -> Message {
    let Some(blob) = image else {
        return Message::user(text);
    };
    let img = UserContent::Image(Image {
        data: DocumentSourceKind::Base64(STANDARD.encode(blob.bytes().as_ref())),
        media_type: Some(image_media_type(blob.content_type())),
        detail: None,
        additional_params: None,
    });
    let mut items = Vec::new();
    if !text.trim().is_empty() {
        items.push(UserContent::Text(Text { text: text.to_string() }));
    }
    items.push(img);
    // `items` always holds the image, so `many` never sees an empty list.
    Message::User { content: OneOrMany::many(items).expect("image content present") }
}

fn image_media_type(content_type: &str) -> ImageMediaType {
    match content_type {
        "image/png" => ImageMediaType::PNG,
        "image/webp" => ImageMediaType::WEBP,
        "image/gif" => ImageMediaType::GIF,
        // Telegram photos and our default are JPEG.
        _ => ImageMediaType::JPEG,
    }
}
