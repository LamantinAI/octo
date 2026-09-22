//! `octo-connector-imagegen` — draw or edit an image with `gpt-image-2` through the
//! ChatGPT-subscription Images endpoint (the same `auth.json` the LLM uses), so images cost
//! no API key and no per-image billing.
//!
//! An env-as-tools organ: dispatch `imagegen.run { prompt, size?, quality?, background?,
//! images? }` and get a correlated `imagegen.run.result { path }` (or `{ error }`). The PNG
//! is written into the shared workspace (jailed against `..`/absolute escapes);
//! `chat.send_file { path }` on the telegram connector then sends it as a photo. Input
//! images (`images`, workspace paths) turn the call into an edit. The subscription token
//! comes from the shared [`SubscriptionAuth`] handed in at construction — the SAME
//! refresh-owner the cogitator's LLM path uses.
//!
//! This is the request Codex's own `image_gen` tool makes: a plain JSON POST to
//! `{codex base}/images/generations` (or `/images/edits`, inputs as data URLs) answered with
//! base64 PNGs — one call, no agent loop in between.

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use chrono::Utc;
use octo_core::{
    Connector, ConnectorCapabilities, ConnectorContext, ConnectorId, Envelope, EventKind, Filter,
    OctoResult, SubscribeOptions,
};
use octo_openai_auth::{Subscription, SubscriptionAuth};
use octo_workspace::{read_in_root, workspace_root, write_in_root};
use reqwest::{Client as HttpClient, StatusCode};
use serde_json::{json, Map, Value};
use tracing::{info, warn};
use uuid::Uuid;

/// Command kind this connector accepts.
const RUN: &str = "imagegen.run";
/// The Codex backend on the subscription; the Images routes hang off it.
const CODEX_BASE: &str = "https://chatgpt.com/backend-api/codex";
/// The image model the endpoint serves.
const MODEL: &str = "gpt-image-2";
/// What the endpoint expects the client to call itself.
const ORIGINATOR: &str = "Codex Desktop";
/// The endpoint sits behind Cloudflare, which refuses some client signatures outright.
const USER_AGENT: &str = "Codex Desktop";
/// One image takes tens of seconds (longer at `high`); generous for a slow link.
const TIMEOUT: Duration = Duration::from_secs(300);
/// The edit endpoint takes at most this many input images.
const MAX_INPUT_IMAGES: usize = 5;
const QUALITIES: [&str; 4] = ["low", "medium", "high", "auto"];
const BACKGROUNDS: [&str; 3] = ["transparent", "opaque", "auto"];

const CATALOG: &str = "Draw or edit an image with gpt-image-2 on the ChatGPT subscription. Dispatch to this connector's id:
- imagegen.run { prompt, size?, quality?, background?, images? } -> { path }
  `prompt` is the full image spec. `size`: \"1024x1024\", \"1536x1024\", \"1024x1536\", \"2048x1152\"
  or \"auto\" (edges multiples of 16, ratio at most 3:1; omitted -> the endpoint picks). `quality`: low |
  medium | high | auto (low is fastest). `background`: transparent | opaque | auto. `images`: up to 5
  workspace paths — present, the call EDITS them (describe each one's role in the prompt). Returns
  `path`: a workspace PNG; send it with chat.send_file { path } and it arrives as a photo. One image
  takes ~20-60 s.";

pub struct ImagegenConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    /// Shared subscription auth — one refresh owner with the rest of the runtime.
    auth: Arc<SubscriptionAuth>,
    /// Explicit workspace root; `None` -> resolved from the environment at use.
    workspace: Option<PathBuf>,
}

impl ImagegenConnector {
    /// Construct the connector with the shared subscription-auth handle and an optional
    /// workspace root (the shared file jail inputs are read from and the PNG written to).
    pub fn new(
        id: impl Into<String>,
        auth: Arc<SubscriptionAuth>,
        workspace: Option<PathBuf>,
    ) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_accept_kinds([EventKind::from_static(RUN)])
            .with_description(CATALOG);
        Arc::new(Self { id: ConnectorId::new(id), capabilities, auth, workspace })
    }

    async fn handle(&self, env: &Envelope, ctx: &ConnectorContext) {
        if env.kind.as_str() != RUN {
            return;
        }
        let params = env.payload_as::<Value>().cloned().unwrap_or(Value::Null);
        let payload = self.run(&params).await.unwrap_or_else(|e| json!({ "error": e }));
        let resp = Envelope::new(self.id.clone(), EventKind::new(format!("{RUN}.result")), payload)
            .with_correlation(env.id);
        if let Err(e) = ctx.publish(resp).await {
            warn!(error = %e, "imagegen: failed to publish result");
        }
    }

    async fn run(&self, params: &Value) -> Result<Value, String> {
        let root = workspace_root(self.workspace.as_deref()).map_err(|e| e.to_string())?;
        let request = ImageRequest::from_params(params, |path| {
            read_in_root(&root, path).map_err(|e| e.to_string())
        })?;
        let sub = self.auth.fresh().await.map_err(|e| e.to_string())?;

        info!(edit = request.is_edit(), size = ?request.size, "imagegen: run");
        let png = match generate(&request, &sub).await {
            // The server can revoke a token ahead of its `exp`: refresh once and retry.
            Err(ImageError::Unauthorized(_)) => {
                warn!("imagegen: token refused; forcing a refresh and retrying once");
                let sub = self.auth.force_refresh().await.map_err(|e| e.to_string())?;
                generate(&request, &sub).await
            }
            other => other,
        }
        .map_err(|e| e.to_string())?;

        let rel = format!("image-{}.png", Utc::now().timestamp_nanos_opt().unwrap_or_default());
        write_in_root(&root, &rel, &png).map_err(|e| e.to_string())?;
        info!(path = %rel, bytes = png.len(), "imagegen: wrote image");
        Ok(json!({ "path": rel, "bytes": png.len() }))
    }
}

#[async_trait]
impl Connector for ImagegenConnector {
    fn id(&self) -> &ConnectorId {
        &self.id
    }

    fn capabilities(&self) -> &ConnectorCapabilities {
        &self.capabilities
    }

    async fn run(self: Arc<Self>, ctx: ConnectorContext) -> OctoResult<()> {
        let mut cmds = ctx
            .subscribe(Filter::by_target(self.id.clone()), SubscribeOptions::default())
            .await?;
        info!(connector = %self.id, "imagegen ready");
        loop {
            tokio::select! {
                next = cmds.next() => match next {
                    Some(env) => self.handle(&env, &ctx).await,
                    None => return Ok(()),
                },
                _ = ctx.shutdown.cancelled() => return Ok(()),
            }
        }
    }
}

/// A validated generation or edit request.
struct ImageRequest {
    prompt: String,
    size: Option<String>,
    quality: Option<String>,
    background: Option<String>,
    /// Input images as data URLs; non-empty makes this an edit.
    images: Vec<String>,
}

impl ImageRequest {
    /// Validate the dispatch params; `read` loads a workspace input image by path.
    fn from_params(
        params: &Value,
        read: impl Fn(&str) -> Result<Vec<u8>, String>,
    ) -> Result<Self, String> {
        let prompt = params
            .get("prompt")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .ok_or("provide `prompt` (the image spec)")?
            .to_string();
        let size = optional_str(params, "size").map(validate_size).transpose()?;
        let quality = optional_str(params, "quality").map(|q| one_of(q, &QUALITIES, "quality")).transpose()?;
        let background =
            optional_str(params, "background").map(|b| one_of(b, &BACKGROUNDS, "background")).transpose()?;

        let paths: Vec<&str> = match params.get("images") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| v.as_str().ok_or("`images` must be a list of workspace paths"))
                .collect::<Result<_, _>>()?,
            Some(_) => return Err("`images` must be a list of workspace paths".into()),
        };
        if paths.len() > MAX_INPUT_IMAGES {
            return Err(format!("at most {MAX_INPUT_IMAGES} input images, got {}", paths.len()));
        }
        let images = paths
            .iter()
            .map(|path| Ok(data_url(path, &read(path)?)))
            .collect::<Result<_, String>>()?;

        Ok(Self { prompt, size, quality, background, images })
    }

    fn is_edit(&self) -> bool {
        !self.images.is_empty()
    }

    /// The route under the Codex base: generations, or edits when there are inputs.
    fn route(&self) -> &'static str {
        if self.is_edit() { "images/edits" } else { "images/generations" }
    }

    /// The JSON body, shaped like Codex's own `ImageGenerationRequest` / `ImageEditRequest`.
    fn body(&self) -> Value {
        let mut body = Map::new();
        body.insert("model".into(), json!(MODEL));
        body.insert("prompt".into(), json!(self.prompt));
        for (key, value) in [("size", &self.size), ("quality", &self.quality), ("background", &self.background)] {
            if let Some(v) = value {
                body.insert(key.into(), json!(v));
            }
        }
        if self.is_edit() {
            let images: Vec<Value> = self.images.iter().map(|u| json!({ "image_url": u })).collect();
            body.insert("images".into(), Value::Array(images));
        }
        Value::Object(body)
    }
}

/// Why a request failed. `Unauthorized` means the server refused the token — the caller
/// can force a refresh and retry once (a revocation ahead of the JWT's `exp`).
#[derive(Debug)]
enum ImageError {
    Unauthorized(String),
    Failed(String),
}

impl fmt::Display for ImageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unauthorized(msg) | Self::Failed(msg) => f.write_str(msg),
        }
    }
}

/// POST the request on the subscription token and return the first image's PNG bytes.
async fn generate(request: &ImageRequest, sub: &Subscription) -> Result<Vec<u8>, ImageError> {
    let resp = HttpClient::new()
        .post(format!("{CODEX_BASE}/{}", request.route()))
        .timeout(TIMEOUT)
        .header("Authorization", format!("Bearer {}", sub.access_token))
        .header("chatgpt-account-id", sub.account_id.as_str())
        .header("originator", ORIGINATOR)
        .header("User-Agent", USER_AGENT)
        .header("x-codex-image-turn-id", Uuid::now_v7().to_string())
        .json(&request.body())
        .send()
        .await
        .map_err(|e| ImageError::Failed(format!("image request failed: {e}")))?;

    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if status == StatusCode::UNAUTHORIZED {
        return Err(ImageError::Unauthorized(format!(
            "HTTP 401: the subscription token was refused: {}",
            snippet(&text)
        )));
    }
    if !status.is_success() {
        return Err(ImageError::Failed(explain_http(status.as_u16(), &text)));
    }
    first_image(&text).map_err(ImageError::Failed)
}

/// Decode `data[0].b64_json` from an Images response.
fn first_image(body: &str) -> Result<Vec<u8>, String> {
    let parsed: Value =
        serde_json::from_str(body).map_err(|e| format!("bad response body: {e}: {}", snippet(body)))?;
    let b64 = parsed
        .get("data")
        .and_then(|d| d.get(0))
        .and_then(|d| d.get("b64_json"))
        .and_then(Value::as_str)
        .ok_or_else(|| format!("no image in response: {}", snippet(body)))?;
    BASE64.decode(b64).map_err(|e| format!("bad image payload: {e}"))
}

/// Map the endpoint's errors to something the agent can act on.
fn explain_http(code: u16, body: &str) -> String {
    match code {
        400 => format!("HTTP 400 — the request was rejected (often the prompt hit the content policy, or a bad size): {}", snippet(body)),
        403 => format!("HTTP 403 — blocked before the endpoint (Cloudflare) or not allowed on this plan: {}", snippet(body)),
        429 => "HTTP 429 — the Codex usage window is exhausted; try later.".into(),
        _ => format!("HTTP {code}: {}", snippet(body)),
    }
}

/// `"WxH"` with both edges multiples of 16 and a ratio no steeper than 3:1, or `"auto"`.
fn validate_size(size: &str) -> Result<String, String> {
    if size == "auto" {
        return Ok(size.into());
    }
    let bad = || format!("bad size {size:?}: use \"auto\" or \"WxH\" (edges multiples of 16, ratio at most 3:1)");
    let (w, h) = size.split_once('x').ok_or_else(bad)?;
    let (w, h): (u32, u32) = (w.parse().map_err(|_| bad())?, h.parse().map_err(|_| bad())?);
    if w == 0 || h == 0 || w % 16 != 0 || h % 16 != 0 || w.max(h) > 3 * w.min(h) {
        return Err(bad());
    }
    Ok(size.into())
}

fn one_of(value: &str, allowed: &[&str], what: &str) -> Result<String, String> {
    if allowed.contains(&value) {
        Ok(value.into())
    } else {
        Err(format!("bad {what} {value:?}: use one of {allowed:?}"))
    }
}

fn optional_str<'a>(params: &'a Value, key: &str) -> Option<&'a str> {
    params.get(key).and_then(Value::as_str).map(str::trim).filter(|s| !s.is_empty())
}

/// An input image as a data URL, its type guessed from the extension.
fn data_url(path: &str, bytes: &[u8]) -> String {
    let ext = Path::new(path).extension().and_then(|e| e.to_str()).unwrap_or("").to_ascii_lowercase();
    let mime = match ext.as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "webp" => "image/webp",
        "gif" => "image/gif",
        _ => "image/png",
    };
    format!("data:{mime};base64,{}", BASE64.encode(bytes))
}

/// First 200 chars of an error/response body, for a log line that stays readable.
fn snippet(text: &str) -> String {
    text.chars().take(200).collect()
}

#[cfg(test)]
mod tests {
    use super::{first_image, validate_size, ImageRequest};
    use serde_json::json;

    fn no_files(_: &str) -> Result<Vec<u8>, String> {
        Err("no files in this test".into())
    }

    #[test]
    fn sizes_follow_the_endpoint_rules() {
        assert!(validate_size("1024x1024").is_ok());
        assert!(validate_size("2048x1152").is_ok());
        assert!(validate_size("auto").is_ok());
        assert!(validate_size("1000x1000").is_err()); // not multiples of 16
        assert!(validate_size("3200x1024").is_err()); // steeper than 3:1
        assert!(validate_size("big").is_err());
    }

    #[test]
    fn a_plain_request_is_a_generation() {
        let r = ImageRequest::from_params(&json!({ "prompt": "a fox", "quality": "low" }), no_files).unwrap();
        assert_eq!(r.route(), "images/generations");
        let body = r.body();
        assert_eq!(body["model"], "gpt-image-2");
        assert_eq!(body["quality"], "low");
        assert!(body.get("size").is_none() && body.get("images").is_none());
    }

    #[test]
    fn input_images_make_an_edit_with_data_urls() {
        let r = ImageRequest::from_params(
            &json!({ "prompt": "swap the background", "images": ["in/photo.jpg"] }),
            |_| Ok(vec![0xff, 0xd8]),
        )
        .unwrap();
        assert_eq!(r.route(), "images/edits");
        let url = r.body()["images"][0]["image_url"].as_str().unwrap().to_string();
        assert!(url.starts_with("data:image/jpeg;base64,"));
    }

    #[test]
    fn bad_params_are_refused_with_a_reason() {
        assert!(ImageRequest::from_params(&json!({}), no_files).is_err());
        assert!(ImageRequest::from_params(&json!({ "prompt": "x", "quality": "ultra" }), no_files).is_err());
        assert!(ImageRequest::from_params(&json!({ "prompt": "x", "images": "a.png" }), no_files).is_err());
        let six: Vec<String> = (0..6).map(|i| format!("{i}.png")).collect();
        assert!(ImageRequest::from_params(&json!({ "prompt": "x", "images": six }), |_| Ok(vec![1])).is_err());
    }

    #[test]
    fn the_first_image_is_decoded() {
        assert_eq!(first_image(r#"{"data":[{"b64_json":"AQID"}]}"#).unwrap(), vec![1, 2, 3]);
        assert!(first_image(r#"{"data":[]}"#).is_err());
    }

    /// LIVE: draw one image on the subscription. Ignored by default (hits the network and
    /// needs a real subscription auth.json):
    ///   IMAGEGEN_OUT=/tmp/image.png \
    ///     cargo test -p octo-connector-imagegen live_imagegen -- --ignored --nocapture
    /// The token store defaults to $HOME/.codex/auth.json (override with ALBERT_AUTH_JSON).
    #[tokio::test]
    #[ignore = "hits chatgpt.com; needs a real subscription auth.json"]
    async fn live_imagegen() {
        use super::generate;
        use octo_openai_auth::SubscriptionAuth;
        use std::path::PathBuf;

        let prompt = std::env::var("IMAGEGEN_PROMPT")
            .unwrap_or_else(|_| "A small green frog reading a book, flat vector illustration".into());
        let out = std::env::var("IMAGEGEN_OUT").unwrap_or_else(|_| "/tmp/image.png".into());
        let auth_path = std::env::var("ALBERT_AUTH_JSON")
            .unwrap_or_else(|_| format!("{}/.codex/auth.json", std::env::var("HOME").expect("HOME")));

        let auth = SubscriptionAuth::new(PathBuf::from(auth_path));
        let sub = auth.fresh().await.expect("a fresh subscription token");
        let request = ImageRequest::from_params(&json!({ "prompt": prompt, "quality": "low" }), no_files).unwrap();
        let png = generate(&request, &sub).await.expect("the endpoint draws the image");
        std::fs::write(&out, &png).expect("write the png");
        println!("\n=== DREW {} bytes -> {out} ===", png.len());
        assert_eq!(&png[1..4], b"PNG");
    }
}
