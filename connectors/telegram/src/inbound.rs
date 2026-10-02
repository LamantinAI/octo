use octo_core::{
    ChannelId, ChannelMetadata, ConnectorContext, ConnectorId, Envelope, EventKind, ReplyChannel,
    TrustLevel,
};
use teloxide::{Bot, net::Download, requests::Requester};

use crate::{
    Role,
    batch::{Emit, Flush},
};

/// The coalescing key for a message, or `None` to emit it immediately.
///
/// Telegram groups an album under one `media_group_id`; a forwarded burst has no
/// batch id, so forwarded messages are grouped per chat. A plain typed message
/// (the common case) returns `None` and is published with zero added latency.
pub(super) fn coalesce_key(msg: &teloxide::types::Message, chat: &str) -> Option<String> {
    if let Some(group) = msg.media_group_id() {
        Some(format!("mg:{chat}:{}:{}", sender_key(msg), group.0))
    } else if msg.forward_origin().is_some() {
        Some(format!("fwd:{chat}:{}", sender_key(msg)))
    } else {
        None
    }
}

fn sender_key(msg: &teloxide::types::Message) -> String {
    if let Some(chat) = &msg.sender_chat {
        return format!("chat-{}", chat.id);
    }
    msg.from
        .as_ref()
        .map(|u| u.id.to_string())
        .unwrap_or_else(|| "unknown".into())
}

/// Publish a flushed batch as one `chat.message` envelope.
pub(super) async fn publish_flush(id: &ConnectorId, ctx: &ConnectorContext, flush: Flush) {
    let Flush {
        chat,
        trust,
        emit,
        source,
    } = flush;
    let env = match emit {
        Emit::Text { text, caption } => chat_envelope(id, &chat, text, caption.as_deref(), trust),
        Emit::Image { blob, caption } => chat_envelope(id, &chat, blob, caption.as_deref(), trust),
        Emit::Audio {
            blob,
            caption,
            duration_secs,
            path,
        } => {
            let mut env = chat_envelope(id, &chat, blob, caption.as_deref(), trust);
            // Length is metadata, not content; the workspace path lets a cogitator hand
            // the recording to a tool (the transcribe organ) by reference.
            if let Some(secs) = duration_secs {
                env = env.with_tag("duration_secs", secs.to_string());
            }
            if let Some(path) = path {
                env = env.with_tag("workspace_path", path);
            }
            env
        }
        Emit::Multipart(msg) => chat_envelope(id, &chat, msg, None, trust),
    };
    let env = match source {
        Some(source) => source.apply(env),
        None => env,
    };
    if let Err(e) = ctx.publish(env).await {
        tracing::warn!(error = %e, "failed to publish chat.message");
    }
}

/// Build an inbound `chat.message` envelope: the payload is the text (a `String`)
/// or the image (a `Blob`); the `chat_id` rides on both the channel and the
/// reply recommendation, and a non-empty caption is attached as a `caption` tag.
pub(super) fn chat_envelope<P: std::any::Any + Send + Sync>(
    id: &ConnectorId,
    chat: &str,
    payload: P,
    caption: Option<&str>,
    trust: Option<(Role, TrustLevel)>,
) -> Envelope {
    let mut env = Envelope::new(id.clone(), EventKind::from_static("chat.message"), payload)
        .with_channel(ChannelId::new(chat.to_string()))
        .with_reply_to(ReplyChannel::new(ChannelId::new(chat.to_string())));
    if let Some(cap) = caption.filter(|c| !c.is_empty()) {
        env = env.with_tag("caption", cap);
    }
    // Front-load authorization: the trust gradient (generic reflex gating) plus
    // the precise role (capability checks live downstream).
    if let Some((role, level)) = trust {
        env = env.with_channel_metadata(
            ChannelMetadata::new()
                .with_trust(level)
                .with_tag("role", role.as_str()),
        );
    }
    env
}

/// An inbound voice note or audio file: what's needed to fetch it and label the
/// resulting [`Blob`].
pub(super) struct VoiceMedia<'a> {
    pub(super) file_id: &'a teloxide::types::FileId,
    /// MIME as declared by the sender, defaulted per kind when absent.
    pub(super) mime: String,
    pub(super) filename: String,
    pub(super) duration_secs: u32,
}

/// Inbound audio: a voice note (`voice`) or an audio file sent as music (`audio`).
/// A voice note is OGG/Opus and its MIME is often missing, hence the defaults.
pub(super) fn voice_media(msg: &teloxide::types::Message) -> Option<VoiceMedia<'_>> {
    if let Some(voice) = msg.voice() {
        return Some(VoiceMedia {
            file_id: &voice.file.id,
            mime: voice
                .mime_type
                .as_ref()
                .map_or_else(|| "audio/ogg".to_string(), |m| m.essence_str().to_string()),
            filename: "voice.ogg".to_string(),
            duration_secs: voice.duration.seconds(),
        });
    }
    let audio = msg.audio()?;
    Some(VoiceMedia {
        file_id: &audio.file.id,
        mime: audio
            .mime_type
            .as_ref()
            .map_or_else(|| "audio/mpeg".to_string(), |m| m.essence_str().to_string()),
        filename: audio
            .file_name
            .clone()
            .unwrap_or_else(|| "audio".to_string()),
        duration_secs: audio.duration.seconds(),
    })
}

/// An inbound video: what's needed to fetch it, name it in the workspace, and
/// describe it to a cogitator.
pub(super) struct VideoMedia<'a> {
    pub(super) file_id: &'a teloxide::types::FileId,
    /// Telegram's own poster frame, when it sent one — a free first look at the
    /// clip that costs no local decoding.
    pub(super) thumbnail: Option<&'a teloxide::types::PhotoSize>,
    pub(super) filename: String,
    pub(super) duration_secs: u32,
    /// A round `video_note` rather than a regular clip; the two read differently
    /// in a chat, so the note says which arrived.
    pub(super) is_note: bool,
}

/// Inbound video: a clip (`video`) or a round video note (`video_note`). Video
/// sent as a plain *document* deliberately keeps the generic workspace route,
/// exactly as audio documents do.
pub(super) fn video_media(msg: &teloxide::types::Message) -> Option<VideoMedia<'_>> {
    if let Some(video) = msg.video() {
        return Some(VideoMedia {
            file_id: &video.file.id,
            thumbnail: video.thumbnail.as_ref(),
            filename: video
                .file_name
                .clone()
                .unwrap_or_else(|| "video.mp4".to_string()),
            duration_secs: video.duration.seconds(),
            is_note: false,
        });
    }
    let note = msg.video_note()?;
    Some(VideoMedia {
        file_id: &note.file.id,
        thumbnail: note.thumbnail.as_ref(),
        // A video note carries no name of its own.
        filename: "video_note.mp4".to_string(),
        duration_secs: note.duration.seconds(),
        is_note: true,
    })
}

/// A clip length as `m:ss`, or `h:mm:ss` once it passes an hour.
pub(super) fn hms(secs: u32) -> String {
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m}:{s:02}")
    }
}

/// What the cogitator reads for an inbound video: the sender's caption (if any)
/// plus a note naming the clip, its length and the workspace path it was saved
/// to — the handle every ffmpeg/transcription tool needs. Mirrors the
/// `saved to workspace path` wording a plain document already gets.
pub(super) fn video_caption(
    video: &VideoMedia<'_>,
    caption: Option<String>,
    saved: Option<&str>,
) -> String {
    let kind = if video.is_note { "video note" } else { "video" };
    let (name, len) = (&video.filename, hms(video.duration_secs));
    let note = match saved {
        Some(rel) => {
            format!("[received {kind} `{name}` ({len}) — saved to workspace path `{rel}`]")
        }
        // Perception still happens (the thumbnail is attached); only the handle
        // for tools is missing, so say that rather than naming a path that isn't.
        None => format!("[received {kind} `{name}` ({len}) — could not save it to the workspace]"),
    };
    match caption {
        Some(c) if !c.is_empty() => format!("{c}\n\n{note}"),
        _ => note,
    }
}

/// A document attachment that is actually an image (`image/*` MIME), with its
/// MIME type rendered to a string — the "send as file" (uncompressed) path.
pub(super) fn image_document(
    msg: &teloxide::types::Message,
) -> Option<(&teloxide::types::Document, String)> {
    let doc = msg.document()?;
    let mime = doc.mime_type.as_ref()?;
    (mime.type_() == "image").then(|| (doc, mime.essence_str().to_string()))
}

/// A unique inbox filename for an incoming image: `<unix_millis>-<basename>`.
/// Telegram photos all arrive named `photo.jpg`, so a bare basename would have
/// each new image clobber the previous one in the inbox.
pub(super) fn inbox_name(base: &str) -> String {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let base = std::path::Path::new(base)
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("image");
    format!("{ts}-{base}")
}

/// Fold the saved workspace path into the image caption, so a cogitator learns
/// the image also lives on disk (and can be saved / forwarded / referenced) —
/// mirroring the `saved to workspace path` note a plain document already gets.
/// With no saved path (the write failed), the caption is passed through untouched.
pub(super) fn caption_with_saved(caption: Option<String>, saved: Option<&str>) -> Option<String> {
    let Some(rel) = saved else { return caption };
    let note = format!("[image saved to workspace path `{rel}`]");
    Some(match caption {
        Some(c) if !c.is_empty() => format!("{c}\n\n{note}"),
        _ => note,
    })
}

/// A short context line for a message that replies to another one, naming what it
/// answers so the cogitator has the quoted message in view. Quoted text (or caption)
/// is included, trimmed to a snippet; quoted media is named, never downloaded.
/// `None` when the message is not a reply.
pub(super) fn reply_context(msg: &teloxide::types::Message) -> Option<String> {
    let replied = msg.reply_to_message()?;
    let what = if let Some(text) = replied.text().or_else(|| replied.caption()) {
        let text = text.trim();
        let snippet: String = text.chars().take(300).collect();
        let ellipsis = if text.chars().count() > 300 {
            "…"
        } else {
            ""
        };
        format!("\"{snippet}{ellipsis}\"")
    } else if replied.photo().is_some() {
        "a photo".to_string()
    } else if replied.video().is_some() || replied.video_note().is_some() {
        "a video".to_string()
    } else if replied.voice().is_some() {
        "a voice message".to_string()
    } else if replied.audio().is_some() {
        "an audio file".to_string()
    } else if replied.document().is_some() {
        "a file".to_string()
    } else {
        "an earlier message".to_string()
    };
    Some(format!("[replying to {what}]"))
}

/// Fold an optional reply-context line onto the front of the user's text/caption, so
/// the quoted message rides with what they actually wrote. Empty body → the context
/// stands alone; no reply → the body passes through untouched.
pub(super) fn with_reply(reply: &Option<String>, body: Option<String>) -> Option<String> {
    match (reply, body) {
        (Some(r), Some(b)) if !b.trim().is_empty() => Some(format!("{r}\n{b}")),
        (Some(r), _) => Some(r.clone()),
        (None, b) => b,
    }
}

/// Resolve a Telegram `file_id` and download its bytes.
pub(super) async fn download_bytes(
    bot: &Bot,
    file_id: &teloxide::types::FileId,
) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
    let file = bot.get_file(file_id.clone()).await?;
    let mut bytes: Vec<u8> = Vec::new();
    bot.download_file(&file.path, &mut bytes).await?;
    Ok(bytes)
}
