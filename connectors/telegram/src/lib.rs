//! `octo-connector-telegram` — bidirectional connector over the Telegram Bot
//! API (teloxide long polling).
//!
//! Speaks the runtime's generic chat shapes, so any cogitator works unchanged —
//! only the `channel` carries transport detail: here it's the `chat_id`. Inbound
//! text messages become `chat.message` (with `reply_to = chat_id`); `chat.reply`
//! envelopes targeted at us are sent back to their `channel`'s chat (a `Blob`
//! payload → photo/document, a `String` → a rich message whose Markdown Telegram
//! lays out itself, falling back to the older HTML rendering — see [`format`] —
//! if the server has no rich messages). Inbound photos and image
//! documents are downloaded into `Blob` payloads for a vision cogitator, and
//! voice notes / audio files likewise for a hearing one (with a `duration_secs`
//! tag; audio sent as a plain document keeps the workspace-path route). Videos
//! and video notes are saved to the workspace for a tool to open, and reach the
//! cogitator as Telegram's own thumbnail plus a note naming that path — the
//! bytes themselves never travel to a model. While
//! a turn is running, `chat.typing` keeps the "typing…" indicator alive and
//! `chat.status` streams a live tool-use trace (see [`live`]).
//!
//! **Authorization** is an optional per-chat allow-list ([`Acl`]): a message from
//! a chat not on the list is dropped at the edge — before the bus — so untrusted
//! input never reaches cognition. Listed chats get their trust gradient + role
//! stamped onto the envelope ([`ChannelMetadata`]). Constructed in code
//! ([`TelegramConnector::new`] / [`with_acl`](TelegramConnector::with_acl)) or
//! from a `type = "telegram"` manifest via [`factory`] — the secret token stays
//! in the environment, the ACL in a JSON state file named by the manifest.

mod acl;
mod api;
mod batch;
mod commands;
mod format;
mod fs;
mod live;

use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
    time::Duration,
};

use octo_core::{ConnectorCapabilities, ConnectorId, EventKind};

use crate::commands::SET_COMMANDS;

use self::source::{CommandCatalog, GroupSettings};
pub use acl::{Acl, AclEntry, GroupMode, Role};
mod config;
mod control;
mod inbound;
mod outbound;
mod runtime;
mod source;
#[cfg(test)]
use self::{
    inbound::{
        caption_with_saved, hms, inbox_name, reply_context, video_caption, video_media,
        voice_media, with_reply,
    },
    outbound::{Outbound, is_voice_blob, outbound_kind},
};
pub use config::{TelegramConnectorFactory, factory};

/// Default quiet window before a coalescing burst (album / forward) is flushed.
/// Short, because these bursts arrive machine-fast; single typed messages are
/// never buffered, so this adds no latency to normal chat.
const DEFAULT_DEBOUNCE: Duration = Duration::from_millis(500);
/// Default cap on how long a buffer may stay open (so a steady stream still
/// flushes) — see [`batch::Batcher`].
const DEFAULT_MAX_WAIT: Duration = Duration::from_secs(3);
/// How often the run loop checks for buffers due to flush.
const FLUSH_TICK: Duration = Duration::from_millis(100);

/// Control commands that mutate the ACL at runtime (an owner-instructed
/// cogitator dispatches these — the connector is a manageable actor, like the
/// scheduler). Each gets a correlated `<kind>.result` reply.
const ALLOW_CHAT: &str = "octo.telegram.allow_chat";
const REMOVE_CHAT: &str = "octo.telegram.remove_chat";
const LIST_CHATS: &str = "octo.telegram.list_chats";
const GROUP_MODE: &str = "octo.telegram.group_mode";

/// Outbound command: send a file from the shared workspace. Images go as a photo
/// (inline preview), everything else as a document; an optional `caption` rides along.
/// Payload `{ path, chat?, filename? }` — chat falls back to the envelope channel.
const SEND_FILE: &str = "chat.send_file";

/// Live-turn feedback kinds (see [`live`]): keep the "typing…" indicator alive
/// while the cogitator thinks, and stream its tool-use trace into an in-place
/// edited status message.
const TYPING: &str = "chat.typing";
const STATUS: &str = "chat.status";

/// What the runtime tells cognition about this channel — its commands, and how a
/// reply is rendered. The second half is the point: the model picks the shape of
/// its answer, and only the connector knows which shapes survive the trip.
///
/// Note that a chat channel opting into the env-as-tools catalogue stretches what
/// [`ConnectorCapabilities::description`] has meant so far ("I am an agent-callable
/// tool"); here it also says "I am an environment, and this is how to speak in it".
///
/// Two deliberate omissions:
///
/// - The `octo.telegram.*` ACL commands. They still work when dispatched, but the
///   allow-list is this connector's security edge and an inbound message is
///   untrusted input — telling the model how to grant chat access would turn a
///   prompt injection into privilege escalation.
/// - Anything about *who* is on the other end. This string reaches a model and,
///   through it, a chat log; chat ids, the allow-list, the workspace path and the
///   token all stay out of it. It is a `const` so it cannot drift into carrying
///   them — see `catalog_is_static_and_holds_no_deployment_detail`.
const CATALOG: &str = "A chat channel with a person — conversation, not a tool call. A reply is a \
`chat.reply` envelope carrying the chat id on its channel; these commands are accepted too:
- chat.send_file { path, chat?, filename?, caption? } -> send a file from the shared workspace (an image as a photo, anything else as a document)
- chat.typing -> hold the \"typing…\" indicator while a turn runs
- chat.status \"<line>\" -> append a line to the turn's progress trace, which is deleted when the reply lands

Reply text is Markdown and Telegram lays it out, so structure survives the trip: \
headings, ordered/unordered lists, task lists, tables, block quotes, fenced code with \
a language, thematic breaks, **bold**, *italic*, ~~strikethrough~~, `code`, ==marked==, \
||spoiler|| and links all render as themselves. Answer tabular things with a table — it \
arrives as a table, not as ASCII art. Raw HTML is shown literally rather than parsed: \
write `<details>` or `<b>` and the reader sees the tag, so reach for Markdown instead. \
Table cells carry inline formatting only. A long reply is split between top-level \
blocks on the way out, so there is no need to chunk it by hand.";

/// Shared, mutable ACL state: the list behind a lock + where it persists.
struct AclState {
    acl: RwLock<Acl>,
    /// JSON file to persist to on mutation (`None` → in-memory only).
    path: Option<PathBuf>,
}

pub struct TelegramConnector {
    id: ConnectorId,
    capabilities: ConnectorCapabilities,
    token: String,
    /// Channel allow-list. `None` → no authorization (allow all — the
    /// console-fallback / playground path). `Some` → messages from unlisted
    /// chats are dropped at the edge, listed chats get `trust`/`role` stamped,
    /// and `octo.telegram.*` control commands mutate it at runtime.
    acl: Option<Arc<AclState>>,
    /// Coalescing window: a chat's rapid burst of messages is buffered and
    /// flushed as one input after `debounce` of quiet (capped at `max_wait`), so
    /// forwarding several messages yields one turn, not one per message. A zero
    /// `debounce` disables coalescing (publish each message immediately).
    debounce: Duration,
    max_wait: Duration,
    /// Shared workspace root for file transfer (inbound documents saved here,
    /// `chat.send_file` reads from here). `None` → resolved from the environment
    /// at use, matching octo-code. See [`fs`].
    workspace: Option<PathBuf>,
    groups: GroupSettings,
    commands: Arc<RwLock<CommandCatalog>>,
}

impl TelegramConnector {
    /// Open with no access control — every chat is allowed. Fine for a local
    /// playground; a real deployment uses [`with_acl`](Self::with_acl).
    pub fn new(id: impl Into<String>, token: impl Into<String>) -> Arc<Self> {
        Self::build(id, token, None, DEFAULT_DEBOUNCE, DEFAULT_MAX_WAIT, None)
    }

    /// Open gated by an access-control list: a message from a chat not on the
    /// list is dropped before it reaches the bus (so untrusted input never
    /// reaches cognition). `acl_path`, when set, is where runtime mutations
    /// (`allow_chat` / `remove_chat`) are persisted.
    pub fn with_acl(
        id: impl Into<String>,
        token: impl Into<String>,
        acl: Acl,
        acl_path: Option<PathBuf>,
    ) -> Arc<Self> {
        let state = Arc::new(AclState {
            acl: RwLock::new(acl),
            path: acl_path,
        });
        Self::build(
            id,
            token,
            Some(state),
            DEFAULT_DEBOUNCE,
            DEFAULT_MAX_WAIT,
            None,
        )
    }

    fn build(
        id: impl Into<String>,
        token: impl Into<String>,
        acl: Option<Arc<AclState>>,
        debounce: Duration,
        max_wait: Duration,
        workspace: Option<PathBuf>,
    ) -> Arc<Self> {
        let capabilities = ConnectorCapabilities::bidirectional()
            .with_emit_kinds([EventKind::from_static("chat.message")])
            .with_accept_kinds([
                EventKind::from_static("chat.reply"),
                EventKind::from_static(SEND_FILE),
                EventKind::from_static(TYPING),
                EventKind::from_static(STATUS),
                EventKind::from_static(ALLOW_CHAT),
                EventKind::from_static(REMOVE_CHAT),
                EventKind::from_static(LIST_CHATS),
                EventKind::from_static(GROUP_MODE),
                EventKind::from_static(SET_COMMANDS),
            ])
            .with_description(CATALOG);
        Arc::new(Self {
            id: ConnectorId::new(id),
            capabilities,
            token: token.into(),
            acl,
            debounce,
            max_wait,
            workspace,
            groups: GroupSettings::default(),
            commands: Arc::new(RwLock::new(CommandCatalog::default())),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use octo_core::{Blob, Connector, ConnectorFactory, FactoryContext};

    /// A Telegram `Message` from its wire JSON, so the media classifiers can be
    /// tested against what the API actually sends.
    fn message(media: &str) -> teloxide::types::Message {
        let json = format!(
            r#"{{"message_id":1,"date":1700000000,
                 "chat":{{"id":42,"type":"private","first_name":"T"}},
                 "from":{{"id":42,"is_bot":false,"first_name":"T"}},{media}}}"#
        );
        serde_json::from_str(&json).expect("valid Telegram message JSON")
    }

    /// The catalogue reaches a model, and through it a chat log and whatever the
    /// model writes next. Nothing about *this* deployment may ride along, so it
    /// must not vary with the token, the allow-list, or where that list is kept.
    #[test]
    fn catalog_is_static_and_holds_no_deployment_detail() {
        let mut acl = Acl::new();
        acl.insert(424242, Role::Owner);
        let configured = TelegramConnector::with_acl(
            "telegram",
            "111222:SECRETTOKEN",
            acl,
            Some(PathBuf::from("/home/someone/private-acl.json")),
        );
        let bare = TelegramConnector::new("telegram", "333444:OTHERTOKEN");

        let described = configured.capabilities().description.as_deref();
        assert_eq!(described, bare.capabilities().description.as_deref());
        assert_eq!(described, Some(CATALOG));

        let catalog = described.expect("the channel describes itself");
        for private in ["424242", "SECRETTOKEN", "someone", "private-acl"] {
            assert!(!catalog.contains(private), "catalogue leaked {private}");
        }
        // The ACL commands stay dispatchable but undocumented — see CATALOG.
        for hidden in [ALLOW_CHAT, REMOVE_CHAT, LIST_CHATS] {
            assert!(!catalog.contains(hidden), "catalogue advertises {hidden}");
        }
    }

    #[test]
    fn voice_note_is_recognized_with_its_duration() {
        // A voice note carries no file_name, and its MIME may be null — both defaulted.
        let msg = message(
            r#""voice":{"file_id":"AwACF","file_unique_id":"u1","file_size":4242,
                        "duration":7,"mime_type":null}"#,
        );
        let media = voice_media(&msg).expect("a voice note is inbound audio");
        assert_eq!(media.file_id.0, "AwACF");
        assert_eq!(media.mime, "audio/ogg");
        assert_eq!(media.filename, "voice.ogg");
        assert_eq!(media.duration_secs, 7);
    }

    #[test]
    fn audio_file_keeps_its_own_mime_and_name() {
        let msg = message(
            r#""audio":{"file_id":"CQACG","file_unique_id":"u2","file_size":99,"duration":183,
                       "file_name":"lecture.m4a","mime_type":"audio/mp4"}"#,
        );
        let media = voice_media(&msg).expect("an audio file is inbound audio");
        assert_eq!(media.mime, "audio/mp4");
        assert_eq!(media.filename, "lecture.m4a");
        assert_eq!(media.duration_secs, 183);
    }

    #[test]
    fn video_clip_is_recognized_with_name_length_and_thumbnail() {
        let msg = message(
            r#""video":{"file_id":"BAACV","file_unique_id":"u4","file_size":900,"duration":95,
                       "width":1920,"height":1080,"file_name":"clip.mp4","mime_type":"video/mp4",
                       "thumbnail":{"file_id":"THUMB","file_unique_id":"u5","file_size":2,
                                    "width":320,"height":180}}"#,
        );
        let media = video_media(&msg).expect("a video is inbound video");
        assert_eq!(media.file_id.0, "BAACV");
        assert_eq!(media.filename, "clip.mp4");
        assert_eq!(media.duration_secs, 95);
        assert!(!media.is_note);
        assert_eq!(
            media
                .thumbnail
                .expect("Telegram sent a poster frame")
                .file
                .id
                .0,
            "THUMB"
        );
    }

    #[test]
    fn video_note_is_recognized_and_named() {
        // A round video note carries neither file_name nor mime_type.
        let msg = message(
            r#""video_note":{"file_id":"DQACN","file_unique_id":"u6","file_size":700,
                            "duration":8,"length":384}"#,
        );
        let media = video_media(&msg).expect("a video note is inbound video");
        assert_eq!(media.filename, "video_note.mp4");
        assert_eq!(media.duration_secs, 8);
        assert!(media.is_note);
        assert!(media.thumbnail.is_none());
    }

    #[test]
    fn video_caption_carries_the_workspace_path_and_the_senders_words() {
        let msg = message(
            r#""video":{"file_id":"B","file_unique_id":"u7","file_size":1,"duration":62,
                       "width":2,"height":2,"file_name":"clip.mp4","mime_type":"video/mp4"}"#,
        );
        let media = video_media(&msg).unwrap();

        // The path is the handle every ffmpeg/transcription tool needs, and the
        // sender's own words must survive alongside it.
        let with_caption =
            video_caption(&media, Some("посмотри".into()), Some("inbox/17-clip.mp4"));
        assert!(with_caption.starts_with("посмотри\n\n"));
        assert!(with_caption.contains("saved to workspace path `inbox/17-clip.mp4`"));
        assert!(with_caption.contains("(1:02)"), "length is rendered m:ss");

        // No caption → just the note; no saved path → no invented path.
        assert!(video_caption(&media, None, Some("inbox/a.mp4")).starts_with("[received video"));
        let unsaved = video_caption(&media, None, None);
        assert!(unsaved.contains("could not save"));
        assert!(!unsaved.contains("workspace path `"));
    }

    #[test]
    fn hms_renders_minutes_and_only_then_hours() {
        assert_eq!(hms(7), "0:07");
        assert_eq!(hms(62), "1:02");
        assert_eq!(hms(600), "10:00");
        assert_eq!(hms(3661), "1:01:01");
    }

    #[test]
    fn text_photo_and_voice_are_not_video() {
        assert!(video_media(&message(r#""text":"привет""#)).is_none());
        let photo = message(
            r#""photo":[{"file_id":"P","file_unique_id":"u8","file_size":1,"width":1,"height":1}]"#,
        );
        assert!(video_media(&photo).is_none());
        let voice = message(
            r#""voice":{"file_id":"V","file_unique_id":"u9","file_size":1,"duration":3,
                       "mime_type":null}"#,
        );
        assert!(video_media(&voice).is_none());
    }

    #[test]
    fn outgoing_files_are_classified_by_extension() {
        assert_eq!(outbound_kind("cover.PNG"), Outbound::Photo);
        assert_eq!(outbound_kind("shot.jpeg"), Outbound::Photo);
        assert_eq!(outbound_kind("reply.ogg"), Outbound::Voice);
        assert_eq!(outbound_kind("reply.oga"), Outbound::Voice);
        assert_eq!(outbound_kind("speech.opus"), Outbound::Voice);
        // Telegram rejects these as voice notes, so they stay documents.
        assert_eq!(outbound_kind("song.mp3"), Outbound::Document);
        assert_eq!(outbound_kind("lecture.m4a"), Outbound::Document);
        assert_eq!(outbound_kind("report.pdf"), Outbound::Document);
    }

    #[test]
    fn voice_blob_is_ogg_by_mime_or_name() {
        assert!(is_voice_blob(&Blob::new(vec![1], "audio/ogg")));
        assert!(is_voice_blob(&Blob::new(vec![1], "audio/ogg; codecs=opus")));
        // A generic MIME with an .ogg name still goes out as a voice note.
        assert!(is_voice_blob(
            &Blob::new(vec![1], "application/octet-stream").with_filename("say.ogg")
        ));
        assert!(!is_voice_blob(
            &Blob::new(vec![1], "audio/mpeg").with_filename("song.mp3")
        ));
        assert!(!is_voice_blob(
            &Blob::new(vec![1], "image/png").with_filename("a.png")
        ));
    }

    #[test]
    fn text_and_photo_are_not_audio() {
        assert!(voice_media(&message(r#""text":"привет""#)).is_none());
        let photo = message(
            r#""photo":[{"file_id":"P","file_unique_id":"u3","file_size":1,"width":1,"height":1}]"#,
        );
        assert!(voice_media(&photo).is_none());
    }

    #[test]
    fn factory_builds_connector_from_manifest() {
        // Unique env var so the test can't collide with a real OCTO_TELEGRAM_TOKEN.
        unsafe { std::env::set_var("TG_FACTORY_TEST_TOKEN", "123:abc") };
        let manifest = r#"
            [connector]
            id = "telegram"
            type = "telegram"
            token_env = "TG_FACTORY_TEST_TOKEN"
            owner_chat = 42
        "#;
        let value: toml::Value = toml::from_str(manifest).unwrap();
        let factory = TelegramConnectorFactory;
        assert_eq!(factory.type_name(), "telegram");
        let conn = factory
            .create(
                ConnectorId::new("telegram"),
                &value,
                FactoryContext {
                    base_dir: std::path::Path::new("."),
                },
            )
            .expect("factory builds the connector");
        assert_eq!(conn.id().as_str(), "telegram");
    }

    #[test]
    fn factory_errors_when_token_env_unset() {
        let manifest = r#"
            [connector]
            id = "telegram"
            type = "telegram"
            token_env = "TG_DEFINITELY_UNSET_VAR_XYZ"
        "#;
        let value: toml::Value = toml::from_str(manifest).unwrap();
        let result = TelegramConnectorFactory.create(
            ConnectorId::new("telegram"),
            &value,
            FactoryContext {
                base_dir: std::path::Path::new("."),
            },
        );
        // `dyn Connector` isn't `Debug`, so match instead of `unwrap_err`.
        let err = match result {
            Ok(_) => panic!("expected an error when the token env var is unset"),
            Err(e) => e,
        };
        assert!(err.to_string().contains("not set"), "got: {err}");
    }

    #[test]
    fn caption_with_saved_folds_the_path_and_preserves_the_caption() {
        // No saved path (write failed): the caption passes through untouched.
        assert_eq!(
            caption_with_saved(Some("hi".into()), None),
            Some("hi".into())
        );
        assert_eq!(caption_with_saved(None, None), None);

        // A caption present: the saved-path note is appended, not replaced.
        let both =
            caption_with_saved(Some("what is this?".into()), Some("inbox/1-photo.jpg")).unwrap();
        assert!(both.starts_with("what is this?"));
        assert!(both.contains("inbox/1-photo.jpg"));

        // No caption: the note stands alone (so the file is still discoverable).
        assert_eq!(
            caption_with_saved(None, Some("inbox/1-photo.jpg")),
            Some("[image saved to workspace path `inbox/1-photo.jpg`]".into())
        );
    }

    #[test]
    fn a_reply_carries_the_quoted_message_into_context() {
        // Reply to a text message: the quoted text rides into the context line.
        let to_text = message(
            r#""text":"and the deadline?","reply_to_message":{"message_id":7,"date":1699999999,
                 "chat":{"id":42,"type":"private","first_name":"T"},
                 "from":{"id":9,"is_bot":true,"first_name":"Albert"},
                 "text":"The report is due Friday."}"#,
        );
        let ctx = reply_context(&to_text).expect("this message is a reply");
        assert!(ctx.contains("The report is due Friday."), "got: {ctx}");
        assert!(ctx.starts_with("[replying to \""));

        // A message that is not a reply → no context.
        assert!(reply_context(&message(r#""text":"hi""#)).is_none());

        // Reply to media: named, never downloaded.
        let to_photo = message(
            r#""text":"who is this?","reply_to_message":{"message_id":8,"date":1699999999,
                 "chat":{"id":42,"type":"private","first_name":"T"},
                 "from":{"id":9,"is_bot":false,"first_name":"T"},
                 "photo":[{"file_id":"P","file_unique_id":"u","file_size":1,"width":1,"height":1}]}"#,
        );
        assert_eq!(
            reply_context(&to_photo).as_deref(),
            Some("[replying to a photo]")
        );
    }

    #[test]
    fn with_reply_folds_context_onto_the_body() {
        let r = Some("[replying to \"x\"]".to_string());
        assert_eq!(
            with_reply(&r, Some("hi".into())).unwrap(),
            "[replying to \"x\"]\nhi"
        );
        // An empty body leaves the context standing alone (a bare reply, e.g. a sticker).
        assert_eq!(
            with_reply(&r, Some("  ".into())).unwrap(),
            "[replying to \"x\"]"
        );
        assert_eq!(with_reply(&r, None).unwrap(), "[replying to \"x\"]");
        // No reply → the body passes through untouched.
        assert_eq!(with_reply(&None, Some("hi".into())), Some("hi".into()));
        assert_eq!(with_reply(&None, None), None);
    }

    #[test]
    fn inbox_name_is_basenamed_and_timestamped() {
        // A crafted path is reduced to its basename (no escaping the inbox).
        assert!(inbox_name("../../etc/passwd").ends_with("-passwd"));
        assert!(!inbox_name("../../etc/passwd").contains('/'));

        // The timestamp prefix keeps same-named files (every photo is `photo.jpg`)
        // from clobbering each other.
        let a = inbox_name("photo.jpg");
        assert!(a.ends_with("-photo.jpg"));
        assert!(a.len() > "-photo.jpg".len());
    }
}
