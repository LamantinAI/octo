# Telegram group access and provenance

Telegram emits `chat.message` envelopes with transport-derived `channel_metadata`
tags: `chat_id`, `chat_type`, `message_id`, `sender_id` (when known), optional
`sender_name`, `sender_username`, `chat_title`, `sender_chat_id`, plus `addressed`,
`forwarded`, `role`, `acl_admin`, `bootstrap_only` and `group_mode` when applicable.
Anonymous/channel posts never inherit the synthetic Telegram sender's privileges.
A forwarding user's identity is separate from the quoted original author.

With an ACL, a group and its sender must both be allowed. Persisted group mode
`all` also admits other participants as `Role::Guest` / low trust. Legacy ACL JSON
loads with mode `allowed`. `owner_chat` identifies an owner user; optional manifest
`admins = [123456789]` grants ACL management, not owner status. A group added by an
owner/admin is automatically allowed; other add events do not grant access.
Telegram migration service events transfer group access/mode to the new ID.

The connector reports addressing; **the assembly decides whether to think**.
It recognizes Telegram mention entities, replies to the bot, name prefixes from
`address_names` (plus the bot's first name), registered bare commands, and explicit
`/command@this_bot`. Commands addressed to another bot are dropped. Forwarded
content does not count as name/mention addressing. Albums and forwarded bursts
are keyed by chat and sender; an album keeps the addressing signal from its caption.

At startup the assembly publishes `chat.set_commands` with envelope tag
`control_plane=true`. `commands` and `owner_commands` supply the recognized names;
optional `bootstrap_commands` is a subset allowed for owner/admin in unlisted
groups. Such events carry `bootstrap_only=true`; the assembly must handle them
as deterministic administration, never as cognition.

Control kinds: `octo.telegram.allow_chat`, `.remove_chat`, `.list_chats`,
`.group_mode`. Changes require the actual actor in envelope metadata `sender_id`;
a JSON payload claiming an owner role is insufficient. Group mode additionally
requires owner status and payload `{ "chat_id": -42, "mode": "all" }` (or
`"allowed"`). Mutation is applied in memory only after persistence succeeds.
These are trusted host control messages, not model-dispatch capabilities.

Outbound messages, status and files require an allowed destination; addressing
is irrelevant to scheduled deliveries. A deterministic host acknowledgement may
use envelope tag `control_reply=true` with original sender/chat metadata to answer
bootstrap commands or confirm revocation. It cannot target a different chat.
No ACL configured retains the standalone connector's allow-all behavior.

## Per-reply Markdown rendering

The host can attach `ClassicMarkdown` to a reply's context to use native Telegram
formatted text instead of rich-message blocks. Markdown becomes the supported
inline HTML subset, with explicit newline gaps between paragraphs. This is useful
for bounded control notices whose literal spacing matters. The preference applies
to that reply only; it does not disable rich rendering for subsequent answers.

## Host inline controls

An assembly can attach an `InlineKeyboard` to the context of a string
`chat.reply`. `InlineButton::new(label, data)` validates a single-line label of
at most 64 characters and callback data of 1–64 bytes; `InlineKeyboard::new(rows)`
bounds the rows and button count. These are host types, never reconstructed from
model JSON or envelope tags. The `oa:` callback namespace is reserved for the
existing approval subsystem.

A marked reply is one literal plain-text message with its keyboard. It retains
the normal outbound ACL, host delivery checks and native delivery observations.
Invalid or oversized text fails before sending; it is not split with a missing
keyboard or retried through another renderer. Ordinary rich replies and approval
cards keep their existing rendering paths.

`TelegramHostPolicy::callback` receives a `CallbackFrame` with transport-derived
connector, bot, update/query IDs, clicker, message facts and opaque data. Message
accessibility is explicit. Missing or inline-only messages without a known chat,
malformed data and ACL refusals are acknowledged without invoking host policy.
The frame grants no native tool authority. The host must check its own resource
access and idempotency; the button message's date/ID is not a new click identity.

The hook returns a bounded `CallbackDecision`: an optional acknowledgement,
alert flag and post-events. It cannot manufacture `chat.message` input. Polling
awaits callback admission before handling the next update, so subsequent native
input observes the committed host state. Both the hook and acknowledgement have
deadlines; defaults are five seconds each, adjustable through `callback_limits`.
Acknowledgement failure does not discard committed post-events or trigger an
automatic retry. Post-events still pass normal host publication/delivery fences.
`oa:` callbacks remain exclusively with the approval handler.

## File delivery results

`chat.send_file` is a request/reply operation. The connector advertises
`chat.send_file.result` and emits one result correlated to the request ID on every
path, including ACL denial and invalid input. Destination is the payload's explicit
numeric `chat`, otherwise the envelope's host-bound channel; explicit invalid
values are rejected rather than falling back to another destination.

Results contain `ok` and `status`: `sent` includes `chat_id`, `message_id` and
`sent` (workspace path), `not_sent` indicates validation or a definite Telegram
rejection, and `unknown` indicates an unconfirmed transport outcome. Uploads have
a 90-second timeout and are not retried automatically. An unknown outcome needs
verification before another send to avoid duplicate photos/documents.

Assemblies using `octo-rig` can bind conversation context with
`OctoDispatchTool::with_channel_for(connector_id, channel_id)`. The binding is
only a default for that connector. The dispatch tool's optional `channel` overrides
it explicitly, so cross-channel and cross-connector delivery remains supported.
`with_origin` separately attaches host-provided `origin.connector` / `origin.channel`
provenance tags, regardless of the destination. Envelope `source` stays the actual
publisher, and no sender ACL metadata is copied or conferred by provenance.
`SendFileTool::with_confirmation_timeout` opts into acknowledgements for supporting
connectors. Legacy fire-and-forget mode reports `queued`, never falsely `sent`.
