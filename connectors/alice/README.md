# `alice` — Yandex Alice skill connector

Makes a Yandex Alice skill (a Yandex Station speaker, the Alice app) a chat
channel of an Octo assembly. Yandex STT/TTS do the voice; the connector only
moves text.

```
Alice ── HTTPS POST /alice/<secret> ──▶ alice ── chat.message ──▶ bus ──▶ cognition
Alice ◀── {"response":{"text":…}} ──── alice ◀── chat.reply ───── bus ◀──
```

## Envelopes

- **Emits** `chat.message` (String) on channel `alice:<user_id>` (or
  `alice:app:<application_id>` for a signed-out device), with `reply_to` set and
  `channel_metadata`: trust from `role`, tags `role`, `chat_type = "voice"`,
  `chat_id`, `sender_id`, `sender_name`. `chat_type = "voice"` is the hint for the
  assembly to answer briefly and without Markdown.
- **Accepts** `chat.reply` addressed to the connector (`target`), String or Blob.
  Text is converted to speech-friendly form (Markdown, links and code removed,
  list items become sentences) and split into ≤ `max_chars` pieces. `chat.typing`,
  `chat.status` and `chat.send_file` are ignored.

## Timing: fillers and the speaker's own voice

Alice waits about 3 seconds for a webhook. A request that starts a turn waits up
to `reply_wait_ms`; a reply that is in by then is spoken right away.

**With `[connector.push]`** (recommended): otherwise the request answers with a
random `fillers` phrase and ends the session; when the reply comes, the speaker
says it by itself through the Yandex smart-home cloud ("произнести текст" in a
scenario — the same mechanism the Home Assistant integration AlexxIT/YandexStation
uses). That path is not a public API: it needs an x-token of the Yandex account
that owns the speaker (one-time QR login: `tools/yandex_qr_login.py`), takes at most
100 characters per utterance (longer text is said in a row of pieces, paced by an
estimate — the cloud reports no playback state), and may change without notice.
Unsolicited replies (a reminder firing) are spoken the same way. If the cloud
call fails, the text is parked as below.

**Without it**: the filler is the `thinking` phrase and the reply is parked per
speaker until they say one of `continue_words` («дальше»); long replies come one
piece per «дальше». Continue words never reach the bus — a new message would
interrupt the running turn.

## Access

- Unguessable path secret (`secret_env`) — Yandex does not sign requests.
- Optional `skill_id` match.
- Allow-list of `allowed_users` / `allowed_applications`; an empty list refuses
  everyone and logs the ids it saw.
- Yandex's `ping` health check is answered without touching the bus.

## Setup

1. Register the factory: `.register_connector_type("alice", octo_connector_alice::factory())`.
2. Copy [`alice.toml`](alice.toml) into the connectors dir, set
   `ALICE_WEBHOOK_SECRET`.
3. Expose `listen` through an HTTPS proxy with a valid certificate.
4. Optional, for the speaker's own voice: `uv run tools/yandex_qr_login.py
   --env-file <your .env>` (or `--ssh user@host:/path/.env`), scan the QR with the
   Yandex app, add `[connector.push]` to the manifest. The connector logs the
   account's speakers at startup; set `device` if there is more than one.
5. In the [Yandex Dialogs console](https://dialogs.yandex.ru/developer) create an
   Alice skill, set the webhook URL `https://<host>/alice/<secret>`, mark it
   private, and test. Say something to it, then copy your `user_id` from the
   connector's log into `allowed_users`.
