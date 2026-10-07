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

## Timing: «дальше»

Alice waits about 3 seconds for a webhook. A request that starts a turn waits up
to `reply_wait_ms`; if the reply is not in yet it answers «Думаю. Скажите
«дальше»…». The reply is parked per speaker and spoken when they say one of
`continue_words`. Long replies are handed out one piece per «дальше». Continue
words never reach the bus — a new message would interrupt the running turn.
An uncorrelated reply (e.g. a reminder) to the channel is parked the same way;
the next launch mentions it.

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
4. In the [Yandex Dialogs console](https://dialogs.yandex.ru/developer) create an
   Alice skill, set the webhook URL `https://<host>/alice/<secret>`, mark it
   private, and test. Say something to it, then copy your `user_id` from the
   connector's log into `allowed_users`.
