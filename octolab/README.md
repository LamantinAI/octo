# octolab

Playground for exercising the **Octo runtime** with a real **ReAct LLM agent**.
A workspace member of `octo/` (the runtime); the vault lives in `research/`.

The point is to test the runtime as the substrate for an online agent: a real
LLM cogitator decides, connectors are pluggable/auditable transport (and the
agent's action space — env-as-tools), and the reply is routed by the
recommendation carried on the incoming envelope.

## Stack

- **Runtime:** `octo-core` (workspace path dependency).
- **LLM:** [`rig`](https://github.com/0xPlaygrounds/rig) — `openrouter` provider,
  native tool-calling via the `octo-rig` `dispatch_to_connector` tool.
- **Telegram:** `octo-connector-telegram` (official connector, teloxide long
  polling) when `OCTO_TELEGRAM_TOKEN` is set; console fallback otherwise.
- **Tool connector:** `octo-connector-http` driving the petstore manifest.

## Configuration

Reads the **repo-root `.env`** (anchored on the crate manifest, so cwd doesn't
matter):

```
OCTO_OPENAI_KEY=...                         # put your key here before running
OCTO_LLM_BASE_URL=https://openrouter.ai/api/v1
OCTO_LLM_MODEL=deepseek/deepseek-v4-flash
OCTO_TELEGRAM_TOKEN=...                      # optional; absent → console channel
OCTO_HISTORY=memory                          # or file:<dir>
```

## Run

```sh
cargo run -p octolab          # from the octo/ workspace root
# console mode: type a message, Ctrl-D to quit
```

Flow: inbound message → `chat.message` → `ReactCogitator` (rig native
tool-calling; may dispatch to a connector) → `chat.reply` routed back to the
source connector on the same channel/correlation. Telegram and console share
this exact envelope shape — only the `channel` differs (chat_id vs `stdin`).

## What's wired

- `ReactCogitator`: rig native tool-calling, per-channel history (memory/file),
  reflex fast-path for `/start` `/help` `/pic`, incoming-provenance front-loaded
  into the preamble.
- env-as-tools: the cogitator discovers connectors via runtime introspection
  (`ctx.connectors()`) and reaches them through one `dispatch_to_connector` tool.
- Supervision: in-process (octo-core restart policy) + whole-process via
  `octolab.service` (`Restart=always`).

## Notes

- `rig` is fast-moving; if a compile hits a renamed method, it's a small fix in
  `src/llm.rs`.
- Heavy (rig/LLM + teloxide), so octolab is **excluded from `default-members`** —
  a plain `cargo test` in the workspace stays light; build it explicitly with
  `-p octolab`.
