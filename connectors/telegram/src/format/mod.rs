//! Preparing the model's Markdown for Telegram — two renderers, one contract.
//!
//! [`rich`] is the primary path. Bot API 10.1 rich messages accept
//! GitHub-flavoured Markdown directly (`sendRichMessage`), so headings, lists,
//! tables, fenced code and `<details>` render natively and nothing has to be
//! flattened; the module only sanitises what the model can't know about the
//! dialect.
//!
//! [`html`] is the fallback. It is the pre-10.1 rendering into the small HTML
//! subset `parse_mode=HTML` accepts — lossy (headings become bold lines, tables
//! become a monospace grid) but accepted by every Bot API server, so it stays as
//! the second rung under a rejected rich send, with plain text as the third.
//!
//! Both keep the same contract: never emit something the Bot API would reject,
//! and never let raw HTML from the model reach Telegram as live markup.

mod html;
mod rich;

pub(crate) use html::{esc, split_for_telegram, strip_tags, to_telegram_html};
pub(crate) use rich::{sanitize_rich, split_rich};
