//! Prepare the model's Markdown for a rich message (`sendRichMessage`).
//!
//! Rich Markdown is GitHub-flavoured, so a reply goes out almost untouched —
//! what a cogitator already writes (`##`, `- `, `| a | b |`, ```` ```python ````)
//! is exactly what Telegram lays out. Two things still need handling, because
//! they are properties of the 10.1 dialect the model has no way to know about:
//!
//! - **Raw HTML is live now.** Before 10.1 a stray `<b>` was escaped and shown
//!   literally; in a rich message it is parsed, and a tag outside Telegram's
//!   whitelist is dropped *together with its content* — a `<script>alert(1)</script>`
//!   in a probe reached the chat as nothing at all. Silently losing a sentence is
//!   worse than showing an angle bracket, so every raw-HTML run the parser reports
//!   is escaped back to literal text. That keeps the pre-10.1 contract: markup the
//!   model emits is content, not instructions to the renderer.
//! - **The ceiling moved** from 4096 UTF-16 units to 32768 characters, with a
//!   500-block structural limit beside it. Splitting therefore happens on
//!   top-level block boundaries — cutting a table or a `<details>` in half would
//!   leave both chunks malformed.
//!
//! Note that escaping does *not* extend to Telegram's automatic entity detection:
//! `#tag`, `/command` and `@name` are linkified inside a rich message and a
//! leading backslash does not stop it (probed — `\#tag` still arrived as a
//! hashtag). Only the `skip_entity_detection` flag does, and it would also kill
//! plain URLs, so it stays off.

use std::{mem::take, ops::Range};

use pulldown_cmark::{Event, Options, Parser};

/// The rich-message ceiling is 32768 characters. Gate on bytes — never fewer
/// than characters, whichever way the API counts — and leave margin.
const MAX_RICH_BYTES: usize = 32_000;

/// The ceiling is 500 blocks, counting nested ones, list items and table rows.
const MAX_RICH_BLOCKS: usize = 450;

/// GitHub-flavoured subset Telegram's rich Markdown is compatible with.
fn options() -> Options {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_TABLES);
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    opts
}

/// Escape the raw HTML the model emitted so Telegram shows it as text instead of
/// parsing it — or, for a tag it doesn't support, swallowing it whole.
pub fn sanitize_rich(md: &str) -> String {
    let mut spans: Vec<Range<usize>> = Vec::new();
    for (ev, range) in Parser::new_ext(md, options()).into_offset_iter() {
        if !matches!(ev, Event::Html(_) | Event::InlineHtml(_)) {
            continue;
        }
        // A multi-line HTML block arrives as one event per line; extend the open
        // span instead of accumulating a span per line.
        match spans.last_mut() {
            Some(last) if last.end == range.start => last.end = range.end,
            _ => spans.push(range),
        }
    }
    if spans.is_empty() {
        return md.to_string();
    }
    let mut out = String::with_capacity(md.len() + spans.len() * 8);
    let mut cut = 0;
    for span in spans {
        if span.start < cut {
            continue;
        }
        out.push_str(&md[cut..span.start]);
        out.push_str(&md[span.start..span.end].replace('<', "&lt;").replace('>', "&gt;"));
        cut = span.end;
    }
    out.push_str(&md[cut..]);
    out
}

/// Split Markdown into messages Telegram will accept, breaking between
/// top-level blocks so no table, list or `<details>` is cut in half.
pub fn split_rich(md: &str) -> Vec<String> {
    let blocks = top_level_blocks(md);
    if md.len() <= MAX_RICH_BYTES && blocks.iter().map(|b| b.1).sum::<usize>() <= MAX_RICH_BLOCKS {
        return vec![md.to_string()];
    }
    let mut chunks = Vec::new();
    let mut cur = String::new();
    let mut cur_blocks = 0;
    for (text, count) in blocks {
        if text.is_empty() {
            continue;
        }
        let over_bytes = cur.len() + "\n\n".len() + text.len() > MAX_RICH_BYTES;
        let over_blocks = cur_blocks + count > MAX_RICH_BLOCKS;
        if !cur.is_empty() && (over_bytes || over_blocks) {
            chunks.push(take(&mut cur));
            cur_blocks = 0;
        }
        // One block bigger than a whole message (a long table, a dumped file):
        // nothing to break between, so cut it by lines and accept the seam.
        if text.len() > MAX_RICH_BYTES {
            if !cur.is_empty() {
                chunks.push(take(&mut cur));
                cur_blocks = 0;
            }
            chunks.extend(hard_split(text));
            continue;
        }
        if !cur.is_empty() {
            cur.push_str("\n\n");
        }
        cur.push_str(text);
        cur_blocks += count;
    }
    if !cur.is_empty() {
        chunks.push(cur);
    }
    chunks
}

/// The document's top-level blocks, each with the number of blocks it holds
/// (itself plus everything nested — what the 500-block limit counts).
fn top_level_blocks(md: &str) -> Vec<(&str, usize)> {
    let mut spans: Vec<(Range<usize>, usize)> = Vec::new();
    let mut depth = 0usize;
    let mut open = 0usize;
    let mut count = 0usize;
    let mut after_html = false;
    for (ev, range) in Parser::new_ext(md, options()).into_offset_iter() {
        match ev {
            Event::Start(_) => {
                count += 1;
                if depth == 0 {
                    open = range.start;
                    after_html = false;
                }
                depth += 1;
            }
            Event::End(_) => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    spans.push((open..range.end, take(&mut count)));
                }
            }
            // A raw HTML block and a thematic break are leaves, never wrapped in
            // a Start/End pair — and an HTML block is reported line by line, so
            // consecutive runs merge back into the one block they came from.
            Event::Html(_) | Event::Rule if depth == 0 => {
                match spans.last_mut() {
                    Some((span, _)) if after_html && span.end == range.start => span.end = range.end,
                    _ => spans.push((range, 1)),
                }
                after_html = true;
            }
            _ => {}
        }
    }
    spans.into_iter().map(|(span, n)| (md[span.start..span.end].trim(), n)).collect()
}

/// Last resort for a single block past the ceiling: break on line boundaries,
/// and mid-line (on char boundaries) if even one line doesn't fit.
fn hard_split(block: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    for line in block.split_inclusive('\n') {
        if !cur.is_empty() && cur.len() + line.len() > MAX_RICH_BYTES {
            out.push(take(&mut cur));
        }
        if line.len() > MAX_RICH_BYTES {
            for c in line.chars() {
                if cur.len() + c.len_utf8() > MAX_RICH_BYTES {
                    out.push(take(&mut cur));
                }
                cur.push(c);
            }
        } else {
            cur.push_str(line);
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn markdown_passes_through_untouched() {
        let md = "## Heading\n\n- one\n- two\n\n| a | b |\n|---|---|\n| 1 | 2 |";
        assert_eq!(sanitize_rich(md), md);
    }

    #[test]
    fn raw_html_is_escaped_to_literal_text() {
        assert_eq!(sanitize_rich("text <b>x</b>"), "text &lt;b&gt;x&lt;/b&gt;");
        assert_eq!(
            sanitize_rich("<details><summary>s</summary>body</details>"),
            "&lt;details&gt;&lt;summary&gt;s&lt;/summary&gt;body&lt;/details&gt;"
        );
    }

    #[test]
    fn html_inside_code_is_left_alone() {
        // Fenced code is text, not markup — escaping it would show `&lt;` to the user.
        let md = "```html\n<b>x</b>\n```";
        assert_eq!(sanitize_rich(md), md);
        assert_eq!(sanitize_rich("`<b>`"), "`<b>`");
    }

    #[test]
    fn short_message_is_not_split() {
        assert_eq!(split_rich("## Hi\n\nbody").len(), 1);
    }

    #[test]
    fn split_breaks_between_blocks_and_respects_the_ceiling() {
        let para = format!("{}\n\n", "слово ".repeat(200));
        let md = para.repeat(60); // well past 32000 bytes in UTF-8
        let chunks = split_rich(&md);
        assert!(chunks.len() > 1, "expected a split, got {}", chunks.len());
        assert!(chunks.iter().all(|c| c.len() <= MAX_RICH_BYTES));
    }

    #[test]
    fn split_keeps_a_table_whole() {
        let mut md = String::from("| a | b |\n|---|---|\n");
        for i in 0..600 {
            md.push_str(&format!("| строка номер {i} | значение {i} |\n"));
        }
        md.push_str("\ntail paragraph\n");
        let chunks = split_rich(&md);
        let table_chunks = chunks.iter().filter(|c| c.contains("| строка номер 0 |")).count();
        assert_eq!(table_chunks, 1, "the table stayed in one piece");
        assert!(chunks.iter().any(|c| c.contains("tail paragraph")));
    }

    #[test]
    fn oversized_single_block_is_cut_rather_than_dropped() {
        let md = "x".repeat(MAX_RICH_BYTES * 2 + 10);
        let chunks = split_rich(&md);
        assert!(chunks.len() >= 3);
        assert!(chunks.iter().all(|c| c.len() <= MAX_RICH_BYTES));
        assert_eq!(chunks.concat().len(), md.len());
    }
}
