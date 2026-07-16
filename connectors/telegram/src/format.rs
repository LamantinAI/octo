//! Render the Markdown that LLMs emit into the small HTML subset Telegram's
//! `parse_mode=HTML` understands, so replies show as formatted text instead of
//! raw `**`/`#`/`-` and broken tables.
//!
//! Telegram supports only a handful of inline tags (`b i u s a code pre
//! blockquote`) and **no** block structures — no headings, lists, or tables. So
//! headings become bold lines, list items get `•`/`N.` bullets, and Markdown
//! tables (which Telegram can't render at all) are laid out as a monospace grid
//! inside `<pre>`. Anything we can't map is degraded to plain text rather than
//! emitted as an unsupported tag that would make the Bot API reject the message.

use pulldown_cmark::{Event, Options, Parser, Tag, TagEnd};

/// Telegram's per-message ceiling is 4096 UTF-16 units; stay under it with margin.
const MAX_LEN: usize = 4000;

/// Convert Markdown to Telegram-flavoured HTML.
pub fn to_telegram_html(md: &str) -> String {
    let mut opts = Options::empty();
    opts.insert(Options::ENABLE_TABLES);
    opts.insert(Options::ENABLE_STRIKETHROUGH);
    let mut r = Renderer::default();
    for ev in Parser::new_ext(md, opts) {
        r.event(ev);
    }
    r.finish()
}

/// Split rendered output into chunks Telegram will accept, breaking on line
/// boundaries (so inline tags aren't cut) and hard-splitting any single
/// over-long line as a last resort.
pub fn split_for_telegram(s: &str) -> Vec<String> {
    if s.chars().count() <= MAX_LEN {
        return vec![s.to_string()];
    }
    let mut chunks = Vec::new();
    let mut cur = String::new();
    for line in s.split_inclusive('\n') {
        if line.chars().count() > MAX_LEN {
            if !cur.is_empty() {
                chunks.push(std::mem::take(&mut cur));
            }
            let mut buf = String::new();
            for c in line.chars() {
                if buf.chars().count() + 1 > MAX_LEN {
                    chunks.push(std::mem::take(&mut buf));
                }
                buf.push(c);
            }
            cur = buf;
        } else if cur.chars().count() + line.chars().count() > MAX_LEN {
            chunks.push(std::mem::take(&mut cur));
            cur.push_str(line);
        } else {
            cur.push_str(line);
        }
    }
    if !cur.is_empty() {
        chunks.push(cur);
    }
    chunks
}

/// Strip HTML tags and unescape entities — the plain-text fallback for when
/// Telegram rejects the HTML (so a message always gets through).
pub fn strip_tags(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    for c in html.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    out.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&amp;", "&")
}

#[derive(Default)]
struct Renderer {
    out: String,
    /// One entry per open list; `Some(n)` = ordered (next number), `None` = bullet.
    lists: Vec<Option<u64>>,
    /// When set, inline text is routed into the current table cell instead of `out`.
    table: Option<Table>,
}

struct Table {
    rows: Vec<Vec<String>>,
    cur_row: Vec<String>,
    cell: String,
}

impl Renderer {
    fn event(&mut self, ev: Event) {
        match ev {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(tag),
            Event::Text(t) => self.push_text(&t),
            Event::Code(t) => {
                if self.table.is_some() {
                    self.push_text(&t);
                } else {
                    self.out.push_str("<code>");
                    self.out.push_str(&esc(&t));
                    self.out.push_str("</code>");
                }
            }
            Event::SoftBreak | Event::HardBreak => {
                if self.table.is_some() {
                    self.cell_push(' ');
                } else {
                    self.out.push('\n');
                }
            }
            Event::Rule => {
                self.block_gap();
                self.out.push_str("———\n");
            }
            Event::TaskListMarker(done) => self.out.push_str(if done { "☑ " } else { "☐ " }),
            // Raw HTML from the model would break Telegram's parser — show it literally.
            Event::Html(h) | Event::InlineHtml(h) => self.push_text(&h),
            _ => {}
        }
    }

    fn start(&mut self, tag: Tag) {
        // Inside a table, only structural tags matter; inline formatting is
        // flattened to plain cell text.
        if self.table.is_some() {
            match tag {
                Tag::TableHead | Tag::TableRow => {
                    self.table.as_mut().unwrap().cur_row.clear();
                }
                Tag::TableCell => self.table.as_mut().unwrap().cell.clear(),
                _ => {}
            }
            return;
        }
        match tag {
            Tag::Paragraph => {
                // A blank-line gap between paragraphs, but not right after a
                // container just opened (e.g. `<blockquote>`) nor inside a list.
                if self.lists.is_empty() && !self.out.ends_with('>') {
                    self.block_gap();
                }
            }
            Tag::Heading { .. } => {
                self.block_gap();
                self.out.push_str("<b>");
            }
            Tag::BlockQuote(_) => {
                self.block_gap();
                self.out.push_str("<blockquote>");
            }
            Tag::CodeBlock(_) => {
                self.block_gap();
                self.out.push_str("<pre>");
            }
            Tag::List(start) => self.lists.push(start),
            Tag::Item => {
                self.out.push('\n');
                let depth = self.lists.len().saturating_sub(1);
                for _ in 0..depth {
                    self.out.push_str("  ");
                }
                match self.lists.last_mut() {
                    Some(Some(n)) => {
                        self.out.push_str(&format!("{n}. "));
                        *n += 1;
                    }
                    _ => self.out.push_str("• "),
                }
            }
            Tag::Emphasis => self.out.push_str("<i>"),
            Tag::Strong => self.out.push_str("<b>"),
            Tag::Strikethrough => self.out.push_str("<s>"),
            Tag::Link { dest_url, .. } => {
                self.out.push_str("<a href=\"");
                self.out.push_str(&esc_attr(&dest_url));
                self.out.push_str("\">");
            }
            // Telegram can't inline images; keep the alt text, linking to the source.
            Tag::Image { dest_url, .. } => {
                self.out.push_str("<a href=\"");
                self.out.push_str(&esc_attr(&dest_url));
                self.out.push_str("\">");
            }
            Tag::Table(_) => {
                self.table = Some(Table { rows: Vec::new(), cur_row: Vec::new(), cell: String::new() });
            }
            _ => {}
        }
    }

    fn end(&mut self, tag: TagEnd) {
        if self.table.is_some() {
            match tag {
                TagEnd::TableCell => {
                    let t = self.table.as_mut().unwrap();
                    let cell = t.cell.trim().to_string();
                    t.cur_row.push(cell);
                }
                TagEnd::TableHead | TagEnd::TableRow => {
                    let t = self.table.as_mut().unwrap();
                    let row = std::mem::take(&mut t.cur_row);
                    t.rows.push(row);
                }
                TagEnd::Table => {
                    let t = self.table.take().unwrap();
                    self.block_gap();
                    self.out.push_str(&render_table(&t));
                }
                _ => {}
            }
            return;
        }
        match tag {
            TagEnd::Paragraph => {
                if self.lists.is_empty() {
                    self.out.push('\n');
                }
            }
            TagEnd::Heading(_) => self.out.push_str("</b>\n"),
            TagEnd::BlockQuote(_) => self.out.push_str("</blockquote>\n"),
            TagEnd::CodeBlock => self.out.push_str("</pre>\n"),
            TagEnd::List(_) => {
                self.lists.pop();
                if self.lists.is_empty() {
                    self.out.push('\n');
                }
            }
            TagEnd::Emphasis => self.out.push_str("</i>"),
            TagEnd::Strong => self.out.push_str("</b>"),
            TagEnd::Strikethrough => self.out.push_str("</s>"),
            TagEnd::Link | TagEnd::Image => self.out.push_str("</a>"),
            _ => {}
        }
    }

    fn push_text(&mut self, t: &str) {
        if self.table.is_some() {
            self.table.as_mut().unwrap().cell.push_str(t);
        } else {
            self.out.push_str(&esc(t));
        }
    }

    fn cell_push(&mut self, c: char) {
        if let Some(t) = self.table.as_mut() {
            t.cell.push(c);
        }
    }

    /// Ensure the buffer ends with a blank line before a new block (collapsing
    /// any run of trailing newlines to exactly two).
    fn block_gap(&mut self) {
        if self.out.is_empty() {
            return;
        }
        while self.out.ends_with('\n') {
            self.out.pop();
        }
        self.out.push_str("\n\n");
    }

    fn finish(self) -> String {
        self.out.trim().to_string()
    }
}

/// Lay a parsed table out as an aligned monospace grid inside `<pre>` — the only
/// way to get a table-like rendering out of Telegram.
fn render_table(t: &Table) -> String {
    if t.rows.is_empty() {
        return String::new();
    }
    let cols = t.rows.iter().map(Vec::len).max().unwrap_or(0);
    let mut widths = vec![0usize; cols];
    for row in &t.rows {
        for (i, cell) in row.iter().enumerate() {
            widths[i] = widths[i].max(cell.chars().count());
        }
    }
    let mut out = String::from("<pre>");
    for (ri, row) in t.rows.iter().enumerate() {
        let mut line = String::new();
        for (i, width) in widths.iter().enumerate() {
            let cell = row.get(i).map(String::as_str).unwrap_or("");
            line.push_str(cell);
            line.push_str(&" ".repeat(width - cell.chars().count()));
            if i + 1 < cols {
                line.push_str(" │ ");
            }
        }
        out.push_str(&esc(line.trim_end()));
        out.push('\n');
        if ri == 0 {
            // Header underline: ───┼───┼───
            let sep: Vec<String> = widths.iter().map(|w| "─".repeat(*w)).collect();
            out.push_str(&esc(&sep.join("─┼─")));
            out.push('\n');
        }
    }
    out.push_str("</pre>");
    out
}

/// Escape text for an HTML body.
pub(crate) fn esc(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// Escape a URL for an `href="…"` attribute.
fn esc_attr(s: &str) -> String {
    esc(s).replace('"', "&quot;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inline_formatting() {
        assert_eq!(to_telegram_html("**bold** and *italic* and `code`"),
            "<b>bold</b> and <i>italic</i> and <code>code</code>");
    }

    #[test]
    fn heading_becomes_bold() {
        assert_eq!(to_telegram_html("## Today"), "<b>Today</b>");
    }

    #[test]
    fn bullets_and_numbers() {
        assert_eq!(to_telegram_html("- a\n- b"), "• a\n• b");
        assert_eq!(to_telegram_html("1. one\n2. two"), "1. one\n2. two");
    }

    #[test]
    fn link_rendered() {
        assert_eq!(to_telegram_html("[site](https://x.io)"),
            "<a href=\"https://x.io\">site</a>");
    }

    #[test]
    fn escapes_html_specials() {
        assert_eq!(to_telegram_html("a < b & c > d"), "a &lt; b &amp; c &gt; d");
    }

    #[test]
    fn table_becomes_pre_grid() {
        let md = "| Meeting | Time |\n|---|---|\n| Docora | 10:15 |";
        let html = to_telegram_html(md);
        assert!(html.starts_with("<pre>"), "table wraps in <pre>: {html}");
        assert!(html.contains("Meeting │ Time"), "aligned header: {html}");
        assert!(html.contains("Docora  │ 10:15"), "padded cell: {html}");
        assert!(html.ends_with("</pre>"));
    }

    #[test]
    fn strip_tags_recovers_plain_text() {
        assert_eq!(strip_tags("<b>hi</b> &amp; <i>bye</i>"), "hi & bye");
    }

    #[test]
    fn splits_only_when_over_limit() {
        assert_eq!(split_for_telegram("short").len(), 1);
        let big = "x\n".repeat(5000);
        assert!(split_for_telegram(&big).iter().all(|c| c.chars().count() <= MAX_LEN));
    }
}

