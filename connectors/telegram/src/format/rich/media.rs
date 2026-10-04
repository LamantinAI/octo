//! Media blocks in a rich message.
//!
//! A rich message lays out pictures, videos and audio as blocks of their own:
//! Markdown `![](https://… "caption")` for one item, and Telegram's own
//! `<tg-collage>` / `<tg-slideshow>` HTML for several side by side. The Markdown
//! form is not HTML and passes through untouched. The HTML form would be escaped
//! like any other raw HTML, so [`is_media_html`] names the exact subset that is
//! let through: those two containers, `<img src>` with an http(s) URL, and a
//! `<figcaption>` (with an optional `<cite>`). Anything else — another tag, an
//! extra attribute, a non-http source — keeps the escape.
//!
//! Telegram fetches each URL itself, and one it can't fetch fails the whole
//! message. [`media_as_links`] is the second try: every media block turned into a
//! plain link, so the text still arrives rich and the pictures stay one tap away.

use pulldown_cmark::{Event, Parser, Tag, TagEnd};

/// Containers and their parts that may stay live. Lowercase, compared lowercase.
const MEDIA_TAGS: [&str; 5] = ["tg-collage", "tg-slideshow", "img", "figcaption", "cite"];

/// How an `<img>` source opens, double- or single-quoted.
const IMG_SRC: [&str; 2] = ["src=\"", "src='"];

/// Whether a run of raw HTML consists only of media-block tags — and so is safe
/// to hand to Telegram as markup rather than escape. Text between the tags (a
/// caption) is fine; a run with no tag at all is not media.
pub(super) fn is_media_html(html: &str) -> bool {
    let mut rest = html;
    let mut tags = 0;
    while let Some(open) = rest.find('<') {
        let Some(close) = rest[open..].find('>') else {
            return false;
        };
        if !is_media_tag(&rest[open + 1..open + close]) {
            return false;
        }
        tags += 1;
        rest = &rest[open + close + 1..];
    }
    tags > 0
}

/// One tag's inside (between `<` and `>`): an allowed name, and attributes only
/// on `img` — exactly one, `src`, pointing at http(s).
fn is_media_tag(inner: &str) -> bool {
    let inner = inner.trim();
    let (closing, inner) = match inner.strip_prefix('/') {
        Some(rest) => (true, rest.trim_start()),
        None => (false, inner),
    };
    let inner = inner.strip_suffix('/').unwrap_or(inner).trim_end();
    let (name, attrs) = match inner.find(char::is_whitespace) {
        Some(i) => (&inner[..i], inner[i..].trim()),
        None => (inner, ""),
    };
    let name = name.to_ascii_lowercase();
    if !MEDIA_TAGS.contains(&name.as_str()) {
        return false;
    }
    if closing || name != "img" {
        return attrs.is_empty();
    }
    is_http_src(attrs)
}

/// `src="https://…"` (or single-quoted), nothing else.
fn is_http_src(attrs: &str) -> bool {
    let Some(value) = attrs.strip_prefix("src") else {
        return false;
    };
    let Some(value) = value.trim_start().strip_prefix('=') else {
        return false;
    };
    let value = value.trim_start();
    let Some(quote) = value.chars().next().filter(|c| *c == '"' || *c == '\'') else {
        return false;
    };
    let Some(url) = value[1..].strip_suffix(quote) else {
        return false;
    };
    !url.contains(quote)
        && !url.chars().any(char::is_whitespace)
        && (url.starts_with("https://") || url.starts_with("http://"))
}

/// Whether the Markdown carries any media block — an image, or a live media tag.
pub(crate) fn has_media(md: &str) -> bool {
    Parser::new(md).any(|ev| match ev {
        Event::Start(Tag::Image { .. }) => true,
        Event::Html(h) | Event::InlineHtml(h) => is_media_html(&h),
        _ => false,
    })
}

/// The same Markdown with every media block turned into a link: `![](url "c")`
/// becomes `[c](url)` (the URL itself when there is no caption), and a collage's
/// `<img src>` items become links in its place. Used to retry a message Telegram
/// rejected over a medium it could not fetch.
pub(crate) fn media_as_links(md: &str) -> String {
    let mut out = String::with_capacity(md.len());
    let mut cut = 0;
    let mut events = Parser::new(md).into_offset_iter().peekable();
    while let Some((ev, range)) = events.next() {
        match ev {
            Event::Start(Tag::Image {
                dest_url, title, ..
            }) => {
                // Skip the alt text up to the image's end; the span covers it all.
                for (inner, _) in events.by_ref() {
                    if matches!(inner, Event::End(TagEnd::Image)) {
                        break;
                    }
                }
                out.push_str(&md[cut..range.start]);
                let label = if title.is_empty() {
                    dest_url.as_ref()
                } else {
                    title.as_ref()
                };
                out.push_str(&format!("[{label}]({dest_url})"));
                cut = range.end;
            }
            Event::Html(h) | Event::InlineHtml(h) if is_media_html(&h) => {
                out.push_str(&md[cut..range.start]);
                for src in IMG_SRC {
                    let mut rest: &str = &h;
                    while let Some(i) = rest.find(src) {
                        let tail = &rest[i + src.len()..];
                        let quote = src.chars().last().unwrap_or('"');
                        let end = tail.find(quote).unwrap_or(tail.len());
                        out.push_str(&format!("{}\n", &tail[..end]));
                        rest = &tail[end..];
                    }
                }
                cut = range.end;
            }
            _ => {}
        }
    }
    out.push_str(&md[cut..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collage_tags_are_media() {
        assert!(is_media_html(
            "<tg-collage><img src=\"https://a.org/1.jpg\"/><img src='https://a.org/2.jpg'>"
        ));
        assert!(is_media_html("</figcaption></tg-collage>"));
        assert!(is_media_html(
            "<figcaption>Two vases<cite>Commons</cite></figcaption>"
        ));
        assert!(is_media_html("<tg-slideshow>"));
    }

    #[test]
    fn anything_else_is_not() {
        assert!(!is_media_html("<b>bold</b>"));
        assert!(!is_media_html(
            "<img src=\"https://a.org/1.jpg\" onerror=\"x\">"
        ));
        assert!(!is_media_html("<img src=\"javascript:alert(1)\">"));
        assert!(!is_media_html("<img src=\"file:///etc/passwd\">"));
        assert!(!is_media_html("<tg-collage class=\"x\">"));
        assert!(!is_media_html("<tg-collage><script>x</script>"));
        assert!(!is_media_html("plain text"));
        assert!(!is_media_html("<img src=\"https://a.org/1.jpg\""));
    }

    #[test]
    fn detects_media() {
        assert!(has_media("text\n\n![](https://a.org/1.jpg \"cap\")\n"));
        assert!(has_media(
            "<tg-collage><img src=\"https://a.org/1.jpg\"/></tg-collage>"
        ));
        assert!(!has_media("just **text** and a [link](https://a.org)"));
    }

    #[test]
    fn media_becomes_links() {
        let md = "Intro.\n\n![](https://a.org/1.jpg \"Amphora\")\n\nAfter.";
        assert_eq!(
            media_as_links(md),
            "Intro.\n\n[Amphora](https://a.org/1.jpg)\n\nAfter."
        );
        let bare = "![](https://a.org/1.jpg)";
        assert_eq!(
            media_as_links(bare),
            "[https://a.org/1.jpg](https://a.org/1.jpg)"
        );
        let collage = "<tg-collage><img src=\"https://a.org/1.jpg\"/><img src=\"https://a.org/2.jpg\"/></tg-collage>\n";
        let links = media_as_links(collage);
        assert!(links.contains("https://a.org/1.jpg") && links.contains("https://a.org/2.jpg"));
        assert!(!links.contains("<img"));
    }
}
