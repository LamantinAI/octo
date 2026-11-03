//! Turning a chat reply into something a speaker can say: strip Markdown and
//! links, flatten lists into sentences, and cut the result into pieces that fit
//! Alice's per-response limit at sentence boundaries.

use std::sync::LazyLock;

use regex::Regex;

static LINK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"!?\[([^\]]*)\]\([^)]*\)").unwrap());
static URL: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"https?://\S+").unwrap());
static EMPHASIS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\*\*|__|~~|`|\*").unwrap());
static BULLET: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^\s*(?:[-*•–—]|\d+[.)])\s+").unwrap());
static HEADING: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^\s*#{1,6}\s*").unwrap());
static SPACES: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"[ \t]{2,}").unwrap());
static BEFORE_PUNCT: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\s+([.,!?;:])").unwrap());

/// Plain spoken text from a Markdown-ish chat reply. Every non-empty line
/// becomes a sentence; code blocks and table rules are dropped; links keep
/// their text, bare URLs go.
pub fn to_speech(reply: &str) -> String {
    let mut sentences: Vec<String> = Vec::new();
    let mut in_code = false;
    for raw in reply.lines() {
        if raw.trim_start().starts_with("```") {
            in_code = !in_code;
            continue;
        }
        if in_code || raw.trim_start().starts_with('|') && raw.contains("---") {
            continue;
        }
        let line = HEADING.replace(raw, "");
        let line = BULLET.replace(&line, "");
        let line = LINK.replace_all(&line, "$1");
        let line = URL.replace_all(&line, "");
        let line = EMPHASIS.replace_all(&line, "");
        let line = line.replace('|', ", ").replace('>', "");
        let line = SPACES.replace_all(line.trim(), " ");
        let line = BEFORE_PUNCT.replace_all(&line, "$1");
        let line = line.trim().trim_end_matches([',', ';', ':']).trim();
        if line.is_empty() {
            continue;
        }
        let mut sentence = line.to_string();
        if !sentence.ends_with(['.', '!', '?', '…']) {
            sentence.push('.');
        }
        sentences.push(sentence);
    }
    sentences.join(" ")
}

/// Cut `text` into pieces of at most `max` characters, preferring sentence
/// ends, then spaces; a single overlong word is hard-split.
pub fn chunk(text: &str, max: usize) -> Vec<String> {
    let max = max.max(16);
    let mut out = Vec::new();
    let mut rest = text.trim();
    while rest.chars().count() > max {
        let limit = byte_index(rest, max);
        let head = &rest[..limit];
        let cut = sentence_end(head)
            .or_else(|| head.rfind(' ').filter(|&i| i > 0))
            .unwrap_or(limit);
        out.push(rest[..cut].trim().to_string());
        rest = rest[cut..].trim_start();
    }
    if !rest.is_empty() {
        out.push(rest.to_string());
    }
    out
}

/// Byte offset just after the last sentence terminator followed by a space,
/// ignoring one in the first third (too short a piece).
fn sentence_end(head: &str) -> Option<usize> {
    let floor = head.len() / 3;
    head.char_indices()
        .rev()
        .filter(|&(i, c)| i >= floor && matches!(c, '.' | '!' | '?' | '…'))
        .map(|(i, c)| i + c.len_utf8())
        .find(|&end| head[end..].starts_with(' '))
}

fn byte_index(s: &str, chars: usize) -> usize {
    s.char_indices().nth(chars).map_or(s.len(), |(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_markdown_and_flattens_lists() {
        let md = "## План на завтра\n\n- **11:00** — созвон с [Артёмом](https://t.me/x)\n- обед\n\nПодробнее: https://example.com/a?b=c\n```\nlet x = 1;\n```";
        assert_eq!(
            to_speech(md),
            "План на завтра. 11:00 — созвон с Артёмом. обед. Подробнее."
        );
    }

    #[test]
    fn keeps_existing_punctuation() {
        assert_eq!(to_speech("Готово!\nЧто-то ещё?"), "Готово! Что-то ещё?");
    }

    #[test]
    fn chunks_at_sentence_boundaries() {
        let text = "Первое предложение тут. Второе предложение здесь. Третье.";
        let parts = chunk(text, 40);
        assert_eq!(
            parts,
            vec![
                "Первое предложение тут.",
                "Второе предложение здесь. Третье."
            ]
        );
        assert!(parts.iter().all(|p| p.chars().count() <= 40));
    }

    #[test]
    fn chunks_long_sentence_at_spaces_and_short_text_untouched() {
        let text = "слово ".repeat(30);
        let parts = chunk(&text, 50);
        assert!(parts.len() > 1);
        assert!(
            parts
                .iter()
                .all(|p| p.chars().count() <= 50 && !p.starts_with(' '))
        );
        assert_eq!(chunk("Коротко.", 900), vec!["Коротко."]);
        assert!(chunk("  ", 900).is_empty());
    }
}
