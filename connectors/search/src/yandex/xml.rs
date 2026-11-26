//! Extract ordered documents and passages from the documented XML result.
use quick_xml::{
    Reader,
    events::{BytesStart, Event},
};
use reqwest::Url;

use super::SearchHit;

#[derive(Default)]
struct Document {
    title: String,
    url: String,
    passages: String,
    headline: String,
}

#[derive(Clone, Copy)]
enum Field {
    Title,
    Url,
    Passage,
    Headline,
}

impl Document {
    fn field(&mut self, selected: Field) -> &mut String {
        match selected {
            Field::Title => &mut self.title,
            Field::Url => &mut self.url,
            Field::Passage => &mut self.passages,
            Field::Headline => &mut self.headline,
        }
    }

    fn finish(self) -> Option<SearchHit> {
        let url = self.url.trim();
        let parsed = Url::parse(url).ok()?;
        if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
            return None;
        }
        let title = clean(&self.title);
        let snippet = clean(&self.passages);
        Some(SearchHit {
            title: if title.is_empty() { url.into() } else { title },
            url: url.into(),
            snippet: if snippet.is_empty() {
                clean(&self.headline)
            } else {
                snippet
            },
        })
    }
}

pub(super) fn parse_results(xml: &[u8], limit: usize) -> Result<Vec<SearchHit>, String> {
    let mut reader = Reader::from_reader(xml);
    let mut path: Vec<Vec<u8>> = Vec::new();
    let mut hits = Vec::new();
    let mut saw_response = false;
    let mut document: Option<Document> = None;
    let mut active: Option<(usize, Field)> = None;
    loop {
        match reader.read_event().map_err(|_| invalid_xml())? {
            Event::Start(tag) => {
                if path.len() >= 64 {
                    return Err(invalid_xml());
                }
                let name = tag.name();
                let name = name.as_ref();
                let parent = path.last().map(Vec::as_slice);
                if path.len() == 1 && parent == Some(b"yandexsearch") && name == b"response" {
                    saw_response = true;
                }
                if response_child(&path) && name == b"error" {
                    return search_error(&tag);
                }
                if in_response(&path) && parent == Some(b"group") && name == b"doc" {
                    document = Some(Document::default());
                } else if let Some(doc) = &mut document
                    && active.is_none()
                    && let Some(field) = match (parent, name) {
                        (Some(b"doc"), b"title") => Some(Field::Title),
                        (Some(b"doc"), b"url") => Some(Field::Url),
                        (Some(b"doc"), b"headline") => Some(Field::Headline),
                        (Some(b"passages"), b"passage") => Some(Field::Passage),
                        _ => None,
                    }
                {
                    if matches!(field, Field::Passage) && !doc.passages.is_empty() {
                        doc.passages.push(' ');
                    }
                    active = Some((path.len() + 1, field));
                }
                path.push(name.to_vec());
            }
            Event::Empty(tag) => {
                if response_child(&path) && tag.name().as_ref() == b"error" {
                    return search_error(&tag);
                }
                if path.len() == 1
                    && path[0] == b"yandexsearch"
                    && tag.name().as_ref() == b"response"
                {
                    saw_response = true;
                }
            }
            Event::Text(text) => {
                if let (Some(doc), Some((_, field))) = (&mut document, active) {
                    doc.field(field)
                        .push_str(&text.unescape().map_err(|_| invalid_xml())?);
                }
            }
            Event::CData(text) => {
                if let (Some(doc), Some((_, field))) = (&mut document, active) {
                    doc.field(field).push_str(
                        &reader
                            .decoder()
                            .decode(text.as_ref())
                            .map_err(|_| invalid_xml())?,
                    );
                }
            }
            Event::End(tag) => {
                if active.is_some_and(|(depth, _)| depth == path.len()) {
                    active = None;
                }
                if tag.name().as_ref() == b"doc"
                    && let Some(doc) = document.take()
                    && hits.len() < limit
                    && let Some(hit) = doc.finish()
                {
                    hits.push(hit);
                }
                path.pop().ok_or_else(invalid_xml)?;
            }
            Event::Eof => break,
            _ => (),
        }
    }
    if !saw_response || !path.is_empty() {
        return Err(invalid_xml());
    }
    Ok(hits)
}

fn in_response(path: &[Vec<u8>]) -> bool {
    path.len() >= 2 && path[0] == b"yandexsearch" && path[1] == b"response"
}

fn response_child(path: &[Vec<u8>]) -> bool {
    path.len() == 2 && in_response(path)
}

fn search_error(tag: &BytesStart<'_>) -> Result<Vec<SearchHit>, String> {
    let code = tag
        .try_get_attribute("code")
        .map_err(|_| invalid_xml())?
        .and_then(|attr| std::str::from_utf8(&attr.value).ok()?.parse::<u16>().ok());
    match code {
        Some(15) => Ok(Vec::new()),
        Some(code) => Err(format!("yandex: search API error {code}")),
        None => Err("yandex: search API returned an error".into()),
    }
}

fn clean(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}
fn invalid_xml() -> String {
    "yandex: invalid XML search response".into()
}
