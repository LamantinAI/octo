//! DuckDuckGo backend — a small engine over **libcurl** (in-process, via the `curl`
//! crate). No official DDG API exists, so this stays best-effort: a challenge page or
//! a markup change surfaces as an honest error/empty list, never a panic.
//!
//! # Why libcurl and not reqwest — and why the *system* libcurl specifically
//!
//! Measured, not guessed. DDG's anti-bot answers reqwest/hyper with `202 Accepted`
//! and a challenge page carrying zero results — with rustls **and** native-tls (the
//! very same system OpenSSL curl uses), over HTTP/1.1 **and** HTTP/2, with
//! browser-like headers. The same request through curl, same IP, same second, gets
//! `200` and a full result page. The block keys on the TLS/transport fingerprint
//! (cipher and extension order, ALPN offer), which hyper does not let you reshape —
//! so every ready-made DDG crate, all of them reqwest wrappers, hits the same wall.
//!
//! The curl binary is only a thin shell over `libcurl`, so linking libcurl gives the
//! same handshake in-process. **But it must be the *system* libcurl.** Verified both
//! ways on the same box:
//!
//! | client | result |
//! |---|---|
//! | reqwest (rustls / native-tls, h1 / h2) | `202`, 0 results |
//! | `curl` crate against a **vendored** build (curl 8.21, no system dev headers) | TLS `CURLE_SSL_CONNECT_ERROR` — peer drops the handshake |
//! | `curl` crate against the **system** libcurl (8.5.0, OpenSSL 3.0.13, nghttp2) | `200`, 10 results |
//!
//! A vendored build is compiled with a different feature set (e.g. no nghttp2, so a
//! different ALPN offer), which changes the fingerprint enough to be rejected.
//!
//! ## Build trap
//!
//! `curl-sys` links the system library **only if it can find it** (pkg-config); with
//! no `libcurl4-openssl-dev` present it silently vendors its own copy, and search
//! then fails at runtime. Worse, cargo caches that build-script decision: installing
//! the headers afterwards is not enough — run `cargo clean -p curl-sys` and rebuild.
//! [`libcurl_version`] is logged at connector start so the linked build is visible.

use std::time::Duration;

use async_trait::async_trait;
use curl::easy::{Easy, List};
use scraper::{Html, Selector};
use tokio::task::spawn_blocking;

use crate::{SearchBackend, SearchHit};

const ENDPOINT: &str = "https://html.duckduckgo.com/html/";
const UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const ACCEPT: &str = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
const ACCEPT_LANG: &str = "en-US,en;q=0.9,ru;q=0.8";

/// The linked libcurl's version string, e.g. `"8.5.0"`. Logged at startup so a
/// vendored build (see the module's build trap) is obvious in the logs.
pub fn libcurl_version() -> String {
    curl::Version::get().version().to_string()
}

/// DuckDuckGo search over the HTML endpoint, fetched with libcurl.
pub struct DdgBackend {
    timeout: Duration,
    /// DDG `kl` locale (region-language), e.g. `"ru-ru"`; `None` = DDG default.
    region: Option<String>,
}

impl DdgBackend {
    pub fn new(timeout: Duration, region: Option<String>) -> Result<Self, String> {
        Ok(Self { timeout, region })
    }
}

#[async_trait]
impl SearchBackend for DdgBackend {
    fn name(&self) -> &str {
        "ddg"
    }

    async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, String> {
        // libcurl's easy interface is blocking; keep it off the async runtime.
        let (query_owned, region, timeout) = (query.to_string(), self.region.clone(), self.timeout);
        let body = spawn_blocking(move || fetch_blocking(&query_owned, region.as_deref(), timeout))
            .await
            .map_err(|e| format!("ddg: fetch task failed: {e}"))??;

        let hits = parse_ddg_html(&body, limit);
        // Distinguish "DDG served the bot challenge" from "genuinely no matches", so
        // the agent is told the truth instead of believing nothing exists.
        if hits.is_empty() && looks_like_challenge(&body) {
            return Err("ddg: blocked by the anti-bot challenge (no results served)".into());
        }
        Ok(hits)
    }
}

/// One blocking libcurl POST; returns the HTML body.
fn fetch_blocking(query: &str, region: Option<&str>, timeout: Duration) -> Result<String, String> {
    let mut easy = Easy::new();
    easy.url(ENDPOINT).map_err(|e| format!("ddg: url: {e}"))?;
    easy.timeout(timeout).map_err(|e| format!("ddg: timeout: {e}"))?;
    easy.useragent(UA).map_err(|e| format!("ddg: user-agent: {e}"))?;

    let mut headers = List::new();
    headers
        .append(&format!("Accept: {ACCEPT}"))
        .and_then(|()| headers.append(&format!("Accept-Language: {ACCEPT_LANG}")))
        .map_err(|e| format!("ddg: headers: {e}"))?;
    easy.http_headers(headers).map_err(|e| format!("ddg: headers: {e}"))?;

    // Form body, percent-encoded by libcurl itself.
    let mut form = format!("q={}", easy.url_encode(query.as_bytes()));
    if let Some(kl) = region {
        form.push_str(&format!("&kl={}", easy.url_encode(kl.as_bytes())));
    }
    easy.post(true).map_err(|e| format!("ddg: post: {e}"))?;
    easy.post_fields_copy(form.as_bytes()).map_err(|e| format!("ddg: body: {e}"))?;

    let mut buf = Vec::new();
    {
        let mut transfer = easy.transfer();
        transfer
            .write_function(|chunk| {
                buf.extend_from_slice(chunk);
                Ok(chunk.len())
            })
            .map_err(|e| format!("ddg: writer: {e}"))?;
        transfer.perform().map_err(|e| format!("ddg: request failed: {e}"))?;
    }

    let code = easy.response_code().unwrap_or_default();
    if !(200..300).contains(&code) {
        return Err(format!("ddg: http {code}"));
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// DDG's anomaly/challenge interstitial, as opposed to a real empty result page.
fn looks_like_challenge(body: &str) -> bool {
    body.contains("anomaly") || body.contains("challenge-form")
}

/// Extract hits from the DDG html-endpoint page. Each result is a `div.result`
/// carrying `a.result__a` (title text + a direct href) and `.result__snippet`.
fn parse_ddg_html(body: &str, limit: usize) -> Vec<SearchHit> {
    let doc = Html::parse_document(body);
    // `unwrap` on static, known-good selectors — a parse failure here is a bug.
    let result_sel = Selector::parse("div.result").unwrap();
    let link_sel = Selector::parse("a.result__a").unwrap();
    let snippet_sel = Selector::parse(".result__snippet").unwrap();

    let mut hits = Vec::new();
    for result in doc.select(&result_sel) {
        let Some(link) = result.select(&link_sel).next() else {
            continue; // ads / "no results" blocks carry no result__a
        };
        let url = link.value().attr("href").unwrap_or_default().trim().to_string();
        if url.is_empty() {
            continue;
        }
        let title = collapse_ws(&link.text().collect::<String>());
        let snippet = result
            .select(&snippet_sel)
            .next()
            .map(|s| collapse_ws(&s.text().collect::<String>()))
            .unwrap_or_default();
        hits.push(SearchHit { title, url, snippet });
        if hits.len() >= limit {
            break;
        }
    }
    hits
}

/// Trim and collapse internal whitespace runs (DDG markup is heavily indented).
fn collapse_ws(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIXTURE: &str = r#"
    <div class="result results_links">
      <h2 class="result__title">
        <a rel="nofollow" class="result__a" href="https://tokio.rs/tokio/tutorial/async">Tokio async tutorial</a>
      </h2>
      <a class="result__snippet" href="https://tokio.rs">Learn async Rust with Tokio, a runtime for writing reliable network apps.</a>
    </div>
    <div class="result results_links">
      <h2 class="result__title">
        <a rel="nofollow" class="result__a" href="https://github.com/smol-rs/smol">smol-rs/smol</a>
      </h2>
      <a class="result__snippet">A small and fast async runtime.</a>
    </div>
    <div class="result result--ad"><span>an ad with no result__a</span></div>
    "#;

    #[test]
    fn parses_hits_and_skips_adlike_blocks() {
        let hits = parse_ddg_html(FIXTURE, 10);
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].url, "https://tokio.rs/tokio/tutorial/async");
        assert_eq!(hits[0].title, "Tokio async tutorial");
        assert!(hits[0].snippet.starts_with("Learn async Rust"));
        assert_eq!(hits[1].url, "https://github.com/smol-rs/smol");
        assert_eq!(hits[1].snippet, "A small and fast async runtime.");
    }

    #[test]
    fn respects_limit() {
        assert_eq!(parse_ddg_html(FIXTURE, 1).len(), 1);
    }

    #[test]
    fn detects_the_challenge_page() {
        assert!(looks_like_challenge("<html>…anomaly-modal…</html>"));
        assert!(!looks_like_challenge(FIXTURE));
    }

    /// Live smoke test against DuckDuckGo — needs network and a *system* libcurl, so
    /// it is ignored by default. Run with
    /// `cargo test -p octo-connector-search -- --ignored`.
    ///
    /// Two ways this fails that are NOT a broken backend:
    /// - **Rate limiting.** DDG throttles rapid repeats from one IP, so back-to-back
    ///   runs can come back empty. Wait a moment and re-run before believing it.
    /// - **A vendored libcurl.** A challenge/`202` failure usually means `curl-sys`
    ///   vendored its own build (see the module's build trap) rather than DDG changing;
    ///   the printed libcurl version tells you which build you linked.
    #[tokio::test]
    #[ignore = "hits the network"]
    async fn live_ddg_search_returns_hits() {
        println!("linked libcurl: {}", libcurl_version());
        let backend = DdgBackend::new(Duration::from_secs(20), None).unwrap();
        let hits = backend.search("rust async runtime", 5).await.unwrap();
        assert!(!hits.is_empty(), "expected some hits");
        assert!(hits.iter().all(|h| h.url.starts_with("http")));
        for h in &hits {
            println!("- {} — {}", h.title, h.url);
        }
    }
}
