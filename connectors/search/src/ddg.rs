//! DuckDuckGo backend — a small engine over the system `curl`.
//!
//! **Why curl and not reqwest.** Measured, not guessed: DDG's anti-bot answers
//! reqwest/hyper with a `202 Accepted` "anomaly" challenge page and zero results —
//! with rustls *and* with native-tls (OpenSSL), over HTTP/1.1 *and* HTTP/2, with
//! browser-ish headers. curl from the same IP, same second, gets `200` and a full
//! result page. So the block keys on the TLS/HTTP client fingerprint, which hyper
//! can't spoof. Every ready-made DDG crate wraps reqwest, so they all hit this wall.
//!
//! curl is already a hard requirement on our targets (the forkd sandbox needs it),
//! so leaning on it costs no new dependency. A TLS-impersonating client (e.g.
//! `rquest`/BoringSSL) is the upgrade if we ever want to drop the curl binary.
//!
//! No official DDG API exists, so this stays best-effort: a challenge page or a
//! markup change surfaces as an honest error/empty list, never a panic.

use std::process::Stdio;
use std::time::Duration;

use async_trait::async_trait;
use scraper::{Html, Selector};
use tokio::process::Command;

use crate::{SearchBackend, SearchHit};

const ENDPOINT: &str = "https://html.duckduckgo.com/html/";
const UA: &str = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36";
const ACCEPT: &str = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
const ACCEPT_LANG: &str = "en-US,en;q=0.9,ru;q=0.8";

/// DuckDuckGo search over the HTML endpoint, fetched with `curl`.
pub struct DdgBackend {
    /// The curl binary (name on `PATH`, or an absolute path).
    curl: String,
    timeout: Duration,
    /// DDG `kl` locale (region-language), e.g. `"ru-ru"`; `None` = DDG default.
    region: Option<String>,
}

impl DdgBackend {
    pub fn new(timeout: Duration, region: Option<String>) -> Result<Self, String> {
        Ok(Self { curl: "curl".to_string(), timeout, region })
    }

    /// Override the curl binary (default: `curl` from `PATH`).
    pub fn with_curl(mut self, curl: impl Into<String>) -> Self {
        self.curl = curl.into();
        self
    }

    /// POST the query through curl; returns the HTML body.
    async fn fetch(&self, query: &str) -> Result<String, String> {
        let secs = self.timeout.as_secs().max(1).to_string();
        let mut cmd = Command::new(&self.curl);
        cmd.arg("-sS") // quiet, but keep errors on stderr
            .arg("-m")
            .arg(&secs)
            .arg("-A")
            .arg(UA)
            .arg("-H")
            .arg(format!("Accept: {ACCEPT}"))
            .arg("-H")
            .arg(format!("Accept-Language: {ACCEPT_LANG}"))
            // `--data-urlencode` makes this a POST and encodes the value for us.
            // The query is a separate argv entry — no shell, so nothing to inject.
            .arg("--data-urlencode")
            .arg(format!("q={query}"));
        if let Some(kl) = &self.region {
            cmd.arg("--data-urlencode").arg(format!("kl={kl}"));
        }
        cmd.arg(ENDPOINT)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);

        let out = cmd
            .output()
            .await
            .map_err(|e| format!("ddg: cannot run `{}`: {e}", self.curl))?;
        if !out.status.success() {
            let err = String::from_utf8_lossy(&out.stderr);
            return Err(format!("ddg: curl failed ({}): {}", out.status, err.trim()));
        }
        Ok(String::from_utf8_lossy(&out.stdout).into_owned())
    }
}

#[async_trait]
impl SearchBackend for DdgBackend {
    fn name(&self) -> &str {
        "ddg"
    }

    async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, String> {
        let body = self.fetch(query).await?;
        let hits = parse_ddg_html(&body, limit);
        // Distinguish "DDG served the bot challenge" from "genuinely no matches", so
        // the agent is told the truth instead of silently believing nothing exists.
        if hits.is_empty() && looks_like_challenge(&body) {
            return Err("ddg: blocked by the anti-bot challenge (no results served)".into());
        }
        Ok(hits)
    }
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

    /// Live smoke test against DuckDuckGo — needs network + `curl`, so it is
    /// ignored by default. Run with `cargo test -p octo-connector-search -- --ignored`.
    #[tokio::test]
    #[ignore = "hits the network"]
    async fn live_ddg_search_returns_hits() {
        let backend = DdgBackend::new(Duration::from_secs(20), None).unwrap();
        let hits = backend.search("rust async runtime", 5).await.unwrap();
        assert!(!hits.is_empty(), "expected some hits");
        assert!(hits.iter().all(|h| h.url.starts_with("http")));
        for h in &hits {
            println!("- {} — {}", h.title, h.url);
        }
    }
}
