mod process;
mod session;
mod webdriver;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use octo_browser_runtime::MoliRuntime;
use reqwest::Url;
use serde::Deserialize;
use tokio::{
    sync::Mutex,
    time::{sleep, timeout},
};

use self::{
    session::{Driver, Session},
    webdriver::WebDriver,
};
use crate::{SearchBackend, SearchHit, SearchPage};

const ENDPOINT: &str = "https://duckduckgo.com/";
const MAX_PAGE_SIZE: usize = 10;
const MORE_RESULTS: &str = "#more-results";
const READ_RESULTS: &str = include_str!("results.js");

pub(super) struct DdgBackend {
    driver: Driver,
    timeout: Duration,
    region: Option<String>,
    browser: Mutex<Option<Browser>>,
}

struct Browser {
    session: Session,
    query: String,
}

impl DdgBackend {
    pub(super) fn with_firefox(
        endpoint: Option<&str>,
        timeout: Duration,
        region: Option<String>,
    ) -> Result<Self, String> {
        Ok(Self::new(
            Driver::Firefox(WebDriver::new(endpoint, timeout)?),
            timeout,
            region,
        ))
    }

    pub(super) fn with_moli(
        runtime: Arc<MoliRuntime>,
        timeout: Duration,
        region: Option<String>,
    ) -> Self {
        Self::new(Driver::Moli(runtime), timeout, region)
    }

    fn new(driver: Driver, timeout: Duration, region: Option<String>) -> Self {
        Self {
            driver,
            timeout,
            region,
            browser: Mutex::new(None),
        }
    }

    async fn load(
        &self,
        browser: &mut Option<Browser>,
        query: &str,
        page: usize,
        range: PageRange,
    ) -> Result<SearchPage, String> {
        if let Some(active) = browser.as_ref() {
            if !self.driver.is_current(&active.session).await {
                *browser = None;
            }
        }
        if browser.is_none() {
            *browser = Some(Browser {
                session: self.driver.start().await?,
                query: String::new(),
            });
        }
        let browser = browser.as_mut().expect("browser initialized above");
        if page == 1 || browser.query != query {
            let mut url = Url::parse(ENDPOINT).expect("valid DuckDuckGo URL");
            url.query_pairs_mut().append_pair("q", query);
            if let Some(region) = &self.region {
                url.query_pairs_mut().append_pair("kl", region);
            }
            browser.session.navigate(&url).await?;
            browser.query = query.to_owned();
        }

        let mut results = wait_for_results(&browser.session, range, 0).await?;
        while results.total < range.end && results.more {
            browser.session.click(MORE_RESULTS).await?;
            results = wait_for_results(&browser.session, range, results.total).await?;
        }
        let more = results.total > range.end || results.more;
        Ok(SearchPage {
            hits: results.hits,
            next_page: page.checked_add(1).filter(|_| more),
        })
    }
}

#[derive(Clone, Copy)]
struct PageRange {
    offset: usize,
    end: usize,
}

impl PageRange {
    fn new(page: usize, limit: usize) -> Result<Self, String> {
        let limit = limit.clamp(1, MAX_PAGE_SIZE);
        let end = page
            .checked_mul(limit)
            .filter(|&end| end != 0)
            .ok_or("ddg: invalid page")?;
        Ok(Self {
            offset: end - limit,
            end,
        })
    }
}

#[derive(Deserialize)]
struct Results {
    valid_origin: bool,
    challenge: bool,
    empty: bool,
    more: bool,
    total: usize,
    hits: Vec<SearchHit>,
}

async fn wait_for_results(
    session: &Session,
    range: PageRange,
    previous_total: usize,
) -> Result<Results, String> {
    loop {
        let results: Results = session
            .evaluate(READ_RESULTS, range.offset, range.end - range.offset)
            .await?;
        if results.challenge {
            return Err("ddg: DuckDuckGo requested a CAPTCHA".into());
        }
        if !results.valid_origin {
            return Err("ddg: search left DuckDuckGo".into());
        }
        if results.total > previous_total || results.empty {
            return Ok(results);
        }
        sleep(Duration::from_millis(150)).await;
    }
}

#[async_trait]
impl SearchBackend for DdgBackend {
    fn name(&self) -> &str {
        "ddg"
    }

    fn supports_pagination(&self) -> bool {
        true
    }

    async fn shutdown(&self) {
        let browser = self.browser.lock().await.take();
        if let Some(browser) = browser {
            browser.session.close().await;
        }
    }

    async fn search(&self, query: &str, limit: usize) -> Result<Vec<SearchHit>, String> {
        Ok(self.search_page(query, limit, 1).await?.hits)
    }

    async fn search_page(
        &self,
        query: &str,
        limit: usize,
        page: usize,
    ) -> Result<SearchPage, String> {
        let range = PageRange::new(page, limit)?;
        let mut browser = self.browser.lock().await;
        let result = timeout(self.timeout, self.load(&mut browser, query, page, range))
            .await
            .unwrap_or_else(|_| Err("ddg: timed out waiting for DuckDuckGo results".into()));
        if result.is_err() {
            *browser = None;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering::Relaxed},
        },
        time::Duration,
    };

    use axum::{
        Json, Router,
        extract::State,
        response::Html,
        routing::{delete, get, post},
        serve,
    };
    use octo_browser_runtime::MoliRuntime;
    use reqwest::Url;
    use serde_json::{Value, json};
    use tokio::{
        net::TcpListener,
        spawn,
        task::JoinHandle,
        time::{sleep, timeout},
    };

    use super::{DdgBackend, Driver, MORE_RESULTS, PageRange, SearchBackend};

    const QUERY: &str = "SQLite database format";
    const SNIPPET: &str = "A detailed excerpt. ";

    #[tokio::test]
    #[ignore = "requires Moli on PATH"]
    async fn moli_waits_for_the_next_document_before_reading_results() {
        let app = Router::new()
            .route(
                "/first",
                get(|| async { Html("<title>First query</title>") }),
            )
            .route(
                "/second",
                get(|| async {
                    sleep(Duration::from_millis(300)).await;
                    Html("<title>Second query</title>")
                }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = spawn(async move { serve(listener, app).await.unwrap() });
        timeout(Duration::from_secs(8), async {
            let browser = Driver::Moli(Arc::new(MoliRuntime::default()));
            let session = browser.start().await.unwrap();
            for (path, expected) in [("first", "First query"), ("second", "Second query")] {
                session
                    .navigate(&Url::parse(&format!("{base}/{path}")).unwrap())
                    .await
                    .unwrap();
                let title: String = session
                    .evaluate("return document.title;", 0, 10)
                    .await
                    .unwrap();
                assert_eq!(title, expected);
            }
            session.close().await;
        })
        .await
        .expect("navigation fixture timed out");
        server.abort();
    }

    #[derive(Default)]
    struct Calls {
        started: AtomicUsize,
        navigated: AtomicUsize,
        clicked: AtomicUsize,
        closed: AtomicUsize,
        challenge: AtomicBool,
    }

    async fn fixture(challenge: bool) -> (DdgBackend, Arc<Calls>, JoinHandle<()>) {
        let calls = Arc::new(Calls::default());
        calls.challenge.store(challenge, Relaxed);
        let app = Router::new()
            .route(
                "/wd/session",
                post(
                    |State(calls): State<Arc<Calls>>, Json(body): Json<Value>| async move {
                        assert_eq!(
                            body["capabilities"]["alwaysMatch"]["browserName"],
                            "firefox"
                        );
                        calls.started.fetch_add(1, Relaxed);
                        Json(json!({"value":{"sessionId":"fixture"}}))
                    },
                ),
            )
            .route(
                "/wd/session/fixture/url",
                post(
                    |State(calls): State<Arc<Calls>>, Json(body): Json<Value>| async move {
                        let url = Url::parse(body["url"].as_str().unwrap()).unwrap();
                        assert_eq!(url.host_str(), Some("duckduckgo.com"));
                        assert_eq!(url.path(), "/");
                        assert_eq!(url.query_pairs().next().unwrap().1, QUERY);
                        calls.navigated.fetch_add(1, Relaxed);
                        Json(json!({"value":null}))
                    },
                ),
            )
            .route(
                "/wd/session/fixture/execute/sync",
                post(
                    |State(calls): State<Arc<Calls>>, Json(body): Json<Value>| async move {
                        let origin = body["args"][0].as_u64().unwrap() as usize;
                        let limit = body["args"][1].as_u64().unwrap() as usize;
                        let challenge = calls.challenge.load(Relaxed);
                        let total = if challenge {
                            0
                        } else {
                            10 * (1 + calls.clicked.load(Relaxed))
                        };
                        let hits: Vec<_> = (origin..(origin + limit).min(total))
                            .map(|n| {
                                json!({
                                    "url": format!("https://example.com/{n}"),
                                    "title": format!("Source {n}"),
                                    "snippet": SNIPPET.repeat(80),
                                })
                            })
                            .collect();
                        Json(json!({"value":{
                            "valid_origin":true, "challenge":challenge, "empty":false,
                            "more":total == 10, "total":total, "hits":hits,
                        }}))
                    },
                ),
            )
            .route(
                "/wd/session/fixture/element",
                post(|Json(body): Json<Value>| async move {
                    assert_eq!(body["value"], MORE_RESULTS);
                    Json(json!({"value":{"element-6066-11e4-a52e-4f735466cecf":"more"}}))
                }),
            )
            .route(
                "/wd/session/fixture/element/more/click",
                post(|State(calls): State<Arc<Calls>>| async move {
                    calls.clicked.fetch_add(1, Relaxed);
                    Json(json!({"value":null}))
                }),
            )
            .route(
                "/wd/session/fixture",
                delete(|State(calls): State<Arc<Calls>>| async move {
                    calls.closed.fetch_add(1, Relaxed);
                    Json(json!({"value":null}))
                }),
            )
            .with_state(calls.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/wd/", listener.local_addr().unwrap());
        let task = spawn(async move { serve(listener, app).await.unwrap() });
        let backend =
            DdgBackend::with_firefox(Some(&endpoint), Duration::from_secs(2), None).unwrap();
        (backend, calls, task)
    }

    #[tokio::test]
    async fn pages_reuse_the_browser_and_preserve_full_snippets() {
        let (backend, calls, server) = fixture(false).await;
        let first = backend.search_page(QUERY, 25, 1).await.unwrap();
        assert_eq!(first.hits.len(), 10);
        assert_eq!(first.hits[0].snippet, SNIPPET.repeat(80));
        assert_eq!(first.next_page, Some(2));
        let second = backend.search_page(QUERY, 25, 2).await.unwrap();
        assert_eq!(second.hits.len(), 10);
        assert_eq!(second.hits[0].url, "https://example.com/10");
        assert_eq!(second.next_page, None);
        assert_eq!(calls.started.load(Relaxed), 1);
        assert_eq!(calls.navigated.load(Relaxed), 1);
        assert_eq!(calls.clicked.load(Relaxed), 1);
        backend.shutdown().await;
        assert_eq!(calls.closed.load(Relaxed), 1);
        server.abort();
    }

    #[tokio::test]
    async fn captcha_is_reported_without_attempting_to_continue() {
        let (backend, calls, server) = fixture(true).await;
        let error = backend.search_page(QUERY, 10, 2).await.err().unwrap();
        assert_eq!(error, "ddg: DuckDuckGo requested a CAPTCHA");
        assert_eq!(calls.started.load(Relaxed), 1);
        assert_eq!(calls.clicked.load(Relaxed), 0);
        server.abort();
    }

    #[test]
    fn page_range_rejects_overflow_and_page_zero() {
        assert!(PageRange::new(usize::MAX, 10).is_err());
        assert!(PageRange::new(0, 10).is_err());
    }
}
