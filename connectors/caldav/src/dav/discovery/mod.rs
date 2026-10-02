use octo_http_auth::HttpAuth;

use super::{DavError, truncate};

/// then pick by display name (if `want_name` is set) or the first VEVENT calendar.
pub async fn discover_collection(
    client: &reqwest::Client,
    base_url: &str,
    auth: &HttpAuth,
    want_name: Option<&str>,
) -> Result<String, DavError> {
    let origin = origin_of(base_url);

    let principal = href_inside(
        &propfind(
            client,
            base_url,
            auth,
            "0",
            r#"<d:propfind xmlns:d="DAV:"><d:prop><d:current-user-principal/></d:prop></d:propfind>"#,
        )
        .await?,
        b"current-user-principal",
    )
    .ok_or_else(|| DavError::Discovery("no current-user-principal".into()))?;

    let home = href_inside(
        &propfind(
            client,
            &resolve_url(&origin, &principal),
            auth,
            "0",
            r#"<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav"><d:prop><c:calendar-home-set/></d:prop></d:propfind>"#,
        )
        .await?,
        b"calendar-home-set",
    )
    .ok_or_else(|| DavError::Discovery("no calendar-home-set".into()))?;

    let list_xml = propfind(
        client,
        &resolve_url(&origin, &home),
        auth,
        "1",
        r#"<d:propfind xmlns:d="DAV:" xmlns:c="urn:ietf:params:xml:ns:caldav"><d:prop><d:resourcetype/><d:displayname/><c:supported-calendar-component-set/></d:prop></d:propfind>"#,
    )
    .await?;
    let calendars = parse_calendars(&list_xml);

    let chosen = want_name
        .and_then(|n| {
            calendars
                .iter()
                .find(|c| c.is_event_calendar() && c.name == n)
        })
        .or_else(|| calendars.iter().find(|c| c.is_event_calendar()))
        .ok_or_else(|| {
            let names: Vec<&str> = calendars.iter().map(|c| c.name.as_str()).collect();
            DavError::Discovery(format!("no VEVENT calendar found; saw: {names:?}"))
        })?;

    Ok(resolve_url(&origin, &chosen.href))
}

async fn propfind(
    client: &reqwest::Client,
    url: &str,
    auth: &HttpAuth,
    depth: &str,
    body: &'static str,
) -> Result<String, DavError> {
    let method = reqwest::Method::from_bytes(b"PROPFIND").expect("valid method");
    let req = client
        .request(method, url)
        .header("Depth", depth)
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/xml; charset=utf-8",
        )
        .body(body);
    let resp = auth.apply(req).await?.send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    if !status.is_success() {
        return Err(DavError::Status {
            status: status.as_u16(),
            body: truncate(&text),
        });
    }
    Ok(text)
}

/// One calendar collection from a `calendar-home-set` listing.
struct CalInfo {
    href: String,
    name: String,
    is_calendar: bool,
    supports_vevent: bool,
    is_schedule: bool, // inbox/outbox — never a target
}

impl CalInfo {
    fn is_event_calendar(&self) -> bool {
        self.is_calendar && !self.is_schedule && self.supports_vevent
    }
}

/// The first `<href>` nested inside the named DAV property (e.g. the principal
/// href inside `<current-user-principal>`), skipping the response-level href.
fn href_inside(xml: &str, prop_local: &[u8]) -> Option<String> {
    use quick_xml::events::Event as Xml;
    use quick_xml::reader::Reader;

    let mut reader = Reader::from_str(xml);
    let mut in_prop = false;
    let mut in_href = false;
    let mut href = String::new();
    loop {
        match reader.read_event() {
            Ok(Xml::Start(e)) => {
                let ln = e.local_name();
                if ln.as_ref() == prop_local {
                    in_prop = true;
                } else if in_prop && ln.as_ref() == b"href" {
                    in_href = true;
                    href.clear();
                }
            }
            Ok(Xml::Text(e)) if in_href => href.push_str(&e.unescape().unwrap_or_default()),
            Ok(Xml::End(e)) => {
                let ln = e.local_name();
                if in_href && ln.as_ref() == b"href" {
                    return Some(href.trim().to_string());
                }
                if ln.as_ref() == prop_local {
                    in_prop = false;
                }
            }
            Ok(Xml::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    None
}

/// Parse a `calendar-home-set` multistatus into calendar collections.
fn parse_calendars(xml: &str) -> Vec<CalInfo> {
    use quick_xml::events::{BytesStart, Event as Xml};
    use quick_xml::reader::Reader;

    fn has_vevent(e: &BytesStart) -> bool {
        e.local_name().as_ref() == b"comp"
            && e.attributes()
                .flatten()
                .any(|a| a.key.local_name().as_ref() == b"name" && a.value.as_ref() == b"VEVENT")
    }

    let mut reader = Reader::from_str(xml);
    let mut out: Vec<CalInfo> = Vec::new();
    let mut cur: Option<CalInfo> = None;
    let mut got_href = false; // capture only the response-level (first) href
    let mut cap: Option<&'static str> = None; // "href" | "displayname"
    loop {
        match reader.read_event() {
            Ok(Xml::Start(e)) => match e.local_name().as_ref() {
                b"response" => {
                    cur = Some(CalInfo {
                        href: String::new(),
                        name: String::new(),
                        is_calendar: false,
                        supports_vevent: false,
                        is_schedule: false,
                    });
                    got_href = false;
                }
                b"href" if cur.is_some() && !got_href => cap = Some("href"),
                b"displayname" if cur.is_some() => cap = Some("displayname"),
                b"calendar" => {
                    if let Some(c) = cur.as_mut() {
                        c.is_calendar = true;
                    }
                }
                b"schedule-inbox" | b"schedule-outbox" => {
                    if let Some(c) = cur.as_mut() {
                        c.is_schedule = true;
                    }
                }
                _ => {
                    if has_vevent(&e) {
                        if let Some(c) = cur.as_mut() {
                            c.supports_vevent = true;
                        }
                    }
                }
            },
            // resourcetype/comp elements are usually empty: <C:calendar/>, <C:comp name="VEVENT"/>
            Ok(Xml::Empty(e)) => match e.local_name().as_ref() {
                b"calendar" => {
                    if let Some(c) = cur.as_mut() {
                        c.is_calendar = true;
                    }
                }
                b"schedule-inbox" | b"schedule-outbox" => {
                    if let Some(c) = cur.as_mut() {
                        c.is_schedule = true;
                    }
                }
                _ => {
                    if has_vevent(&e) {
                        if let Some(c) = cur.as_mut() {
                            c.supports_vevent = true;
                        }
                    }
                }
            },
            Ok(Xml::Text(e)) => {
                if let (Some(c), Some(what)) = (cur.as_mut(), cap) {
                    let t = e.unescape().unwrap_or_default();
                    match what {
                        "href" => c.href.push_str(&t),
                        "displayname" => c.name.push_str(&t),
                        _ => {}
                    }
                }
            }
            Ok(Xml::End(e)) => match e.local_name().as_ref() {
                b"href" => {
                    if cap == Some("href") {
                        got_href = true;
                    }
                    cap = None;
                }
                b"displayname" => cap = None,
                b"response" => {
                    if let Some(c) = cur.take() {
                        out.push(c);
                    }
                }
                _ => {}
            },
            Ok(Xml::Eof) => break,
            Ok(_) => {}
            Err(_) => break,
        }
    }
    out
}

/// `scheme://host[:port]` of a URL, for resolving path-absolute hrefs.
fn origin_of(url: &str) -> String {
    if let Some(after_scheme) = url.find("://") {
        let rest = &url[after_scheme + 3..];
        let host_len = rest.find('/').unwrap_or(rest.len());
        return url[..after_scheme + 3 + host_len].to_string();
    }
    url.trim_end_matches('/').to_string()
}

/// Resolve a possibly path-absolute DAV `href` against the server origin.
fn resolve_url(origin: &str, href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        href.to_string()
    } else if let Some(rest) = href.strip_prefix('/') {
        format!("{}/{}", origin.trim_end_matches('/'), rest)
    } else {
        format!("{}/{}", origin.trim_end_matches('/'), href)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn finds_href_inside_property() {
        let xml = r#"<multistatus xmlns="DAV:">
  <response>
    <href>/principals/</href>
    <propstat><prop>
      <current-user-principal><href>/principals/users/me/</href></current-user-principal>
    </prop></propstat>
  </response>
</multistatus>"#;
        assert_eq!(
            href_inside(xml, b"current-user-principal").as_deref(),
            Some("/principals/users/me/")
        );
    }

    #[test]
    fn parses_calendar_home_listing() {
        // A Yandex-shaped listing: DAV default namespace, one VEVENT calendar,
        // one VTODO calendar, and a schedule-inbox that must be skipped.
        let xml = r#"<multistatus xmlns="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <response>
    <href>/calendars/me/events-1/</href>
    <propstat><prop>
      <resourcetype><collection/><C:calendar/></resourcetype>
      <displayname>My events</displayname>
      <C:supported-calendar-component-set><C:comp name="VEVENT"/></C:supported-calendar-component-set>
    </prop></propstat>
  </response>
  <response>
    <href>/calendars/me/todos-2/</href>
    <propstat><prop>
      <resourcetype><collection/><C:calendar/></resourcetype>
      <displayname>My todos</displayname>
      <C:supported-calendar-component-set><C:comp name="VTODO"/></C:supported-calendar-component-set>
    </prop></propstat>
  </response>
  <response>
    <href>/calendars/me/inbox/</href>
    <propstat><prop>
      <resourcetype><collection/><C:schedule-inbox/></resourcetype>
      <displayname>Inbox</displayname>
    </prop></propstat>
  </response>
</multistatus>"#;
        let cals = parse_calendars(xml);
        assert_eq!(cals.len(), 3);
        let events: Vec<&CalInfo> = cals.iter().filter(|c| c.is_event_calendar()).collect();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].href, "/calendars/me/events-1/");
        assert_eq!(events[0].name, "My events");
    }

    #[test]
    fn resolves_hrefs_against_origin() {
        assert_eq!(
            origin_of("https://caldav.yandex.ru/calendars/x/"),
            "https://caldav.yandex.ru"
        );
        assert_eq!(
            resolve_url("https://caldav.yandex.ru", "/calendars/me/events-1/"),
            "https://caldav.yandex.ru/calendars/me/events-1/"
        );
        assert_eq!(
            resolve_url("https://caldav.yandex.ru", "https://other.example/c/"),
            "https://other.example/c/"
        );
    }

    /// Live discovery against a real server root. Ignored; run with:
    ///   OCTO_YANDEX_APP_PASSWORD=... OCTO_TEST_CALDAV_LOGIN=... \
    ///   OCTO_TEST_CALDAV_BASE_URL=https://caldav.yandex.ru \
    ///   cargo test -p octo-connector-caldav -- --ignored --nocapture live_discover
    #[tokio::test]
    #[ignore]
    async fn live_discover() {
        use octo_http_auth::{AuthConfig, HttpAuth};
        let login = std::env::var("OCTO_TEST_CALDAV_LOGIN").expect("OCTO_TEST_CALDAV_LOGIN");
        let base_url =
            std::env::var("OCTO_TEST_CALDAV_BASE_URL").expect("OCTO_TEST_CALDAV_BASE_URL");
        let calendar = std::env::var("OCTO_TEST_CALDAV_CALENDAR").ok();
        let auth = HttpAuth::new(AuthConfig::Basic {
            login,
            password_env: "OCTO_YANDEX_APP_PASSWORD".into(),
        });
        let client = reqwest::Client::new();
        let collection = discover_collection(&client, &base_url, &auth, calendar.as_deref())
            .await
            .expect("discover_collection");
        println!("LIVE discover_collection -> {collection}");
        assert!(collection.starts_with("http"));
    }
}
