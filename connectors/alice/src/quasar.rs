//! Making a Yandex speaker talk on its own — the cloud "say text" path.
//!
//! A webhook skill can only answer a request, so a reply that is ready *after*
//! the request returned has no official way back. The Yandex smart-home cloud
//! (Quasar) does: a scenario whose action is "произнести текст" on a speaker,
//! rewritten and launched on demand. The protocol is not public; this follows
//! the open-source Home Assistant integration AlexxIT/YandexStation (MIT), which
//! has used it for years:
//!
//! - auth: a long-lived x-token (one-time QR login) → session cookies via
//!   `mobileproxy.passport.yandex.net/1/bundle/auth/x_token/` → the session URL;
//! - every non-GET call carries `x-csrf-token` scraped from `yandex.ru/quasar`;
//! - one scenario per speaker, found again by its voice trigger (the device id
//!   spelled in Russian letters, so no one says it by accident), rewritten with
//!   the text (≤ 100 characters) and launched.

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use regex::Regex;
use serde_json::{Value, json};
use tokio::sync::Mutex;

/// Cloud TTS accepts at most this many characters per launch.
pub const SAY_LIMIT: usize = 100;

const QUASAR: &str = "https://iot.quasar.yandex.ru/m";
static CSRF: LazyLock<Regex> = LazyLock::new(|| Regex::new(r#""csrfToken2":"([^"]+)""#).unwrap());

#[derive(Debug)]
pub struct QuasarError(pub String);

impl std::fmt::Display for QuasarError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for QuasarError {}

type Result<T> = std::result::Result<T, QuasarError>;

fn err(context: &str, e: impl std::fmt::Display) -> QuasarError {
    QuasarError(format!("quasar: {context}: {e}"))
}

/// The speaker we talk through, resolved lazily on first use.
#[derive(Debug, Clone)]
struct Target {
    device_id: String,
    scenario_id: String,
}

#[derive(Default)]
struct State {
    csrf: Option<String>,
    target: Option<Target>,
    /// When the previous utterance should be finished — the cloud gives no
    /// playback feedback, so pieces are paced by an estimate.
    busy_until: Option<Instant>,
}

pub struct Quasar {
    http: reqwest::Client,
    x_token: String,
    /// Speaker name or id from the manifest; `None` → the only speaker.
    device: Option<String>,
    state: Mutex<State>,
}

impl Quasar {
    pub fn new(x_token: String, device: Option<String>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .cookie_store(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .user_agent("Mozilla/5.0 (octo-connector-alice)")
            .build()
            .map_err(|e| err("http client", e))?;
        Ok(Self {
            http,
            x_token,
            device,
            state: Mutex::new(State::default()),
        })
    }

    /// Say `pieces` (each ≤ [`SAY_LIMIT`]) one after another on the speaker.
    /// Serialised: concurrent calls queue behind each other.
    pub async fn say(&self, pieces: &[String]) -> Result<()> {
        for piece in pieces {
            self.run(Action::Say(piece), speaking_time(piece)).await?;
        }
        Ok(())
    }

    /// Have the speaker execute `command` as if it had been said to Alice
    /// ("запусти навык …"). Queued behind anything still being said.
    pub async fn command(&self, command: &str) -> Result<()> {
        self.run(Action::Command(command), Duration::from_secs(2))
            .await
    }

    /// Rewrite the speaker's scenario with `action` and launch it, after the
    /// previous utterance; `busy` estimates how long this one keeps it busy.
    async fn run(&self, action: Action<'_>, busy: Duration) -> Result<()> {
        let mut state = self.state.lock().await;
        let target = self.target(&mut state).await?;
        {
            if let Some(until) = state.busy_until {
                tokio::time::sleep_until(until.into()).await;
            }
            let scenario = scenario(&target.device_id, action);
            self.call(
                &mut state,
                reqwest::Method::PUT,
                &format!("{QUASAR}/v4/user/scenarios/{}", target.scenario_id),
                Some(&scenario),
            )
            .await?;
            self.call(
                &mut state,
                reqwest::Method::POST,
                &format!("{QUASAR}/user/scenarios/{}/actions", target.scenario_id),
                None,
            )
            .await?;
            state.busy_until = Some(Instant::now() + busy);
        }
        Ok(())
    }

    /// Names and ids of the speakers on the account — logged at startup so the
    /// operator can pick `device`.
    pub async fn speakers(&self) -> Result<Vec<(String, String)>> {
        let mut state = self.state.lock().await;
        let devices = self
            .call(
                &mut state,
                reqwest::Method::GET,
                &format!("{QUASAR}/v3/user/devices"),
                None,
            )
            .await?;
        Ok(speakers(&devices))
    }

    async fn target(&self, state: &mut State) -> Result<Target> {
        if let Some(t) = &state.target {
            return Ok(t.clone());
        }
        let devices = self
            .call(
                state,
                reqwest::Method::GET,
                &format!("{QUASAR}/v3/user/devices"),
                None,
            )
            .await?;
        let all = speakers(&devices);
        let device_id = pick(&all, self.device.as_deref())?;
        let trigger = encode(&device_id);

        let scenarios = self
            .call(
                state,
                reqwest::Method::GET,
                &format!("{QUASAR}/user/scenarios"),
                None,
            )
            .await?;
        let existing = scenarios["scenarios"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|s| s["triggers"][0]["value"].as_str() == Some(trigger.as_str()));
        let scenario_id = match existing.and_then(|s| s["id"].as_str()) {
            Some(id) => id.to_string(),
            None => {
                let created = self
                    .call(
                        state,
                        reqwest::Method::POST,
                        &format!("{QUASAR}/v4/user/scenarios"),
                        Some(&scenario(&device_id, Action::Say("пустышка"))),
                    )
                    .await?;
                created["scenario_id"]
                    .as_str()
                    .ok_or_else(|| err("create scenario", created.to_string()))?
                    .to_string()
            }
        };
        tracing::info!(device_id = %device_id, scenario_id = %scenario_id, "alice: speaker voice ready");
        let target = Target {
            device_id,
            scenario_id,
        };
        state.target = Some(target.clone());
        Ok(target)
    }

    /// One Quasar call with the reference integration's recovery: 401 → log in
    /// again with the x-token, 403 → refresh the CSRF token; one retry.
    async fn call(
        &self,
        state: &mut State,
        method: reqwest::Method,
        url: &str,
        body: Option<&Value>,
    ) -> Result<Value> {
        for attempt in 0..2 {
            let mut req = self.http.request(method.clone(), url);
            if method != reqwest::Method::GET {
                if state.csrf.is_none() {
                    state.csrf = Some(self.csrf().await?);
                }
                req = req.header("x-csrf-token", state.csrf.as_deref().unwrap_or_default());
            }
            if let Some(body) = body {
                req = req.json(body);
            }
            let resp = req.send().await.map_err(|e| err(url, e))?;
            let status = resp.status().as_u16();
            match status {
                200 => {
                    let value: Value = resp.json().await.map_err(|e| err(url, e))?;
                    if value["status"] != "ok" {
                        return Err(err(url, value));
                    }
                    return Ok(value);
                }
                401 if attempt == 0 => {
                    self.login().await?;
                    state.csrf = None;
                }
                403 if attempt == 0 => state.csrf = None,
                _ => return Err(err(url, format!("HTTP {status}"))),
            }
        }
        Err(err(url, "retry exhausted"))
    }

    /// Exchange the x-token for passport session cookies.
    async fn login(&self) -> Result<()> {
        let resp: Value = self
            .http
            .post("https://mobileproxy.passport.yandex.net/1/bundle/auth/x_token/")
            .header(
                "Ya-Consumer-Authorization",
                format!("OAuth {}", self.x_token),
            )
            .form(&[("type", "x-token"), ("retpath", "https://www.yandex.ru")])
            .send()
            .await
            .map_err(|e| err("login", e))?
            .json()
            .await
            .map_err(|e| err("login", e))?;
        if resp["status"] != "ok" {
            // The response names the failure (e.g. an expired token) and holds no secret.
            return Err(err(
                "login (x-token rejected — run the QR login again)",
                resp["errors"].clone(),
            ));
        }
        let host = resp["passport_host"]
            .as_str()
            .unwrap_or("https://passport.yandex.ru");
        let track = resp["track_id"].as_str().unwrap_or_default();
        let session = self
            .http
            .get(format!("{host}/auth/session/"))
            .query(&[("track_id", track)])
            .send()
            .await
            .map_err(|e| err("login session", e))?;
        let location = session
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|l| l.to_str().ok())
            .unwrap_or_default();
        if !location.contains("/auth/finish") {
            return Err(err(
                "login session",
                format!("unexpected redirect, HTTP {}", session.status()),
            ));
        }
        tracing::info!("alice: logged in to Yandex with the x-token");
        Ok(())
    }

    async fn csrf(&self) -> Result<String> {
        let page = self
            .http
            .get("https://yandex.ru/quasar")
            .send()
            .await
            .map_err(|e| err("csrf", e))?
            .text()
            .await
            .map_err(|e| err("csrf", e))?;
        CSRF.captures(&page)
            .map(|c| c[1].to_string())
            .ok_or_else(|| err("csrf", "token not found (not logged in?)"))
    }
}

/// Rough time a speaker needs to say `text`: Alice reads ~14 characters a second.
pub fn speaking_time(text: &str) -> Duration {
    Duration::from_millis(text.chars().count() as u64 * 70 + 700)
}

/// The device id spelled in Russian letters — Yandex wants a Russian voice
/// trigger, and nobody will say this one by accident.
fn encode(uid: &str) -> String {
    const EN: &str = "0123456789abcdef-";
    const RU: [char; 17] = [
        'о', 'е', 'а', 'и', 'н', 'т', 'с', 'р', 'в', 'л', 'к', 'м', 'д', 'п', 'у', 'я', 'ы',
    ];
    uid.chars()
        .filter_map(|c| EN.find(c.to_ascii_lowercase()).map(|i| RU[i]))
        .collect()
}

/// `(name, id)` of every own (not shared) speaker in a `/v3/user/devices` reply.
fn speakers(devices: &Value) -> Vec<(String, String)> {
    devices["households"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|h| h["all"].as_array().into_iter().flatten())
        .filter(|d| d.get("sharing_info").is_none_or(Value::is_null))
        .filter(|d| {
            d["quasar_info"].is_object()
                && !matches!(
                    d["quasar_info"]["platform"].as_str(),
                    Some("saturn" | "mike" | "cherry")
                )
                && d["capabilities"].as_array().is_some_and(|c| !c.is_empty())
        })
        .filter_map(|d| {
            Some((
                d["name"].as_str()?.to_string(),
                d["id"].as_str()?.to_string(),
            ))
        })
        .collect()
}

fn pick(all: &[(String, String)], wanted: Option<&str>) -> Result<String> {
    let names = || {
        all.iter()
            .map(|(n, _)| n.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    match wanted {
        Some(w) => all
            .iter()
            .find(|(name, id)| {
                id == w || name.eq_ignore_ascii_case(w) || name.to_lowercase() == w.to_lowercase()
            })
            .map(|(_, id)| id.clone())
            .ok_or_else(|| {
                err(
                    "device",
                    format!("no speaker named {w:?}; have: {}", names()),
                )
            }),
        None if all.len() == 1 => Ok(all[0].1.clone()),
        None if all.is_empty() => Err(err("device", "no speakers on this Yandex account")),
        None => Err(err(
            "device",
            format!("several speakers, set `device` to one of: {}", names()),
        )),
    }
}

/// What the speaker's scenario does when launched.
#[derive(Debug, Clone, Copy)]
enum Action<'a> {
    /// Say the text (cloud TTS, ≤ [`SAY_LIMIT`] characters).
    Say(&'a str),
    /// Execute the text as a spoken command to Alice.
    Command(&'a str),
}

fn scenario(device_id: &str, action: Action<'_>) -> Value {
    let capability = match action {
        Action::Say(text) => json!({
            "type": "devices.capabilities.quasar",
            "state": {"instance": "tts", "value": {"text": text}},
        }),
        Action::Command(text) => json!({
            "type": "devices.capabilities.quasar.server_action",
            "state": {"instance": "text_action", "value": text},
        }),
    };
    json!({
        "name": format!("octo alice {device_id}"),
        "icon": "home",
        "triggers": [{"trigger": {"type": "scenario.trigger.voice", "value": encode(device_id)}}],
        "steps": [{
            "type": "scenarios.steps.actions.v2",
            "parameters": {"items": [{
                "id": device_id,
                "type": "step.action.item.device",
                "value": {
                    "id": device_id,
                    "item_type": "device",
                    "capabilities": [capability],
                },
            }]},
        }],
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_scenario_uses_text_action() {
        let s = scenario("ab", Action::Command("запусти навык Помощник Альберт"));
        let cap = &s["steps"][0]["parameters"]["items"][0]["value"]["capabilities"][0];
        assert_eq!(cap["type"], "devices.capabilities.quasar.server_action");
        assert_eq!(cap["state"]["instance"], "text_action");
        assert_eq!(cap["state"]["value"], "запусти навык Помощник Альберт");
    }

    #[test]
    fn encodes_device_id_in_russian_letters() {
        assert_eq!(encode("0a-F9"), "окыял");
    }

    #[test]
    fn finds_own_speakers_and_picks_one() {
        let devices = json!({"households": [{"all": [
            {"id": "1", "name": "Станция", "quasar_info": {"platform": "yandexstation_2"}, "capabilities": [{}]},
            {"id": "2", "name": "Модуль", "quasar_info": {"platform": "yandexmodule_2"}, "capabilities": []},
            {"id": "3", "name": "Лампа", "capabilities": [{}]},
            {"id": "4", "name": "Чужая", "quasar_info": {"platform": "x"}, "capabilities": [{}], "sharing_info": {"owner": 1}},
        ]}]});
        let all = speakers(&devices);
        assert_eq!(all, vec![("Станция".to_string(), "1".to_string())]);
        assert_eq!(pick(&all, None).unwrap(), "1");
        assert_eq!(pick(&all, Some("станция")).unwrap(), "1");
        assert!(pick(&all, Some("Кухня")).is_err());
        assert!(pick(&[], None).is_err());
    }

    #[test]
    fn scenario_carries_text_and_trigger() {
        let s = scenario("ab", Action::Say("привет"));
        assert_eq!(s["triggers"][0]["trigger"]["value"], "км");
        assert_eq!(
            s["steps"][0]["parameters"]["items"][0]["value"]["capabilities"][0]["state"]["value"]["text"],
            "привет"
        );
    }
}
