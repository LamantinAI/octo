//! Config-driven construction (`type = "alice"` manifest).

use std::{net::SocketAddr, sync::Arc, time::Duration};

use octo_core::{Connector, ConnectorFactory, ConnectorId, FactoryContext, TrustLevel};
use serde::Deserialize;

use crate::AliceConnector;

#[derive(Debug, Deserialize)]
struct ConnectorFile {
    connector: AliceConfig,
}

/// Static config from a `type = "alice"` manifest. The webhook path secret is
/// named by env var, never written into the manifest.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AliceConfig {
    #[allow(dead_code)]
    id: Option<String>,
    #[allow(dead_code)]
    r#type: Option<String>,
    #[serde(default = "default_listen")]
    listen: String,
    /// Env var holding the secret path segment: the webhook lives at
    /// `POST /alice/<secret>`. Yandex does not sign requests, so the
    /// unguessable URL plus the allow-list are the gate.
    #[serde(default = "default_secret_env")]
    secret_env: String,
    /// When set, requests for any other skill are refused.
    skill_id: Option<String>,
    /// Yandex account ids (`session.user.user_id`) allowed to talk.
    #[serde(default)]
    allowed_users: Vec<String>,
    /// Device ids (`session.application.application_id`) allowed to talk.
    #[serde(default)]
    allowed_applications: Vec<String>,
    /// ACL role stamped on every message: `owner` | `trusted` | `guest`.
    #[serde(default = "default_role")]
    role: String,
    #[serde(default)]
    sender_name: Option<String>,
    #[serde(default = "default_reply_wait_ms")]
    reply_wait_ms: u64,
    #[serde(default = "default_turn_ttl_secs")]
    turn_ttl_secs: u64,
    #[serde(default = "default_max_chars")]
    max_chars: usize,
    #[serde(default)]
    phrases: Phrases,
    #[serde(default = "default_continue_words")]
    continue_words: Vec<String>,
    #[serde(default = "default_exit_words")]
    exit_words: Vec<String>,
    /// What to say while the agent thinks (a random one each time).
    #[serde(default = "default_fillers")]
    fillers: Vec<String>,
    /// The speaker's own cloud voice, so slow replies arrive by themselves.
    push: Option<PushConfig>,
}

/// `[connector.push]` — speak late replies through the Yandex cloud voice.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PushConfig {
    /// Env var with the Yandex x-token (one-time QR login, see README).
    #[serde(default = "default_x_token_env")]
    x_token_env: String,
    /// Speaker name or id; omit when the account has one speaker.
    device: Option<String>,
    /// End the skill session after the filler, so the speaker is idle when
    /// the reply is pushed (default true).
    #[serde(default = "default_true")]
    end_session: bool,
    /// The command that opens this skill on the speaker ("запусти навык
    /// <name>"). When set, a finished reply is not read out piecewise by the
    /// cloud voice: the speaker reopens the skill and the reply comes back
    /// whole as the skill's answer, with the conversation still open.
    relaunch: Option<String>,
    /// Seconds of silence between fillers while the agent is still thinking.
    #[serde(default = "default_filler_every_secs")]
    filler_every_secs: u64,
    /// Stop joking after this many fillers (the reply still comes).
    #[serde(default = "default_max_fillers")]
    max_fillers: usize,
}

/// Resolved push settings.
#[derive(Debug, Clone)]
pub struct PushSettings {
    pub x_token: String,
    pub device: Option<String>,
    pub end_session: bool,
    pub relaunch: Option<String>,
    pub filler_gap: Duration,
    pub max_fillers: usize,
}

/// Everything the connector says on its own (not the agent's words).
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Phrases {
    pub greeting: String,
    pub greeting_pending: String,
    pub thinking: String,
    pub still_thinking: String,
    pub more: String,
    pub nothing_more: String,
    pub bye: String,
    pub denied: String,
    pub media: String,
    pub pong: String,
}

impl Default for Phrases {
    fn default() -> Self {
        Self {
            greeting: "Слушаю.".into(),
            greeting_pending: "Слушаю. У меня есть для вас неуслышанный ответ, скажите «дальше»."
                .into(),
            thinking: "Думаю. Скажите «дальше», и я отвечу.".into(),
            still_thinking: "Ещё думаю. Скажите «дальше» чуть позже.".into(),
            more: "Скажите «дальше», чтобы продолжить.".into(),
            nothing_more: "Больше добавить нечего. Спрашивайте.".into(),
            bye: "До связи.".into(),
            denied: "Это приватный навык.".into(),
            media: "Здесь был файл или картинка, голосом их не передать.".into(),
            pong: "На связи.".into(),
        }
    }
}

/// Resolved, validated settings the running connector uses.
#[derive(Debug, Clone)]
pub struct Settings {
    pub listen: SocketAddr,
    pub secret: String,
    pub skill_id: Option<String>,
    pub allowed_users: Vec<String>,
    pub allowed_applications: Vec<String>,
    pub role: String,
    pub trust: TrustLevel,
    pub sender_name: String,
    pub reply_wait: Duration,
    pub turn_ttl: Duration,
    pub max_chars: usize,
    pub phrases: Phrases,
    pub continue_words: Vec<String>,
    pub exit_words: Vec<String>,
    pub fillers: Vec<String>,
    pub push: Option<PushSettings>,
}

impl Settings {
    /// A filler for this moment — any of them, cheap pseudo-randomness.
    pub fn filler(&self) -> &str {
        self.filler_except("")
    }

    /// A filler other than `previous` (when there is more than one).
    pub fn filler_except(&self, previous: &str) -> &str {
        if self.fillers.is_empty() {
            return "Думаю.";
        }
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as usize)
            .unwrap_or(0);
        let mut i = (nanos / 1000) % self.fillers.len();
        if self.fillers.len() > 1 && self.fillers[i] == previous {
            i = (i + 1) % self.fillers.len();
        }
        &self.fillers[i]
    }
}

fn default_filler_every_secs() -> u64 {
    5
}
fn default_max_fillers() -> usize {
    12
}
fn default_true() -> bool {
    true
}
fn default_x_token_env() -> String {
    "YANDEX_X_TOKEN".into()
}
fn default_fillers() -> Vec<String> {
    [
        "Думаю. Сейчас отвечу.",
        "Секунду, соображаю.",
        "Хороший вопрос. Минутку.",
        "Ищу ответ, не уходите.",
    ]
    .map(String::from)
    .to_vec()
}
fn default_listen() -> String {
    "0.0.0.0:8790".into()
}
fn default_secret_env() -> String {
    "ALICE_WEBHOOK_SECRET".into()
}
fn default_role() -> String {
    "trusted".into()
}
fn default_reply_wait_ms() -> u64 {
    2500
}
fn default_turn_ttl_secs() -> u64 {
    600
}
fn default_max_chars() -> usize {
    900
}
fn default_continue_words() -> Vec<String> {
    [
        "дальше",
        "продолжай",
        "продолжи",
        "ну что",
        "что там",
        "готово",
        "ну",
        "давай",
    ]
    .map(String::from)
    .to_vec()
}
fn default_exit_words() -> Vec<String> {
    ["хватит", "стоп", "выход", "выйти", "пока", "закончить"]
        .map(String::from)
        .to_vec()
}

/// Lower-case, `ё`→`е`, punctuation dropped, spaces collapsed — the form both
/// Alice's `command` and the configured trigger words are compared in.
pub fn normalize(s: &str) -> String {
    s.to_lowercase()
        .replace('ё', "е")
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

impl AliceConfig {
    fn into_settings(self) -> Result<Settings, String> {
        let listen: SocketAddr = self
            .listen
            .parse()
            .map_err(|e| format!("alice: bad listen address {:?}: {e}", self.listen))?;
        let secret = std::env::var(&self.secret_env)
            .map_err(|_| format!("alice: env var {} is not set", self.secret_env))?;
        if secret.len() < 16
            || !secret
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(format!(
                "alice: {} must be at least 16 URL-safe characters ([A-Za-z0-9_-])",
                self.secret_env
            ));
        }
        let trust = match self.role.as_str() {
            "owner" => TrustLevel::High,
            "trusted" => TrustLevel::Medium,
            "guest" => TrustLevel::Low,
            other => {
                return Err(format!(
                    "alice: unknown role {other:?} (owner | trusted | guest)"
                ));
            }
        };
        let words = |v: Vec<String>| {
            v.iter()
                .map(|w| normalize(w))
                .filter(|w| !w.is_empty())
                .collect()
        };
        Ok(Settings {
            listen,
            secret,
            skill_id: self.skill_id.filter(|s| !s.is_empty()),
            allowed_users: self.allowed_users,
            allowed_applications: self.allowed_applications,
            role: self.role,
            trust,
            sender_name: self
                .sender_name
                .unwrap_or_else(|| "голос через Алису".into()),
            reply_wait: Duration::from_millis(self.reply_wait_ms.clamp(500, 4000)),
            turn_ttl: Duration::from_secs(self.turn_ttl_secs.max(10)),
            max_chars: self.max_chars.clamp(100, crate::protocol::TEXT_LIMIT - 64),
            phrases: self.phrases,
            continue_words: words(self.continue_words),
            exit_words: words(self.exit_words),
            fillers: self.fillers.into_iter().filter(|f| !f.trim().is_empty()).collect(),
            push: self.push.and_then(|p| match std::env::var(&p.x_token_env) {
                Ok(x_token) if !x_token.trim().is_empty() => Some(PushSettings {
                    x_token: x_token.trim().to_string(),
                    device: p.device.filter(|d| !d.is_empty()),
                    end_session: p.end_session,
                    relaunch: p.relaunch.filter(|r| !r.trim().is_empty()),
                    filler_gap: Duration::from_secs(p.filler_every_secs.clamp(1, 60)),
                    max_fillers: p.max_fillers,
                }),
                _ => {
                    tracing::warn!(
                        env = %p.x_token_env,
                        "alice: [push] configured but the x-token env var is empty — late replies wait for «дальше»"
                    );
                    None
                }
            }),
        })
    }
}

/// [`ConnectorFactory`] for `type = "alice"`.
pub struct AliceConnectorFactory;

impl ConnectorFactory for AliceConnectorFactory {
    fn type_name(&self) -> &str {
        "alice"
    }

    fn create(
        &self,
        id: ConnectorId,
        config: &toml::Value,
        _ctx: FactoryContext<'_>,
    ) -> Result<Arc<dyn Connector>, Box<dyn std::error::Error + Send + Sync>> {
        let file: ConnectorFile = config.clone().try_into()?;
        let settings = file.connector.into_settings()?;
        if settings.allowed_users.is_empty() && settings.allowed_applications.is_empty() {
            tracing::warn!(
                connector = %id,
                "alice: allow-list is empty — every speaker is refused; their ids are logged so you can add them"
            );
        }
        Ok(AliceConnector::new(id, settings))
    }
}

/// Convenience factory handle for registration.
pub fn factory() -> Arc<dyn ConnectorFactory> {
    Arc::new(AliceConnectorFactory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_matches_alice_command_form() {
        assert_eq!(normalize("  Ну, что?! "), "ну что");
        assert_eq!(normalize("Ещё"), "еще");
    }

    #[test]
    fn manifest_parses_and_rejects_typos() {
        let ok: toml::Value = toml::from_str(
            "[connector]\nid='alice'\ntype='alice'\nallowed_users=['U']\n[connector.phrases]\nthinking='Секунду.'\n",
        )
        .unwrap();
        let file: ConnectorFile = ok.try_into().unwrap();
        assert_eq!(file.connector.phrases.thinking, "Секунду.");
        assert_eq!(file.connector.phrases.bye, "До связи.");

        let typo: toml::Value = toml::from_str("[connector]\nallowed_user=['U']\n").unwrap();
        assert!(typo.try_into::<ConnectorFile>().is_err());
    }
}
