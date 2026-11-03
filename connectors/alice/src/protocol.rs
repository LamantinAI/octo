//! The Yandex Dialogs webhook wire format — only the fields this connector reads
//! or writes. Unknown fields are ignored, so new protocol additions don't break
//! parsing. Reference: <https://yandex.ru/dev/dialogs/alice/doc/ru/request> and
//! `.../response`.

use serde::{Deserialize, Serialize};

/// Hard limit Alice puts on `response.text` (characters).
pub const TEXT_LIMIT: usize = 1024;

#[derive(Debug, Default, Deserialize)]
pub struct AliceRequest {
    #[serde(default)]
    pub session: Session,
    #[serde(default)]
    pub request: Utterance,
    #[serde(default = "default_version")]
    pub version: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct Session {
    #[serde(default)]
    pub new: bool,
    #[serde(default)]
    pub skill_id: String,
    /// Present only when the speaker is signed in to a Yandex account.
    pub user: Option<User>,
    #[serde(default)]
    pub application: Application,
}

#[derive(Debug, Default, Deserialize)]
pub struct User {
    pub user_id: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct Application {
    #[serde(default)]
    pub application_id: String,
}

#[derive(Debug, Default, Deserialize)]
pub struct Utterance {
    /// Alice's normalised form: lower-case, no punctuation, numerals as digits,
    /// and — on a launch "попроси <навык> …" — the activation phrase stripped.
    #[serde(default)]
    pub command: String,
    /// The phrase as heard, activation phrase included.
    #[serde(default)]
    pub original_utterance: String,
}

#[derive(Debug, Serialize)]
pub struct AliceResponse {
    pub response: Body,
    pub version: String,
}

#[derive(Debug, Serialize)]
pub struct Body {
    pub text: String,
    pub end_session: bool,
}

impl AliceResponse {
    /// A reply that keeps the session open, clipped to Alice's text limit.
    pub fn say(text: impl Into<String>, version: &str) -> Self {
        Self::build(text.into(), false, version)
    }

    /// A reply that ends the session.
    pub fn bye(text: impl Into<String>, version: &str) -> Self {
        Self::build(text.into(), true, version)
    }

    fn build(text: String, end_session: bool, version: &str) -> Self {
        let text = if text.chars().count() > TEXT_LIMIT {
            text.chars().take(TEXT_LIMIT).collect()
        } else {
            text
        };
        Self {
            response: Body { text, end_session },
            version: version.to_string(),
        }
    }
}

fn default_version() -> String {
    "1.0".to_string()
}

impl AliceRequest {
    /// Who is speaking: the Yandex account when signed in, else the device
    /// (application) — the stable key for both the allow-list and the channel.
    pub fn identity(&self) -> Option<Identity<'_>> {
        if let Some(user) = self.session.user.as_ref().filter(|u| !u.user_id.is_empty()) {
            return Some(Identity::User(&user.user_id));
        }
        let app = &self.session.application.application_id;
        (!app.is_empty()).then_some(Identity::Application(app))
    }

    /// The text to hand the agent. On a launch with a request ("попроси
    /// <навык> …") `command` already has the activation phrase stripped;
    /// mid-session the original wording (punctuation, case) reads better.
    pub fn text(&self) -> &str {
        let original = self.request.original_utterance.trim();
        let command = self.request.command.trim();
        if self.session.new || original.is_empty() {
            command
        } else {
            original
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Identity<'a> {
    User(&'a str),
    Application(&'a str),
}

impl Identity<'_> {
    /// Channel id for the conversation — prefixed so it never collides with
    /// another connector's chat ids in the shared per-channel history.
    pub fn channel(&self) -> String {
        match self {
            Identity::User(id) => format!("alice:{id}"),
            Identity::Application(id) => format!("alice:app:{id}"),
        }
    }

    pub fn id(&self) -> &str {
        match self {
            Identity::User(id) | Identity::Application(id) => id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> AliceRequest {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn launch_with_request_uses_stripped_command() {
        let r = parse(
            r#"{"session":{"new":true,"skill_id":"s","user":{"user_id":"U"},"application":{"application_id":"A"}},
                "request":{"command":"что у меня завтра","original_utterance":"попроси помощника что у меня завтра","type":"SimpleUtterance"},
                "version":"1.0"}"#,
        );
        assert_eq!(r.text(), "что у меня завтра");
        assert_eq!(r.identity(), Some(Identity::User("U")));
        assert_eq!(r.identity().unwrap().channel(), "alice:U");
    }

    #[test]
    fn mid_session_uses_original_and_falls_back_to_device() {
        let r = parse(
            r#"{"session":{"new":false,"application":{"application_id":"A"}},
                "request":{"command":"привет как дела","original_utterance":"Привет, как дела?"}}"#,
        );
        assert_eq!(r.text(), "Привет, как дела?");
        assert_eq!(r.identity(), Some(Identity::Application("A")));
        assert_eq!(r.version, "1.0");
    }

    #[test]
    fn response_is_clipped_to_limit() {
        let r = AliceResponse::say("я".repeat(2000), "1.0");
        assert_eq!(r.response.text.chars().count(), TEXT_LIMIT);
        assert!(!r.response.end_session);
    }
}
