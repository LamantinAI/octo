use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use octo_approval::ApprovalPresentation;
use octo_core::{ConnectorId, Envelope};
use rand::{RngCore, rngs::OsRng};
use teloxide::types::{CallbackQuery, MessageId};

use super::delivery::CardDelivery;

#[derive(Clone, Debug)]
pub(crate) struct CardSettings {
    pub max_cards: usize,
    pub retention_seconds: u64,
    pub max_detail_bytes: usize,
}
impl Default for CardSettings {
    fn default() -> Self {
        Self {
            max_cards: 128,
            retention_seconds: 3600,
            max_detail_bytes: 262_144,
        }
    }
}
impl CardSettings {
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=1024).contains(&self.max_cards)
            || !(60..=86_400).contains(&self.retention_seconds)
            || !(1024..=1_048_576).contains(&self.max_detail_bytes)
        {
            return Err("invalid approval card limits".into());
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Action {
    Confirm,
    Cancel,
    Details,
    Status,
}
impl Action {
    fn code(self) -> &'static str {
        match self {
            Self::Confirm => "y",
            Self::Cancel => "n",
            Self::Details => "d",
            Self::Status => "s",
        }
    }
    pub fn data(self, token: &str) -> String {
        format!("oa:{token}:{}", self.code())
    }
    pub fn parse(data: &str) -> Option<(String, Self)> {
        let parts = data.split(':').collect::<Vec<_>>();
        if parts.len() != 3
            || parts[0] != "oa"
            || parts[1].len() != 32
            || !parts[1].bytes().all(|c| c.is_ascii_hexdigit())
        {
            return None;
        }
        let action = match parts[2] {
            "y" => Self::Confirm,
            "n" => Self::Cancel,
            "d" => Self::Details,
            "s" => Self::Status,
            _ => return None,
        };
        Some((parts[1].into(), action))
    }
    pub fn is_decision(self) -> bool {
        matches!(self, Self::Confirm | Self::Cancel)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum State {
    Pending,
    InFlight,
    Unknown,
    Committed,
    Rejected,
    Cancelled,
    Expired,
    RolledBack,
}
impl State {
    pub fn definitive(self) -> bool {
        matches!(
            self,
            Self::Committed | Self::Rejected | Self::Cancelled | Self::Expired | Self::RolledBack
        )
    }
}
#[derive(Clone, Debug)]
pub(super) struct Card {
    pub token: String,
    pub presentation: ApprovalPresentation,
    pub actor: u64,
    pub chat: i64,
    pub message: Option<MessageId>,
    pub state: State,
    pub delivery: Option<Arc<CardDelivery>>,
    retain_until: Instant,
}
#[derive(Debug)]
pub(super) enum Reject {
    Invalid,
    Expired(Box<Card>),
    Unavailable,
    Forbidden,
}
pub(super) struct Registry {
    pub cards: HashMap<String, Card>,
    pub settings: CardSettings,
}
impl Registry {
    pub fn new(settings: CardSettings) -> Self {
        Self {
            cards: HashMap::new(),
            settings,
        }
    }
    pub fn presentation(envelope: &Envelope, id: &ConnectorId) -> Option<ApprovalPresentation> {
        let presentation = envelope.context.get::<ApprovalPresentation>()?;
        let chat = presentation.private_destination.parse::<i64>().ok()?;
        if envelope.kind.as_str() != "chat.reply"
            || envelope.payload_as::<String>().is_none()
            || chat <= 0
            || envelope.source.as_str() != presentation.executor
            || envelope.target.as_ref() != Some(id)
            || envelope.channel.as_ref().map(|channel| channel.as_str())
                != Some(presentation.private_destination.as_str())
            || presentation.recipient_principal != format!("{}:user:{chat}", id.as_str())
            || presentation.operation_id.is_empty()
            || presentation.operation_id.len() > 128
            || presentation.operation_hash.is_empty()
            || presentation.operation_hash.len() > 128
            || presentation.executor.is_empty()
            || presentation.executor.len() > 128
            || presentation.decision_kind.is_empty()
            || presentation.status_kind.is_empty()
            || presentation.decision_kind.len() > 128
            || presentation.status_kind.len() > 128
        {
            return None;
        }
        Some(presentation.clone())
    }
    pub fn reserve(
        &mut self,
        presentation: ApprovalPresentation,
        now: i64,
    ) -> Result<Card, Reject> {
        self.cards
            .retain(|_, card| card.retain_until > Instant::now());
        if presentation.expires_at <= now {
            return Err(Reject::Unavailable);
        }
        if self.cards.values().any(|card| {
            card.presentation.executor == presentation.executor
                && card.presentation.operation_id == presentation.operation_id
        }) {
            return Err(Reject::Unavailable);
        }
        if self.cards.len() >= self.settings.max_cards {
            if let Some(token) = self
                .cards
                .iter()
                .filter(|(_, card)| card.state.definitive() || card.presentation.expires_at <= now)
                .min_by_key(|(_, card)| card.retain_until)
                .map(|(token, _)| token.clone())
            {
                self.cards.remove(&token);
            }
        }
        if self.cards.len() >= self.settings.max_cards {
            return Err(Reject::Unavailable);
        }
        let mut random = [0u8; 16];
        OsRng.fill_bytes(&mut random);
        let token = random
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let chat = presentation
            .private_destination
            .parse()
            .map_err(|_| Reject::Invalid)?;
        let card = Card {
            token: token.clone(),
            actor: chat as u64,
            chat,
            presentation,
            message: None,
            state: State::Pending,
            delivery: None,
            retain_until: Instant::now() + Duration::from_secs(self.settings.retention_seconds),
        };
        self.cards.insert(token, card.clone());
        Ok(card)
    }
    pub fn activate(&mut self, token: &str, message: MessageId) {
        if let Some(card) = self.cards.get_mut(token) {
            card.message = Some(message);
        }
    }
    pub fn begin(
        &mut self,
        query: &CallbackQuery,
        owner: bool,
        now: i64,
    ) -> Result<(Card, Action), Reject> {
        let (token, action) =
            Action::parse(query.data.as_deref().ok_or(Reject::Invalid)?).ok_or(Reject::Invalid)?;
        let card = self.cards.get_mut(&token).ok_or(Reject::Unavailable)?;
        let message = query.regular_message().ok_or(Reject::Forbidden)?;
        if !owner
            || query.from.is_bot
            || query.inline_message_id.is_some()
            || !message.chat.is_private()
            || query.from.id.0 != card.actor
            || message.chat.id.0 != card.chat
            || card.message != Some(message.id)
            || card.retain_until <= Instant::now()
        {
            return Err(Reject::Forbidden);
        }
        if action.is_decision() && now >= card.presentation.expires_at {
            if card.state == State::Pending {
                card.state = State::Expired;
            }
            return Err(Reject::Expired(Box::new(card.clone())));
        }
        let effective = if action.is_decision() && card.state != State::Pending {
            Action::Status
        } else {
            action
        };
        if effective.is_decision() {
            card.state = State::InFlight;
        }
        Ok((card.clone(), effective))
    }
    pub fn finish(&mut self, token: &str, state: State) -> Option<Card> {
        let card = self.cards.get_mut(token)?;
        // Status responses cannot revive a decision after an uncertain send.
        if (!card.state.definitive() || state.definitive())
            && (state != State::Pending || card.state == State::Pending)
        {
            card.state = state;
        }
        Some(card.clone())
    }
}
