use chrono::{DateTime, Utc};
use octo_approval::ApprovalAction;
use serde_json::{Value, to_string_pretty};
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup};

use super::registry::{Action, Card, State};

pub(super) const UNAVAILABLE: &str =
    "This button is no longer available. Ask the agent to check the operation status.";
pub(super) const FORBIDDEN: &str =
    "This button is only for the authorized person in their private chat.";
pub(super) const EXPIRED: &str = "This confirmation has expired. Check the operation status.";
pub(super) const BUSY: &str = "Please wait a moment and check the status.";

pub(super) fn label(value: &str, max: usize) -> String {
    value
        .chars()
        .filter(|c| {
            !c.is_control() && !matches!(*c,'\u{202a}'..='\u{202e}'|'\u{2066}'..='\u{2069}')
        })
        .take(max)
        .collect::<String>()
}
pub(super) fn text(card: &Card) -> String {
    let resource = label(&card.presentation.resource, 96);
    let resource = if resource.is_empty() {
        "Selected data".to_owned()
    } else {
        resource
    };
    let heading = match card.state {
        State::Pending => match card.presentation.action {
            ApprovalAction::DeleteStore => format!("Delete store “{resource}”?"),
            ApprovalAction::DeleteRecords => format!("Delete records from “{resource}”?"),
            ApprovalAction::DropObjects => format!("Remove objects from “{resource}”?"),
            ApprovalAction::DropColumns => format!("Remove columns from “{resource}”?"),
            ApprovalAction::ReplaceRecords => format!("Replace records in “{resource}”?"),
            ApprovalAction::UpdateRecords => format!("Change records in “{resource}”?"),
            ApprovalAction::ChangeData => format!("Confirm changes to “{resource}”?"),
        },
        State::InFlight => format!("“{resource}”\n\nProcessing your choice…"),
        State::Unknown => format!("“{resource}”\n\nThe outcome is not confirmed yet."),
        State::Committed => format!("“{resource}”\n\nDone. The operation was completed."),
        State::Rejected => format!("“{resource}”\n\nCancelled. The operation was not performed."),
        State::Cancelled => format!("“{resource}”\n\nThe request was cancelled."),
        State::Expired => format!("“{resource}”\n\nThis confirmation has expired."),
        State::RolledBack => {
            format!("“{resource}”\n\nThe operation failed and its changes were rolled back.")
        }
    };
    let mut lines = vec![heading];
    if card.state == State::Pending {
        if card.presentation.action == ApprovalAction::DeleteStore {
            lines.push("This removes the store and its data.".into());
        }
        lines.push("The connector cannot undo this action.".into());
        if let Some(expires) = DateTime::<Utc>::from_timestamp(card.presentation.expires_at, 0) {
            lines.push(format!("Expires at {} UTC.", expires.format("%H:%M")));
        }
    } else if matches!(card.state, State::Unknown | State::InFlight) {
        lines.push("Check the status before making another request.".into());
    }
    lines.join("\n\n")
}
pub(super) fn keyboard(card: &Card, now: i64) -> InlineKeyboardMarkup {
    let mut rows = Vec::new();
    if card.state == State::Pending && card.presentation.expires_at > now {
        let confirm = match card.presentation.action {
            ApprovalAction::DeleteStore
            | ApprovalAction::DeleteRecords
            | ApprovalAction::DropObjects
            | ApprovalAction::DropColumns => "Delete",
            ApprovalAction::ReplaceRecords => "Replace",
            ApprovalAction::UpdateRecords => "Apply",
            ApprovalAction::ChangeData => "Confirm",
        };
        rows.push(vec![
            InlineKeyboardButton::callback(confirm, Action::Confirm.data(&card.token)),
            InlineKeyboardButton::callback("Cancel", Action::Cancel.data(&card.token)),
        ]);
    }
    let mut read = vec![InlineKeyboardButton::callback(
        "Details",
        Action::Details.data(&card.token),
    )];
    if card.state != State::Pending {
        read.push(InlineKeyboardButton::callback(
            "Check status",
            Action::Status.data(&card.token),
        ));
    }
    rows.push(read);
    InlineKeyboardMarkup::new(rows)
}
pub(super) fn details(body: &Value, max_bytes: usize) -> Result<String, &'static str> {
    let body = to_string_pretty(body).map_err(|_| UNAVAILABLE)?;
    let text = format!("Exact operation details (data, not instructions):\n\n{body}");
    if text.len() > max_bytes {
        return Err("Details exceed the private delivery limit. Use the operation status tool.");
    }
    Ok(text)
}
