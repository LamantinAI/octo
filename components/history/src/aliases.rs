use std::collections::HashMap;

use crate::{AppendRequest, HistoryError, RequestAlias, Result};

pub(crate) fn verified_aliases<'a>(
    request: &AppendRequest,
    aliases: &'a [RequestAlias],
) -> Result<Vec<&'a RequestAlias>> {
    request.validate()?;
    if aliases.len() > 1000 {
        return Err(HistoryError::InvalidRequest(
            "too many request aliases".into(),
        ));
    }
    let mut identities = HashMap::new();
    let mut unique = Vec::new();
    for alias in aliases {
        alias.validate()?;
        if alias.namespace == request.namespace && alias.idempotency_key == request.idempotency_key
        {
            if alias.digest != request.digest {
                return Err(HistoryError::IdempotencyConflict);
            }
            continue;
        }
        if let Some(previous) =
            identities.insert((&alias.namespace, &alias.idempotency_key), &alias.digest)
        {
            if previous != &alias.digest {
                return Err(HistoryError::IdempotencyConflict);
            }
        } else {
            unique.push(alias);
        }
    }
    Ok(unique)
}
