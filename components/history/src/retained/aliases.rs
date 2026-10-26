use serde_json::to_vec;

use super::{Receipt, RetainedMemoryHistory, State};
use crate::aliases::verified_aliases;
use crate::{AppendReceipt, AppendRequest, HistoryError, RequestAlias, Result};

impl RetainedMemoryHistory {
    pub(super) fn append_aliased_locked(
        &self,
        state: &mut State,
        key: &str,
        request: &AppendRequest,
        aliases: &[RequestAlias],
    ) -> Result<AppendReceipt> {
        let aliases = verified_aliases(request, aliases)?;
        let batch = to_vec(&request.turns)?;
        let primary = state.journals.get(key).and_then(|j| {
            j.receipts
                .get(&(request.namespace.clone(), request.idempotency_key.clone()))
        });
        let expected = if let Some(primary) = primary {
            if primary.digest != request.digest || primary.batch != batch {
                return Err(HistoryError::IdempotencyConflict);
            }
            primary.receipt.clone()
        } else {
            AppendReceipt {
                record_ids: (1..=request.turns.len())
                    .map(|n| state.next_turn + n as i64)
                    .collect(),
                through_id: state.next_turn + request.turns.len() as i64,
                duplicate: false,
            }
        };
        let mut new_aliases = Vec::new();
        let mut alias_bytes = 0;
        for alias in aliases {
            let stored = state.journals.get(key).and_then(|j| {
                j.receipts
                    .get(&(alias.namespace.clone(), alias.idempotency_key.clone()))
            });
            if let Some(stored) = stored {
                if stored.digest != alias.digest
                    || stored.batch != batch
                    || stored.receipt.record_ids != expected.record_ids
                {
                    return Err(HistoryError::IdempotencyConflict);
                }
            } else {
                alias_bytes += batch.len()
                    + alias.namespace.len()
                    + alias.idempotency_key.len()
                    + alias.digest.len()
                    + to_vec(&expected)?.len();
                new_aliases.push(alias);
            }
        }
        // Reserve aliases first. append_locked either commits its complete batch or
        // changes nothing; undo this reservation on any primary/quota failure.
        self.reserve(state, alias_bytes, 0)?;
        let receipt = match self.append_locked(state, key, &request.turns, Some(request)) {
            Ok(receipt) => receipt,
            Err(error) => {
                state.bytes -= alias_bytes;
                return Err(error);
            }
        };
        let journal = state.journals.get_mut(key).expect("accepted journal");
        journal.bytes += alias_bytes;
        for alias in new_aliases {
            let mut bound = receipt.clone();
            bound.duplicate = false;
            journal.receipts.insert(
                (alias.namespace.clone(), alias.idempotency_key.clone()),
                Receipt {
                    digest: alias.digest.clone(),
                    batch: batch.clone(),
                    receipt: bound,
                },
            );
        }
        Ok(receipt)
    }
}
