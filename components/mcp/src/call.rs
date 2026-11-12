use std::{
    sync::atomic::AtomicBool, sync::atomic::AtomicU64, sync::atomic::Ordering, time::Duration,
};

use rmcp::{model::CallToolRequestParams, service::ServiceError};
use serde_json::to_vec;
use tokio::{select, time::timeout};
use tokio_util::sync::CancellationToken;

use crate::connection::CallLease;
use crate::{CallToolResult, JsonObject, McpError, Result, ServerSession};

struct InvocationGuard<'a> {
    invalid: &'a AtomicU64,
    epoch: u64,
    received: bool,
    _lease: CallLease,
}
impl Drop for InvocationGuard<'_> {
    fn drop(&mut self) {
        if !self.received {
            self.invalid.fetch_max(self.epoch, Ordering::AcqRel);
        }
    }
}
impl ServerSession {
    /// No replay: a dropped, cancelled or timed-out started call is indeterminate.
    /// Result size is a post-decode ceiling; the SDK owns transport allocation.
    pub async fn call(
        &self,
        revision: &str,
        name: &str,
        arguments: JsonObject,
        cancel: CancellationToken,
    ) -> Result<CallToolResult> {
        let snapshot = self.snapshot();
        if snapshot.revision != revision {
            return Err(McpError::SchemaChanged);
        }
        if snapshot.find(name).is_none() {
            return Err(McpError::UnknownTool);
        }
        if to_vec(&arguments)
            .map_err(|_| McpError::InvalidArguments)?
            .len()
            > self.limits.max_argument_bytes
        {
            return Err(McpError::InvalidArguments);
        }
        let started = AtomicBool::new(false);
        let execution = async {
            let _permit = self.calls.acquire().await.map_err(|_| McpError::Closed)?;
            let (peer, epoch, lease) = self.peer(revision).await?;
            let mut guard = InvocationGuard {
                invalid: &self.invalid_epoch,
                epoch,
                received: false,
                _lease: lease,
            };
            started.store(true, Ordering::Release);
            let result = peer
                .call_tool(CallToolRequestParams::new(name.to_owned()).with_arguments(arguments))
                .await;
            let result = match result {
                Ok(result) => {
                    guard.received = true;
                    result
                }
                Err(ServiceError::McpError(_)) => {
                    guard.received = true;
                    return Err(McpError::Rejected);
                }
                Err(_) => return Err(McpError::Indeterminate),
            };
            if to_vec(&result).map_err(|_| McpError::Protocol)?.len() > self.limits.max_result_bytes
            {
                return Err(McpError::ResultTooLarge);
            }
            Ok(result)
        };
        select! {
            biased;
            _=cancel.cancelled()=>Err(if started.load(Ordering::Acquire){McpError::Indeterminate}else{McpError::Cancelled}),
            _=self.shutdown.cancelled()=>Err(if started.load(Ordering::Acquire){McpError::Indeterminate}else{McpError::Closed}),
            result=timeout(Duration::from_millis(self.limits.call_timeout_ms),execution)=>result.unwrap_or_else(|_|Err(if started.load(Ordering::Acquire){McpError::Indeterminate}else{McpError::Timeout})),
        }
    }
}
