//! Opt-in task bookkeeping for connectors that implement scoped cancellation.
//! No event/agent policy: the caller explicitly chooses when to cancel and the
//! operation chooses how to react to its token (drop I/O or finish safe cleanup).

use std::{collections::HashMap, future::Future};

use tokio::task::{Id, JoinError, JoinSet};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct ScopedTasks {
    tasks: JoinSet<()>,
    running: HashMap<Id, (Option<String>, CancellationToken)>,
}

impl ScopedTasks {
    /// Register before accepting the next control event, so an immediate cancel is not lost.
    pub fn spawn<F, Fut>(&mut self, scope: Option<String>, operation: F)
    where
        F: FnOnce(CancellationToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let token = CancellationToken::new();
        let child = token.clone();
        let handle = self.tasks.spawn(async move {
            operation(child).await;
        });
        self.running.insert(handle.id(), (scope, token));
    }

    /// Notify matching operations; this does not forcibly abort their futures.
    pub fn cancel(&self, scope: &str) {
        if scope.is_empty() {
            return;
        }
        for (key, token) in self.running.values() {
            if key.as_deref() == Some(scope) {
                token.cancel();
            }
        }
    }

    pub fn cancel_all(&self) {
        for (_, token) in self.running.values() {
            token.cancel();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Reap success and panic paths alike; no permanent completed-task entries.
    pub async fn join_next(&mut self) -> Option<Result<(), JoinError>> {
        match self.tasks.join_next_with_id().await? {
            Ok((id, ())) => {
                self.running.remove(&id);
                Some(Ok(()))
            }
            Err(error) => {
                self.running.remove(&error.id());
                Some(Err(error))
            }
        }
    }
}

impl Drop for ScopedTasks {
    fn drop(&mut self) {
        self.cancel_all();
    }
}

#[cfg(test)]
mod tests {
    use super::ScopedTasks;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn cancellation_is_explicit_scoped_and_not_lost_before_task_start() {
        let mut tasks = ScopedTasks::default();
        let completed = Arc::new(AtomicUsize::new(0));
        for scope in ["arm", "camera"] {
            let count = completed.clone();
            tasks.spawn(Some(scope.into()), move |token| async move {
                token.cancelled().await;
                count.fetch_add(1, Ordering::SeqCst);
            });
        }
        tasks.cancel("arm");
        tasks.join_next().await.unwrap().unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 1);
        assert!(!tasks.is_empty());
        tasks.cancel_all();
        tasks.join_next().await.unwrap().unwrap();
        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn a_cooperative_operation_can_finish_cleanup_before_it_exits() {
        let mut tasks = ScopedTasks::default();
        let (cleanup, finished) = oneshot::channel();
        tasks.spawn(Some("operation".into()), |token| async move {
            token.cancelled().await;
            cleanup.send("safe stop completed").unwrap();
        });
        tasks.cancel("operation");
        tasks.join_next().await.unwrap().unwrap();
        assert_eq!(finished.await.unwrap(), "safe stop completed");
    }
}
