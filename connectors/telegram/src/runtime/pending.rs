use std::{collections::HashMap, sync::Arc};

use futures::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesOrdered};
use octo_core::TrustLevel;
use teloxide::{Bot, types::Message};

use super::media::receive_media;
use crate::{Role, TelegramConnector, batch::Emit, source::MessageSource};

pub(super) struct MediaInput {
    pub source: MessageSource,
    pub chat: String,
    pub native_base: Option<String>,
    pub key: Option<String>,
    pub buffer: bool,
    pub trust: Option<(Role, TrustLevel)>,
}
pub(super) struct ReadyMedia {
    pub input: MediaInput,
    pub result: Option<(Emit, bool)>,
}

/// Downloads progress without blocking native controls. Ordered completions keep
/// album constituents in arrival order; their host stamp is never resolved again.
#[derive(Default)]
pub(super) struct MediaQueue {
    tasks: FuturesOrdered<BoxFuture<'static, ReadyMedia>>,
    addressed: HashMap<String, usize>,
}
impl MediaQueue {
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
    pub fn full(&self) -> bool {
        self.tasks.len() >= 32
    }
    pub fn addressed(&self, base: &str) -> bool {
        self.addressed.contains_key(base)
    }
    pub fn push(
        &mut self,
        connector: Arc<TelegramConnector>,
        bot: Bot,
        msg: Message,
        input: MediaInput,
        reply: Option<String>,
    ) {
        if input
            .source
            .tags
            .get("addressed")
            .is_some_and(|v| v == "true")
        {
            if let Some(base) = &input.native_base {
                *self.addressed.entry(base.clone()).or_default() += 1;
            }
        }
        self.tasks.push_back(
            async move {
                let result = receive_media(&connector, &bot, &msg, &input.source, &reply).await;
                ReadyMedia { input, result }
            }
            .boxed(),
        );
    }
    pub async fn next(&mut self) -> Option<ReadyMedia> {
        let ready = self.tasks.next().await?;
        if ready
            .input
            .source
            .tags
            .get("addressed")
            .is_some_and(|v| v == "true")
        {
            if let Some(base) = &ready.input.native_base {
                if let Some(count) = self.addressed.get_mut(base) {
                    *count -= 1;
                    if *count == 0 {
                        self.addressed.remove(base);
                    }
                }
            }
        }
        Some(ready)
    }
}
