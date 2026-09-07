// Copyright 2022 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::anyhow;
use futures::future::join_all;
use tokio::sync::oneshot;

use crate::{MetaError, MetaResult};

pub(crate) type CollectionReceiver = oneshot::Receiver<MetaResult<()>>;
pub(crate) type StartReceiver = oneshot::Receiver<MetaResult<Vec<CollectionReceiver>>>;

pub(crate) async fn wait_collection(receivers: Vec<CollectionReceiver>) -> MetaResult<()> {
    let mut first_error = None;
    for result in join_all(receivers).await {
        let result = match result {
            Ok(result) => result,
            Err(_) => Err(anyhow!("failed to collect barrier: notifier dropped").into()),
        };
        if let Err(err) = result
            && first_error.is_none()
        {
            first_error = Some(err);
        }
    }
    match first_error {
        Some(err) => Err(err),
        None => Ok(()),
    }
}

/// Used for notifying the status of a scheduled command/barrier.
#[derive(Debug)]
pub(crate) struct Notifier {
    started: oneshot::Sender<MetaResult<Vec<CollectionReceiver>>>,
}

impl Notifier {
    pub fn new() -> (Self, StartReceiver) {
        let (started, receiver) = oneshot::channel();
        (Self { started }, receiver)
    }

    pub fn start(self) -> NotifierStarter {
        NotifierStarter {
            started: self.started,
            pending_collection: vec![],
        }
    }

    pub fn notify_start_failed(self, err: MetaError) {
        self.started.send(Err(err)).ok();
    }
}

/// Builds the set of collection notifications before publishing that the command has started.
#[derive(Debug)]
pub(crate) struct NotifierStarter {
    started: oneshot::Sender<MetaResult<Vec<CollectionReceiver>>>,
    pending_collection: Vec<CollectionReceiver>,
}

impl NotifierStarter {
    pub fn add_notify(&mut self) -> CollectionNotifier {
        let (collected, receiver) = oneshot::channel();
        self.pending_collection.push(receiver);
        CollectionNotifier { collected }
    }

    pub fn started(self) {
        self.started.send(Ok(self.pending_collection)).ok();
    }

    pub fn notify_start_failed(self, err: MetaError) {
        self.started.send(Err(err)).ok();
    }
}

/// Notifies the completion of one part of a started command.
#[derive(Debug)]
pub(crate) struct CollectionNotifier {
    collected: oneshot::Sender<MetaResult<()>>,
}

impl CollectionNotifier {
    pub fn notify_collected(self) {
        self.collected.send(Ok(())).ok();
    }

    /// Notify when we failed to collect a barrier. This function consumes `self`.
    pub fn notify_collection_failed(self, err: MetaError) {
        self.collected.send(Err(err)).ok();
    }
}

#[cfg(test)]
mod tests {
    use tokio::time::{Duration, timeout};

    use super::*;

    #[tokio::test]
    async fn test_zero_collection_notifier() {
        let (notifier, started_rx) = Notifier::new();
        notifier.start().started();

        let receivers = started_rx.await.unwrap().unwrap();
        assert!(receivers.is_empty());
        wait_collection(receivers).await.unwrap();
    }

    #[tokio::test]
    async fn test_one_collection_notifier() {
        let (notifier, started_rx) = Notifier::new();
        let mut start = notifier.start();
        let collection = start.add_notify();
        start.started();

        collection.notify_collected();
        let receivers = started_rx.await.unwrap().unwrap();
        wait_collection(receivers).await.unwrap();
    }

    #[tokio::test]
    async fn test_wait_all_collection_notifiers() {
        let (notifier, started_rx) = Notifier::new();
        let mut start = notifier.start();
        let first = start.add_notify();
        let second = start.add_notify();
        start.started();

        let receivers = started_rx.await.unwrap().unwrap();
        let mut wait = Box::pin(wait_collection(receivers));
        first.notify_collected();
        assert!(timeout(Duration::from_millis(10), &mut wait).await.is_err());
        second.notify_collected();
        wait.await.unwrap();
    }

    #[tokio::test]
    async fn test_waits_for_all_collection_notifiers_after_failure() {
        let (notifier, started_rx) = Notifier::new();
        let mut start = notifier.start();
        let first = start.add_notify();
        let second = start.add_notify();
        start.started();

        let receivers = started_rx.await.unwrap().unwrap();
        let mut wait = Box::pin(wait_collection(receivers));
        first.notify_collection_failed(anyhow!("first part failed").into());
        assert!(timeout(Duration::from_millis(10), &mut wait).await.is_err());
        second.notify_collection_failed(anyhow!("second part failed").into());
        let err = wait.await.unwrap_err();
        assert!(err.to_string().contains("first part failed"));
    }

    #[tokio::test]
    async fn test_dropped_collection_notifier_fails() {
        let (notifier, started_rx) = Notifier::new();
        let mut start = notifier.start();
        let dropped = start.add_notify();
        start.started();
        drop(dropped);

        let receivers = started_rx.await.unwrap().unwrap();
        assert!(wait_collection(receivers).await.is_err());
    }

    #[tokio::test]
    async fn test_start_failure() {
        let (notifier, started_rx) = Notifier::new();
        notifier.notify_start_failed(anyhow!("start failed").into());
        assert!(started_rx.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_start_failure_after_entering_start_phase() {
        let (notifier, started_rx) = Notifier::new();
        let mut start = notifier.start();
        let collection = start.add_notify();
        start.notify_start_failed(anyhow!("start failed").into());

        assert!(started_rx.await.unwrap().is_err());
        collection.notify_collected();
    }

    #[tokio::test]
    async fn test_dropped_notifier_cancels_start() {
        let (notifier, started_rx) = Notifier::new();
        drop(notifier);
        assert!(started_rx.await.is_err());
    }
}
