// SPDX-License-Identifier: Apache-2.0
//! Graceful shutdown signal coordination.

use tokio::sync::{broadcast, watch};

/// A shutdown signal that can be shared across tasks.
#[derive(Clone)]
pub struct ShutdownSignal {
    /// Watch channel: `true` = shutdown requested.
    receiver: watch::Receiver<bool>,
}

impl ShutdownSignal {
    /// Returns true if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        *self.receiver.borrow()
    }

    /// Waits until shutdown is requested.
    pub async fn wait(&mut self) {
        if *self.receiver.borrow() {
            return;
        }
        // Ignore errors — sender dropped means shutdown
        let _ = self.receiver.changed().await;
    }
}

/// Controller that triggers shutdown.
pub struct ShutdownController {
    sender: watch::Sender<bool>,
    /// Broadcast channel to notify all pipeline components.
    notify: broadcast::Sender<()>,
}

impl ShutdownController {
    /// Creates a new shutdown controller and signal pair.
    pub fn new() -> (Self, ShutdownSignal) {
        let (sender, receiver) = watch::channel(false);
        let (notify, _) = broadcast::channel(1);
        (Self { sender, notify }, ShutdownSignal { receiver })
    }

    /// Triggers shutdown.
    pub fn shutdown(&self) {
        let _ = self.sender.send(true);
        let _ = self.notify.send(());
    }

    /// Subscribes to shutdown notifications.
    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.notify.subscribe()
    }

    /// Creates a new `ShutdownSignal` clone.
    pub fn signal(&self) -> ShutdownSignal {
        ShutdownSignal {
            receiver: self.sender.subscribe(),
        }
    }
}

impl Default for ShutdownController {
    fn default() -> Self {
        Self::new().0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_shutdown_signal() {
        let (controller, mut signal) = ShutdownController::new();
        assert!(!signal.is_shutdown());
        controller.shutdown();
        signal.wait().await;
        assert!(signal.is_shutdown());
    }

    #[tokio::test]
    async fn test_multiple_signals() {
        let (controller, _signal) = ShutdownController::new();
        let mut sig2 = controller.signal();
        let mut sig3 = controller.signal();
        controller.shutdown();
        sig2.wait().await;
        sig3.wait().await;
        assert!(sig2.is_shutdown());
        assert!(sig3.is_shutdown());
    }

    #[test]
    fn test_shutdown_not_requested() {
        let (_controller, signal) = ShutdownController::new();
        assert!(!signal.is_shutdown());
    }

    #[test]
    fn test_shutdown_controller_default() {
        let controller = ShutdownController::default();
        let signal = controller.signal();
        assert!(!signal.is_shutdown());
        controller.shutdown();
        assert!(signal.is_shutdown());
    }

    #[tokio::test]
    async fn test_wait_already_shutdown() {
        let (controller, _signal) = ShutdownController::new();
        controller.shutdown();
        let mut sig = controller.signal();
        // Wait should return immediately since already shut down
        sig.wait().await;
        assert!(sig.is_shutdown());
    }

    #[test]
    fn test_subscribe() {
        let (controller, _signal) = ShutdownController::new();
        let _rx = controller.subscribe();
        controller.shutdown();
    }

    #[test]
    fn test_signal_clone() {
        let (_controller, signal) = ShutdownController::new();
        let signal2 = signal.clone();
        assert!(!signal2.is_shutdown());
    }
}
