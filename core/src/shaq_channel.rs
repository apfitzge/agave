//! In-process bounded channels backed by [`shaq::mpmc`].

use {
    crossbeam_channel::{RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError},
    solana_streamer::streamer::ChannelSend,
    std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
        time::{Duration, Instant},
    },
};

const DISCONNECT_POLL_INTERVAL: Duration = Duration::from_millis(10);

struct ChannelState {
    len: AtomicUsize,
    senders: AtomicUsize,
    receivers: AtomicUsize,
}

/// Sending endpoint for a bounded Shaq MPMC channel.
pub struct Sender<T> {
    producer: shaq::mpmc::Producer<T>,
    state: Arc<ChannelState>,
}

/// Receiving endpoint for a bounded Shaq MPMC channel.
pub struct Receiver<T> {
    consumer: shaq::mpmc::Consumer<T>,
    state: Arc<ChannelState>,
}

/// A sender which evicts the oldest queued value when full.
pub struct EvictingSender<T> {
    sender: Sender<T>,
    receiver: Receiver<T>,
}

/// Creates a bounded in-process Shaq MPMC channel.
pub fn bounded<T: Send>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (producer, consumer) =
        shaq::mpmc::pair(capacity).expect("failed to create Shaq MPMC channel");
    let state = Arc::new(ChannelState {
        len: AtomicUsize::new(0),
        senders: AtomicUsize::new(1),
        receivers: AtomicUsize::new(1),
    });
    (
        Sender {
            producer,
            state: state.clone(),
        },
        Receiver { consumer, state },
    )
}

impl<T> Sender<T> {
    /// Attempts to enqueue a value without blocking.
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        if self.state.receivers.load(Ordering::Acquire) == 0 {
            return Err(TrySendError::Disconnected(value));
        }

        // Count the value before publication so a consumer can never observe
        // it before the length has been incremented.
        self.state.len.fetch_add(1, Ordering::Relaxed);
        match self.producer.try_write(value) {
            Ok(()) => Ok(()),
            Err(value) => {
                self.state.len.fetch_sub(1, Ordering::Relaxed);
                if self.state.receivers.load(Ordering::Acquire) == 0 {
                    Err(TrySendError::Disconnected(value))
                } else {
                    Err(TrySendError::Full(value))
                }
            }
        }
    }

    /// Enqueues a value, waiting for capacity while receivers remain alive.
    pub fn send(&self, mut value: T) -> Result<(), SendError<T>> {
        loop {
            match self.try_send(value) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(returned)) => {
                    value = returned;
                    thread::yield_now();
                }
                Err(TrySendError::Disconnected(returned)) => {
                    return Err(SendError(returned));
                }
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.state.len.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.state.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            producer: self.producer.clone(),
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.state.senders.fetch_sub(1, Ordering::Release);
    }
}

impl<T> Receiver<T> {
    /// Attempts to dequeue a value without blocking.
    #[inline]
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(value) = self.consumer.try_read() {
            self.state.len.fetch_sub(1, Ordering::Relaxed);
            return Ok(value);
        }
        if self.state.senders.load(Ordering::Acquire) == 0 {
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Dequeues a value, waiting until the timeout or sender disconnection.
    pub fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let start = Instant::now();
        loop {
            match self.try_recv() {
                Ok(value) => return Ok(value),
                Err(TryRecvError::Disconnected) => {
                    return Err(RecvTimeoutError::Disconnected);
                }
                Err(TryRecvError::Empty) => {}
            }

            let Some(remaining) = timeout.checked_sub(start.elapsed()) else {
                return Err(RecvTimeoutError::Timeout);
            };
            if remaining.is_zero() {
                return Err(RecvTimeoutError::Timeout);
            }
            let wait = remaining.min(DISCONNECT_POLL_INTERVAL);
            if let Ok(value) = self.consumer.read_timeout(wait) {
                self.state.len.fetch_sub(1, Ordering::Relaxed);
                return Ok(value);
            }
        }
    }

    /// Dequeues a value, waiting while any sender remains alive.
    pub fn recv(&self) -> Result<T, RecvError> {
        loop {
            match self.recv_timeout(DISCONNECT_POLL_INTERVAL) {
                Ok(value) => return Ok(value),
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => return Err(RecvError),
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.state.len.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.state.receivers.fetch_add(1, Ordering::Relaxed);
        Self {
            consumer: self.consumer.clone(),
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.state.receivers.fetch_sub(1, Ordering::Release);
    }
}

impl<T> EvictingSender<T> {
    pub fn new(sender: Sender<T>, receiver: Receiver<T>) -> Self {
        Self { sender, receiver }
    }

    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let Err(err) = self.sender.try_send(value) else {
            return Ok(());
        };
        match err {
            TrySendError::Full(value) => match self.receiver.try_recv() {
                Ok(older) => {
                    self.sender.try_send(value)?;
                    Err(TrySendError::Full(older))
                }
                Err(TryRecvError::Empty) => self.sender.try_send(value),
                Err(TryRecvError::Disconnected) => {
                    unreachable!("evicting sender retains a receiving endpoint")
                }
            },
            TrySendError::Disconnected(_) => {
                unreachable!("evicting sender retains a receiving endpoint")
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.receiver.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.receiver.is_empty()
    }
}

impl<T> Clone for EvictingSender<T> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            receiver: self.receiver.clone(),
        }
    }
}

impl<T> ChannelSend<T> for Sender<T>
where
    T: Send + 'static,
{
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.try_send(value)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl<T> ChannelSend<T> for EvictingSender<T>
where
    T: Send + 'static,
{
    fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        self.try_send(value)
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_receive_and_disconnect() {
        let (sender, receiver) = bounded(2);
        sender.send(1).unwrap();
        sender.send(2).unwrap();
        assert_eq!(receiver.len(), 2);
        assert_eq!(receiver.try_recv(), Ok(1));
        assert_eq!(receiver.recv(), Ok(2));
        drop(sender);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));
    }

    #[test]
    fn test_disconnect_tracks_cloned_endpoints() {
        let (sender, receiver) = bounded::<i32>(1);
        let sender_clone = sender.clone();
        drop(sender);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Empty));
        drop(sender_clone);
        assert_eq!(receiver.try_recv(), Err(TryRecvError::Disconnected));

        let (sender, receiver) = bounded(1);
        let receiver_clone = receiver.clone();
        drop(receiver);
        sender.try_send(1).unwrap();
        assert_eq!(receiver_clone.try_recv(), Ok(1));
        drop(receiver_clone);
        assert_eq!(sender.try_send(2), Err(TrySendError::Disconnected(2)));
    }

    #[test]
    fn test_evicting_sender() {
        let (sender, receiver) = bounded(2);
        let sender = EvictingSender::new(sender, receiver.clone());
        sender.try_send(1).unwrap();
        sender.try_send(2).unwrap();
        assert_eq!(sender.try_send(3), Err(TrySendError::Full(1)));
        assert_eq!(receiver.try_recv(), Ok(2));
        assert_eq!(receiver.try_recv(), Ok(3));
    }
}
