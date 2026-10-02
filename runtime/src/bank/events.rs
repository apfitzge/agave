use {
    agave_event_system::{
        CreateStreamError, EventSystem, PublisherFactory, StreamConfig, event,
        monotonic_timestamp_ns, publisher::Publisher, stream_name::StreamName,
    },
    log::*,
    solana_account::{AccountSharedData, ReadableAccount},
    solana_clock::{BankId, Epoch, Slot},
    solana_pubkey::Pubkey,
    std::{
        cell::RefCell,
        collections::HashMap,
        sync::{
            Arc, Weak,
            atomic::{AtomicU64, Ordering::Relaxed},
        },
    },
};

#[cfg(all(test, target_os = "linux"))]
mod tests;

pub const ACCOUNT_COMMITTED_STREAM: StreamName =
    agave_event_system::stream_name!("account.committed");

/// An account stored by a transaction commit, including fee/nonce rollback accounts.
/// Sysvar updates, rewards, snapshot restoration, and other bank writes are excluded.
/// The event is published after storage, before the transaction's locks are released.
/// Delivery is best effort, as with other validator event streams.
#[event]
#[derive(Debug, PartialEq, Eq)]
pub struct AccountCommitted<'a> {
    pub timestamp_ns: u64,
    pub slot: Slot,
    pub bank_id: BankId,
    /// Unique within this event stream; orders repeated writes to the same account.
    /// This is independent of Geyser's write version.
    pub write_version: u64,
    pub pubkey: [u8; 32],
    pub owner: [u8; 32],
    pub lamports: u64,
    pub executable: bool,
    pub rent_epoch: Epoch,
    #[payload]
    pub data: &'a [u8],
}

type Factory = PublisherFactory<AccountCommitted<'static>>;

struct ThreadPublisher {
    factory: Weak<Factory>,
    // Slots are a lifetime budget. Remember exhaustion instead of retrying each commit.
    publisher: Option<Publisher<AccountCommitted<'static>>>,
}

thread_local! {
    // Publishers must be created and used on the same thread. Keep separate entries
    // for independent bank trees (e.g. multiple validators in the same process).
    static PUBLISHERS: RefCell<HashMap<usize, ThreadPublisher>> = RefCell::default();
}

#[derive(Debug)]
pub(super) struct AccountEventPublishers {
    factory: Arc<Factory>,
    next_write_version: AtomicU64,
}

impl AccountEventPublishers {
    pub(super) fn new(event_system: &EventSystem) -> Result<Self, CreateStreamError> {
        Self::with_config(
            event_system,
            StreamConfig {
                capacity: 1024,
                // A lifetime budget for banking and replay threads, not a concurrency limit.
                publisher_slots: 256,
                subscriber_slots: 8,
            },
            // Per publisher; accommodates maximum-sized accounts and a backlog.
            64 * 1024 * 1024,
        )
    }

    fn with_config(
        event_system: &EventSystem,
        config: StreamConfig,
        payload_capacity: u64,
    ) -> Result<Self, CreateStreamError> {
        let factory = event_system.create_stream_with_payloads::<AccountCommitted>(
            ACCOUNT_COMMITTED_STREAM,
            config,
            payload_capacity,
        )?;
        Ok(Self {
            factory: Arc::new(factory),
            next_write_version: AtomicU64::new(0),
        })
    }

    pub(super) fn publish(
        &self,
        slot: Slot,
        bank_id: BankId,
        accounts: &[(&Pubkey, &AccountSharedData)],
    ) {
        if accounts.is_empty() {
            return;
        }
        let first_write_version = self
            .next_write_version
            .fetch_add(accounts.len() as u64, Relaxed);
        PUBLISHERS.with_borrow_mut(|publishers| {
            let key = Arc::as_ptr(&self.factory) as usize;
            if !publishers.contains_key(&key) {
                // Release publishers belonging to bank trees that have been dropped.
                // Each weak reference keeps its allocation address unique until removed.
                publishers.retain(|_, entry| entry.factory.strong_count() != 0);
                publishers.insert(
                    key,
                    ThreadPublisher {
                        factory: Arc::downgrade(&self.factory),
                        publisher: self.factory.try_create_publisher(),
                    },
                );
            }
            let Some(publisher) = publishers.get_mut(&key).unwrap().publisher.as_mut() else {
                inc_new_counter_error!("account-committed-events-dropped", accounts.len());
                return;
            };
            let mut dropped = 0;
            for (index, (pubkey, account)) in accounts.iter().enumerate() {
                // Payload publication copies directly from the committed account into
                // shared memory, without cloning or staging its data in an owned buffer.
                if publisher
                    .publish(&AccountCommitted {
                        timestamp_ns: monotonic_timestamp_ns(),
                        slot,
                        bank_id,
                        write_version: first_write_version + index as u64,
                        pubkey: pubkey.to_bytes(),
                        owner: account.owner().to_bytes(),
                        lamports: account.lamports(),
                        executable: account.executable(),
                        rent_epoch: account.rent_epoch(),
                        data: account.data(),
                    })
                    .is_err()
                {
                    dropped += 1;
                }
            }
            if dropped != 0 {
                inc_new_counter_error!("account-committed-events-dropped", dropped);
            }
        });
    }
}
