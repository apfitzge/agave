pub use generated::agave::ledger::broadcast_events::{
    BankEventPayload, FrozenBankEvent, NewBankEvent,
};
use {
    flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset},
    generated::agave::ledger::broadcast_events as generated_events,
    libc::{CLOCK_MONOTONIC, clock_gettime, timespec},
    shaq::broadcast::{BroadcastConfig, Producer},
    solana_runtime::bank::Bank,
    std::{
        cell::RefCell,
        ffi::OsStr,
        fs::{self, File, OpenOptions},
        io::Write,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
};

pub const BANK_EVENTS_SCHEMA: &[u8] = include_bytes!("broadcast_events/schemas/bank_events.bfbs");
pub const BANK_EVENT_QUEUE_PAYLOAD_SIZE: usize = 128;
const FLATBUFFER_SIZE_PREFIX_SIZE: usize = 4;
pub type BankEventProducer = Producer<BankEvent>;

thread_local! {
    static BANK_EVENT_BUILDER: RefCell<FlatBufferBuilder<'static>> =
        RefCell::new(FlatBufferBuilder::with_capacity(BANK_EVENT_QUEUE_PAYLOAD_SIZE));
}

mod generated {
    #![allow(clippy::all)]
    #![allow(dead_code)]
    #![allow(missing_docs)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(non_upper_case_globals)]
    #![allow(unsafe_op_in_unsafe_fn)]

    include!("broadcast_events/generated/bank_events_generated.rs");
}

#[repr(C, align(8))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BankEvent {
    flatbuffer: [u8; BANK_EVENT_QUEUE_PAYLOAD_SIZE],
}

impl Default for BankEvent {
    fn default() -> Self {
        Self {
            flatbuffer: [0; BANK_EVENT_QUEUE_PAYLOAD_SIZE],
        }
    }
}

impl BankEvent {
    pub fn flatbuffer(&self) -> &[u8] {
        let len = u32::from_le_bytes(
            self.flatbuffer[..FLATBUFFER_SIZE_PREFIX_SIZE]
                .try_into()
                .expect("slice length checked"),
        ) as usize;
        let end = FLATBUFFER_SIZE_PREFIX_SIZE
            .checked_add(len)
            .expect("bank event length overflow");
        assert!(
            end <= BANK_EVENT_QUEUE_PAYLOAD_SIZE,
            "invalid bank event length {len}"
        );
        &self.flatbuffer[..end]
    }

    fn from_flatbuffer(flatbuffer: &[u8]) -> Self {
        assert!(
            flatbuffer.len() <= BANK_EVENT_QUEUE_PAYLOAD_SIZE,
            "bank event flatbuffer size {} exceeds max {}",
            flatbuffer.len(),
            BANK_EVENT_QUEUE_PAYLOAD_SIZE
        );

        let mut event = Self {
            flatbuffer: [0; BANK_EVENT_QUEUE_PAYLOAD_SIZE],
        };
        event.flatbuffer[..flatbuffer.len()].copy_from_slice(flatbuffer);
        event
    }
}

pub fn create_event_queue<T>(
    events_dir: &Path,
    queue_name: &str,
    schema: &[u8],
    capacity: usize,
    producer_slots: usize,
    consumer_slots: usize,
) -> std::io::Result<Producer<T>> {
    fs::create_dir_all(events_dir)?;
    if !events_dir.is_dir() {
        return Err(std::io::Error::other(format!(
            "events path is not a directory: {}",
            events_dir.display()
        )));
    }

    remove_stale_temp_files(events_dir, queue_name)?;

    let (queue_tmp_path, queue_file) = create_temp_file(events_dir, queue_name, "queue")?;
    let producer = unsafe {
        Producer::create(
            &queue_file,
            BroadcastConfig {
                capacity,
                producer_slots,
                consumer_slots,
            },
        )
    }
    .map_err(|err| {
        std::io::Error::other(format!(
            "failed to create event queue {queue_name}: {err:?}"
        ))
    })?;
    drop(queue_file);

    let (schema_tmp_path, mut schema_file) = create_temp_file(events_dir, queue_name, "bfbs")?;
    schema_file.write_all(schema)?;
    drop(schema_file);

    fs::rename(&queue_tmp_path, event_queue_path(events_dir, queue_name))?;
    fs::rename(&schema_tmp_path, event_schema_path(events_dir, queue_name))?;

    Ok(producer)
}

pub fn event_queue_path(events_dir: &Path, queue_name: &str) -> PathBuf {
    events_dir.join(queue_name)
}

pub fn event_schema_path(events_dir: &Path, queue_name: &str) -> PathBuf {
    events_dir.join(format!("{queue_name}.bfbs"))
}

pub fn new_bank_event(bank: &Bank) -> BankEvent {
    BANK_EVENT_BUILDER.with(|builder| {
        let mut builder = builder.borrow_mut();
        builder.reset();
        let timestamp = monotonic_clock_timestamp_ns();
        let parent_hash = generated_events::Hash::new(&bank.parent_hash().to_bytes());
        let payload = NewBankEvent::create(
            &mut builder,
            &generated_events::NewBankEventArgs {
                slot: bank.slot(),
                parent_slot: bank.parent_slot(),
                parent_hash: Some(&parent_hash),
            },
        );
        build_bank_event(
            &mut builder,
            timestamp,
            BankEventPayload::NewBankEvent,
            payload.as_union_value(),
        )
    })
}

pub fn frozen_bank_event(bank: &Bank) -> BankEvent {
    BANK_EVENT_BUILDER.with(|builder| {
        let mut builder = builder.borrow_mut();
        builder.reset();
        let timestamp = monotonic_clock_timestamp_ns();
        let bank_hash = generated_events::Hash::new(&bank.hash().to_bytes());
        let payload = FrozenBankEvent::create(
            &mut builder,
            &generated_events::FrozenBankEventArgs {
                slot: bank.slot(),
                bank_hash: Some(&bank_hash),
            },
        );
        build_bank_event(
            &mut builder,
            timestamp,
            BankEventPayload::FrozenBankEvent,
            payload.as_union_value(),
        )
    })
}

fn build_bank_event(
    builder: &mut FlatBufferBuilder<'static>,
    timestamp: u64,
    payload_type: BankEventPayload,
    payload: WIPOffset<UnionWIPOffset>,
) -> BankEvent {
    let root = generated_events::BankEvent::create(
        builder,
        &generated_events::BankEventArgs {
            timestamp,
            payload_type,
            payload: Some(payload),
        },
    );
    generated_events::finish_size_prefixed_bank_event_buffer(builder, root);
    BankEvent::from_flatbuffer(builder.finished_data())
}

fn monotonic_clock_timestamp_ns() -> u64 {
    let mut timestamp = timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { clock_gettime(CLOCK_MONOTONIC, &mut timestamp) };
    assert_eq!(result, 0, "CLOCK_MONOTONIC clock_gettime failed");
    (timestamp.tv_sec as u64)
        .saturating_mul(1_000_000_000)
        .saturating_add(timestamp.tv_nsec as u64)
}

fn create_temp_file(
    events_dir: &Path,
    queue_name: &str,
    kind: &str,
) -> std::io::Result<(PathBuf, File)> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let pid = std::process::id();
    for attempt in 0..100 {
        let path = events_dir.join(format!(".{queue_name}.{pid}.{now}.{attempt}.{kind}.tmp"));
        match open_temp_file(&path) {
            Ok(file) => return Ok((path, file)),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err),
        }
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!("failed to allocate temporary event file for {queue_name}"),
    ))
}

fn open_temp_file(path: &Path) -> std::io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(path)
}

fn remove_stale_temp_files(events_dir: &Path, queue_name: &str) -> std::io::Result<()> {
    let prefix = format!(".{queue_name}.");
    for entry in fs::read_dir(events_dir)? {
        let path = entry?.path();
        let Some(file_name) = path.file_name().and_then(OsStr::to_str) else {
            continue;
        };
        if file_name.starts_with(&prefix) && file_name.ends_with(".tmp") {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_runtime::{bank::Bank, genesis_utils::create_genesis_config},
        std::fs,
    };

    #[test]
    fn constructs_bank_events() {
        let genesis_config = create_genesis_config(1).genesis_config;
        let bank = Bank::new_for_tests(&genesis_config);

        let before_new_bank = monotonic_clock_timestamp_ns();
        let new_bank = new_bank_event(&bank);
        let after_new_bank = monotonic_clock_timestamp_ns();
        assert!(new_bank.flatbuffer().len() <= BANK_EVENT_QUEUE_PAYLOAD_SIZE);
        let new_bank =
            generated_events::size_prefixed_root_as_bank_event(new_bank.flatbuffer()).unwrap();
        assert!(new_bank.timestamp() >= before_new_bank);
        assert!(new_bank.timestamp() <= after_new_bank);
        assert_eq!(new_bank.payload_type(), BankEventPayload::NewBankEvent);
        let new_bank_payload = new_bank.payload_as_new_bank_event().unwrap();
        assert_eq!(new_bank_payload.slot(), bank.slot());
        assert_eq!(new_bank_payload.parent_slot(), bank.parent_slot());
        assert_eq!(
            <[u8; 32]>::from(new_bank_payload.parent_hash().unwrap().bytes()),
            bank.parent_hash().to_bytes()
        );
        assert!(new_bank.payload_as_frozen_bank_event().is_none());

        bank.freeze();
        let before_frozen_bank = monotonic_clock_timestamp_ns();
        let frozen_bank = frozen_bank_event(&bank);
        let after_frozen_bank = monotonic_clock_timestamp_ns();
        assert!(frozen_bank.flatbuffer().len() <= BANK_EVENT_QUEUE_PAYLOAD_SIZE);
        let frozen_bank =
            generated_events::size_prefixed_root_as_bank_event(frozen_bank.flatbuffer()).unwrap();
        assert!(frozen_bank.timestamp() >= before_frozen_bank);
        assert!(frozen_bank.timestamp() <= after_frozen_bank);
        assert_eq!(
            frozen_bank.payload_type(),
            BankEventPayload::FrozenBankEvent
        );
        assert!(frozen_bank.payload_as_new_bank_event().is_none());
        let frozen_bank_payload = frozen_bank.payload_as_frozen_bank_event().unwrap();
        assert_eq!(frozen_bank_payload.slot(), bank.slot());
        assert_eq!(
            <[u8; 32]>::from(frozen_bank_payload.bank_hash().unwrap().bytes()),
            bank.hash().to_bytes()
        );
    }

    #[test]
    fn creates_event_queue_and_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let events_dir = temp_dir.path();

        let _producer =
            create_event_queue::<BankEvent>(events_dir, "bank_events", BANK_EVENTS_SCHEMA, 8, 1, 4)
                .unwrap();

        assert!(event_queue_path(events_dir, "bank_events").is_file());
        assert_eq!(
            fs::read(event_schema_path(events_dir, "bank_events")).unwrap(),
            BANK_EVENTS_SCHEMA
        );
    }

    #[test]
    fn replaces_queue_without_mutating_existing_mapping() {
        let temp_dir = tempfile::tempdir().unwrap();
        let events_dir = temp_dir.path();

        let mut old_producer =
            create_event_queue::<BankEvent>(events_dir, "bank_events", BANK_EVENTS_SCHEMA, 8, 1, 1)
                .unwrap();
        let mut old_consumer = old_producer.join_as_consumer().unwrap();
        let old_event = BankEvent::default();
        old_producer.try_write(old_event).unwrap();

        let _new_producer =
            create_event_queue::<BankEvent>(events_dir, "bank_events", BANK_EVENTS_SCHEMA, 8, 1, 1)
                .unwrap();

        assert_eq!(old_consumer.try_read(), Some(old_event));
    }

    #[test]
    fn removes_stale_temp_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let events_dir = temp_dir.path();
        let stale_queue = events_dir.join(".bank_events.stale.queue.tmp");
        let stale_schema = events_dir.join(".bank_events.stale.bfbs.tmp");
        fs::write(&stale_queue, b"stale").unwrap();
        fs::write(&stale_schema, b"stale").unwrap();

        let _producer =
            create_event_queue::<BankEvent>(events_dir, "bank_events", BANK_EVENTS_SCHEMA, 8, 1, 1)
                .unwrap();

        assert!(!stale_queue.exists());
        assert!(!stale_schema.exists());
    }

    #[cfg(unix)]
    #[test]
    fn accepts_symlinked_events_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let real_events_dir = temp_dir.path().join("real-events");
        let events_dir = temp_dir.path().join("events");
        fs::create_dir(&real_events_dir).unwrap();
        std::os::unix::fs::symlink(&real_events_dir, &events_dir).unwrap();

        let _producer = create_event_queue::<BankEvent>(
            &events_dir,
            "bank_events",
            BANK_EVENTS_SCHEMA,
            8,
            1,
            1,
        )
        .unwrap();

        assert!(real_events_dir.join("bank_events").is_file());
        assert!(real_events_dir.join("bank_events.bfbs").is_file());
    }
}
