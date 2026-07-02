use {
    flatrecord::FlatRecord,
    libc::{CLOCK_MONOTONIC, clock_gettime, timespec},
    shaq::broadcast::{BroadcastConfig, Producer},
    solana_runtime::bank::Bank,
    std::{
        ffi::OsStr,
        fs::{self, File, OpenOptions},
        io::Write,
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
};

const SCHEMA_FILE_EXTENSION: &str = "frs";

#[derive(Clone, Copy, Debug, Eq, PartialEq, FlatRecord)]
pub struct NewBankEvent {
    pub timestamp: u64,
    pub slot: u64,
    pub parent_slot: u64,
    pub parent_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, FlatRecord)]
pub struct FrozenBankEvent {
    pub timestamp: u64,
    pub slot: u64,
    pub bank_hash: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, FlatRecord)]
#[schema(version = 1)]
pub enum Event {
    NewBankEvent(NewBankEvent),
    FrozenBankEvent(FrozenBankEvent),
}

impl Event {
    #[allow(non_upper_case_globals)]
    pub const size: usize = 58;
}

pub type BankEvent = [u8; Event::size];
pub type BankEventProducer = Producer<BankEvent>;

pub fn bank_events_schema() -> Vec<u8> {
    wincode::serialize(&Event::schema()).expect("generated flatrecord schema should serialize")
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

    let (schema_tmp_path, mut schema_file) =
        create_temp_file(events_dir, queue_name, SCHEMA_FILE_EXTENSION)?;
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
    events_dir.join(format!("{queue_name}.{SCHEMA_FILE_EXTENSION}"))
}

pub fn new_bank_event(bank: &Bank) -> BankEvent {
    encode_event(&Event::NewBankEvent(NewBankEvent {
        timestamp: monotonic_clock_timestamp_ns(),
        slot: bank.slot(),
        parent_slot: bank.parent_slot(),
        parent_hash: bank.parent_hash().to_bytes(),
    }))
}

pub fn frozen_bank_event(bank: &Bank) -> BankEvent {
    encode_event(&Event::FrozenBankEvent(FrozenBankEvent {
        timestamp: monotonic_clock_timestamp_ns(),
        slot: bank.slot(),
        bank_hash: bank.hash().to_bytes(),
    }))
}

fn encode_event(event: &Event) -> BankEvent {
    let mut bytes = [0; Event::size];
    let written = event
        .write_record(&mut bytes)
        .expect("bank event should fit in the fixed queue payload");
    debug_assert!(written <= Event::size);
    bytes
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
        std::{fs, mem::size_of},
    };

    #[test]
    fn constructs_bank_events() {
        let genesis_config = create_genesis_config(1).genesis_config;
        let bank = Bank::new_for_tests(&genesis_config);

        assert_eq!(size_of::<BankEvent>(), Event::size);

        let before_new_bank = monotonic_clock_timestamp_ns();
        let new_bank = new_bank_event(&bank);
        let after_new_bank = monotonic_clock_timestamp_ns();
        let new_bank = Event::from_record_bytes(&new_bank).unwrap();
        let Event::NewBankEvent(new_bank) = new_bank else {
            panic!("expected new bank event");
        };
        assert!(new_bank.timestamp >= before_new_bank);
        assert!(new_bank.timestamp <= after_new_bank);
        assert_eq!(new_bank.slot, bank.slot());
        assert_eq!(new_bank.parent_slot, bank.parent_slot());
        assert_eq!(new_bank.parent_hash, bank.parent_hash().to_bytes());

        bank.freeze();
        let before_frozen_bank = monotonic_clock_timestamp_ns();
        let frozen_bank = frozen_bank_event(&bank);
        let after_frozen_bank = monotonic_clock_timestamp_ns();
        let frozen_bank = Event::from_record_bytes(&frozen_bank).unwrap();
        let Event::FrozenBankEvent(frozen_bank) = frozen_bank else {
            panic!("expected frozen bank event");
        };
        assert!(frozen_bank.timestamp >= before_frozen_bank);
        assert!(frozen_bank.timestamp <= after_frozen_bank);
        assert_eq!(frozen_bank.slot, bank.slot());
        assert_eq!(frozen_bank.bank_hash, bank.hash().to_bytes());
    }

    #[test]
    fn creates_event_queue_and_schema() {
        let temp_dir = tempfile::tempdir().unwrap();
        let events_dir = temp_dir.path();
        let schema = bank_events_schema();

        let _producer =
            create_event_queue::<BankEvent>(events_dir, "bank_events", &schema, 8, 1, 4).unwrap();

        assert!(event_queue_path(events_dir, "bank_events").is_file());
        assert_eq!(
            fs::read(event_schema_path(events_dir, "bank_events")).unwrap(),
            schema
        );

        let decoded: flatrecord::Schema = wincode::deserialize_exact(&schema).unwrap();
        assert_eq!(decoded, Event::schema());
    }

    #[test]
    fn replaces_queue_without_mutating_existing_mapping() {
        let temp_dir = tempfile::tempdir().unwrap();
        let events_dir = temp_dir.path();
        let schema = bank_events_schema();

        let mut old_producer =
            create_event_queue::<BankEvent>(events_dir, "bank_events", &schema, 8, 1, 1).unwrap();
        let mut old_consumer = old_producer.join_as_consumer().unwrap();
        let old_event = [0; Event::size];
        old_producer.try_write(old_event).unwrap();

        let _new_producer =
            create_event_queue::<BankEvent>(events_dir, "bank_events", &schema, 8, 1, 1).unwrap();

        assert_eq!(old_consumer.try_read(), Some(old_event));
    }

    #[test]
    fn removes_stale_temp_files() {
        let temp_dir = tempfile::tempdir().unwrap();
        let events_dir = temp_dir.path();
        let stale_queue = events_dir.join(".bank_events.stale.queue.tmp");
        let stale_schema = events_dir.join(".bank_events.stale.frs.tmp");
        let schema = bank_events_schema();
        fs::write(&stale_queue, b"stale").unwrap();
        fs::write(&stale_schema, b"stale").unwrap();

        let _producer =
            create_event_queue::<BankEvent>(events_dir, "bank_events", &schema, 8, 1, 1).unwrap();

        assert!(!stale_queue.exists());
        assert!(!stale_schema.exists());
    }

    #[cfg(unix)]
    #[test]
    fn accepts_symlinked_events_dir() {
        let temp_dir = tempfile::tempdir().unwrap();
        let real_events_dir = temp_dir.path().join("real-events");
        let events_dir = temp_dir.path().join("events");
        let schema = bank_events_schema();
        fs::create_dir(&real_events_dir).unwrap();
        std::os::unix::fs::symlink(&real_events_dir, &events_dir).unwrap();

        let _producer =
            create_event_queue::<BankEvent>(&events_dir, "bank_events", &schema, 8, 1, 1).unwrap();

        assert!(real_events_dir.join("bank_events").is_file());
        assert!(real_events_dir.join("bank_events.frs").is_file());
    }
}
