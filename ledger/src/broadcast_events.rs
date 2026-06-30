pub use generated::agave::ledger::broadcast_events::{
    BankEvent, BankEventKind, FrozenBankEvent, NewBankEvent,
};
use {
    generated::agave::ledger::broadcast_events as generated_events,
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

pub const BANK_EVENTS_SCHEMA: &[u8] = include_bytes!("broadcast_events/schemas/bank_events.bfbs");
pub type BankEventProducer = Producer<BankEvent>;

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
    let parent_hash = generated_events::Hash::new(&bank.parent_hash().to_bytes());
    let new_bank = NewBankEvent::new(bank.slot(), bank.parent_slot(), &parent_hash);
    BankEvent::new(
        BankEventKind::NewBank,
        &new_bank,
        &FrozenBankEvent::default(),
    )
}

pub fn frozen_bank_event(bank: &Bank) -> BankEvent {
    let bank_hash = generated_events::Hash::new(&bank.hash().to_bytes());
    let frozen_bank = FrozenBankEvent::new(bank.slot(), &bank_hash);
    BankEvent::new(
        BankEventKind::FrozenBank,
        &NewBankEvent::default(),
        &frozen_bank,
    )
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

        let new_bank = new_bank_event(&bank);
        assert_eq!(new_bank.kind(), BankEventKind::NewBank);
        assert_eq!(new_bank.new_bank().slot(), bank.slot());
        assert_eq!(new_bank.new_bank().parent_slot(), bank.parent_slot());

        bank.freeze();
        let frozen_bank = frozen_bank_event(&bank);
        assert_eq!(frozen_bank.kind(), BankEventKind::FrozenBank);
        assert_eq!(frozen_bank.frozen_bank().slot(), bank.slot());
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
