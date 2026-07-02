use {
    flatrecord::{DynamicRecord, PreparedSchema, RootDef, Schema, ValueRef},
    serde_json::{Map, Number, Value},
    shaq::{broadcast::SliceConsumer, error::WaitError},
    signal_hook::{
        consts::signal::{SIGINT, SIGTERM},
        flag as signal_flag,
    },
    std::{
        env,
        error::Error,
        fmt::{Display, Formatter},
        fs::{self, OpenOptions},
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
};

const DEFAULT_QUEUE_NAME: &str = "bank_events";
const SCHEMA_FILE_EXTENSION: &str = "frs";

type AppResult<T> = Result<T, AppError>;

#[derive(Debug)]
enum AppError {
    Help(String),
    Usage(String),
    Io(std::io::Error),
    Json(serde_json::Error),
    Queue(shaq::error::Error),
    InvalidSchema(String),
    InvalidPayload(String),
}

impl Display for AppError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Help(message) => write!(formatter, "{message}"),
            Self::Usage(message) => write!(formatter, "{message}"),
            Self::Io(err) => write!(formatter, "{err}"),
            Self::Json(err) => write!(formatter, "{err}"),
            Self::Queue(err) => write!(formatter, "{err}"),
            Self::InvalidSchema(message) => write!(formatter, "invalid schema: {message}"),
            Self::InvalidPayload(message) => write!(formatter, "invalid payload: {message}"),
        }
    }
}

impl Error for AppError {}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err)
    }
}

impl From<serde_json::Error> for AppError {
    fn from(err: serde_json::Error) -> Self {
        Self::Json(err)
    }
}

impl From<shaq::error::Error> for AppError {
    fn from(err: shaq::error::Error) -> Self {
        Self::Queue(err)
    }
}

#[derive(Debug)]
struct Args {
    events_dir: PathBuf,
    queue_name: String,
    object_name: Option<String>,
    from_backlog: bool,
    once: bool,
    poll_timeout: Duration,
}

impl Args {
    fn parse() -> AppResult<Self> {
        let mut ledger_dir = None;
        let mut events_dir = None;
        let mut queue_name = DEFAULT_QUEUE_NAME.to_string();
        let mut object_name = None;
        let mut from_backlog = false;
        let mut once = false;
        let mut poll_ms = 1_000;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--help" | "-h" => return Err(AppError::Help(usage())),
                "--ledger-dir" => ledger_dir = Some(next_path(&mut args, "--ledger-dir")?),
                "--events-dir" => events_dir = Some(next_path(&mut args, "--events-dir")?),
                "--queue-name" => queue_name = next_value(&mut args, "--queue-name")?,
                "--object" => object_name = Some(next_value(&mut args, "--object")?),
                "--from-backlog" => from_backlog = true,
                "--once" => once = true,
                "--poll-ms" => {
                    poll_ms = next_value(&mut args, "--poll-ms")?.parse().map_err(|err| {
                        AppError::Usage(format!("invalid --poll-ms value: {err}"))
                    })?;
                }
                unknown => {
                    return Err(AppError::Usage(format!(
                        "unknown argument: {unknown}\n\n{}",
                        usage()
                    )));
                }
            }
        }

        let events_dir = match (events_dir, ledger_dir) {
            (Some(events_dir), _) => events_dir,
            (None, Some(ledger_dir)) => ledger_dir.join("events"),
            (None, None) => {
                return Err(AppError::Usage(format!(
                    "either --events-dir or --ledger-dir is required\n\n{}",
                    usage()
                )));
            }
        };

        Ok(Self {
            events_dir,
            queue_name,
            object_name,
            from_backlog,
            once,
            poll_timeout: Duration::from_millis(poll_ms),
        })
    }
}

fn usage() -> String {
    format!(
        "\
Usage:
  bank-event-consumer --ledger-dir <LEDGER_DIR> [options]
  bank-event-consumer --events-dir <EVENTS_DIR> [options]

Options:
  --queue-name <NAME>   queue and schema name [default: {DEFAULT_QUEUE_NAME}]
  --object <NAME>       only print records with this flatrecord record name
  --from-backlog        start up to one ring behind the current queue frontier
  --once                drain currently available events, then exit
  --poll-ms <MS>        read timeout while waiting for events [default: 1000]
"
    )
}

fn next_value(args: &mut impl Iterator<Item = String>, name: &str) -> AppResult<String> {
    args.next()
        .ok_or_else(|| AppError::Usage(format!("missing value for {name}")))
}

fn next_path(args: &mut impl Iterator<Item = String>, name: &str) -> AppResult<PathBuf> {
    Ok(PathBuf::from(next_value(args, name)?))
}

fn install_shutdown_signal_handlers() -> AppResult<Arc<AtomicBool>> {
    let shutdown_requested = Arc::new(AtomicBool::new(false));
    signal_flag::register(SIGINT, Arc::clone(&shutdown_requested))?;
    signal_flag::register(SIGTERM, Arc::clone(&shutdown_requested))?;
    Ok(shutdown_requested)
}

fn event_schema_path(events_dir: &Path, queue_name: &str) -> PathBuf {
    events_dir.join(format!("{queue_name}.{SCHEMA_FILE_EXTENSION}"))
}

fn prepare_schema(schema_bytes: &[u8]) -> AppResult<PreparedSchema> {
    let schema: Schema = wincode::deserialize_exact(schema_bytes)
        .map_err(|err| AppError::InvalidSchema(format!("failed to decode schema: {err}")))?;
    PreparedSchema::new(schema)
        .map_err(|err| AppError::InvalidSchema(format!("failed to prepare schema: {err}")))
}

fn schema_root_name(schema: &Schema) -> &str {
    match schema.root() {
        RootDef::Struct => schema
            .records()
            .first()
            .map(|record| record.name())
            .unwrap_or("<empty>"),
        RootDef::TaggedUnion { name } => name,
    }
}

fn record_to_json(record: &DynamicRecord<'_, '_>) -> AppResult<Value> {
    let mut fields = Map::new();

    for field in record.fields() {
        let value = field
            .value()
            .map_err(|err| AppError::InvalidPayload(err.to_string()))?;
        fields.insert(field.name().to_string(), value_to_json(value)?);
    }

    let mut event = Map::new();
    event.insert(record.record_name().to_string(), Value::Object(fields));
    Ok(Value::Object(event))
}

fn value_to_json(value: ValueRef<'_>) -> AppResult<Value> {
    match value {
        ValueRef::U8(value) => Ok(unsigned_value(u64::from(value))),
        ValueRef::U16(value) => Ok(unsigned_value(u64::from(value))),
        ValueRef::U32(value) => Ok(unsigned_value(u64::from(value))),
        ValueRef::U64(value) => Ok(unsigned_value(value)),
        ValueRef::I8(value) => Ok(integer_value(i64::from(value))),
        ValueRef::I16(value) => Ok(integer_value(i64::from(value))),
        ValueRef::I32(value) => Ok(integer_value(i64::from(value))),
        ValueRef::I64(value) => Ok(integer_value(value)),
        ValueRef::F32(value) => float_value(f64::from(value)),
        ValueRef::F64(value) => float_value(value),
        ValueRef::Bool(value) => Ok(Value::Bool(value)),
        ValueRef::Bytes(bytes) | ValueRef::ArrayBytes(bytes) => Ok(bytes_to_json(bytes)),
        ValueRef::Str(value) => Ok(Value::String(value.to_string())),
    }
}

fn unsigned_value(value: u64) -> Value {
    Value::Number(Number::from(value))
}

fn integer_value(value: i64) -> Value {
    Value::Number(Number::from(value))
}

fn float_value(value: f64) -> AppResult<Value> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| AppError::InvalidPayload(format!("non-finite float value {value}")))
}

fn bytes_to_json(bytes: &[u8]) -> Value {
    Value::Array(
        bytes
            .iter()
            .map(|byte| Value::Number(Number::from(u64::from(*byte))))
            .collect(),
    )
}

fn record_name_matches(record_name: &str, requested_name: Option<&str>) -> bool {
    let Some(requested_name) = requested_name else {
        return true;
    };
    record_name == requested_name || record_name.rsplit('.').next() == Some(requested_name)
}

fn run() -> AppResult<()> {
    let args = Args::parse()?;
    let shutdown_requested = install_shutdown_signal_handlers()?;

    let queue_path = args.events_dir.join(&args.queue_name);
    let schema_path = event_schema_path(&args.events_dir, &args.queue_name);
    let schema_bytes = fs::read(&schema_path)?;
    let prepared_schema = prepare_schema(&schema_bytes)?;

    let queue_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&queue_path)?;
    let mut consumer = if args.from_backlog {
        unsafe { SliceConsumer::join_from_backlog(&queue_file) }?
    } else {
        unsafe { SliceConsumer::join(&queue_file) }?
    };

    eprintln!(
        "queue={} schema={} schema_version={} root={} records={} payload_size={} consumer_index={}",
        queue_path.display(),
        schema_path.display(),
        prepared_schema.schema().schema_version(),
        schema_root_name(prepared_schema.schema()),
        prepared_schema.schema().records().len(),
        consumer.payload_size(),
        consumer.index()
    );

    while !shutdown_requested.load(Ordering::Relaxed) {
        match consumer.read_timeout(args.poll_timeout) {
            Ok(payload) => {
                let record = DynamicRecord::read(&prepared_schema, payload.as_slice())
                    .map_err(|err| AppError::InvalidPayload(err.to_string()))?;
                if record_name_matches(record.record_name(), args.object_name.as_deref()) {
                    println!("{}", serde_json::to_string(&record_to_json(&record)?)?);
                }
                drop(payload);
            }
            Err(WaitError::Timeout) if args.once => return Ok(()),
            Err(WaitError::Timeout) => {}
        }
    }

    eprintln!("shutdown requested; dropping consumer");
    drop(consumer);
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        match err {
            AppError::Help(message) => println!("{message}"),
            err => {
                eprintln!("{err}");
                std::process::exit(1);
            }
        }
    }
}
