use {
    serde_json::{Map, Number, Value as JsonValue},
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
    wincode::{ReadError, ReadResult},
    wincode_dynamic::{
        Decoder, Field, PrimitiveTy, PrimitiveValue, RootSchema, Value as DynamicValue,
    },
};

const DEFAULT_QUEUE_NAME: &str = "bank_events";
const SCHEMA_FILE_EXTENSION: &str = "wds";

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

impl From<ReadError> for AppError {
    fn from(err: ReadError) -> Self {
        Self::InvalidPayload(err.to_string())
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
  --object <NAME>       only print records with this wincode-dynamic variant name
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

struct PreparedDecoder {
    schema: RootSchema,
    decoder: Decoder,
}

fn prepare_schema(schema_bytes: &[u8]) -> AppResult<PreparedDecoder> {
    let schema: RootSchema = wincode::deserialize_exact(schema_bytes)
        .map_err(|err| AppError::InvalidSchema(format!("failed to decode schema: {err}")))?;
    let decoder = Decoder::new(schema.clone());
    Ok(PreparedDecoder { schema, decoder })
}

fn record_to_json<'meta, 'data>(
    record_name: &str,
    fields: impl IntoIterator<Item = ReadResult<Field<'meta, 'data>>>,
) -> AppResult<JsonValue> {
    let mut field_values = Map::new();

    for field in fields {
        let field = field.map_err(|err| AppError::InvalidPayload(err.to_string()))?;
        field_values.insert(field.name().to_string(), value_to_json(field.value())?);
    }

    let mut event = Map::new();
    event.insert(record_name.to_string(), JsonValue::Object(field_values));
    Ok(JsonValue::Object(event))
}

fn value_to_json(value: &DynamicValue<'_>) -> AppResult<JsonValue> {
    match value {
        DynamicValue::U8(value) => Ok(unsigned_value(u64::from(*value))),
        DynamicValue::U16(value) => Ok(unsigned_value(u64::from(*value))),
        DynamicValue::U32(value) => Ok(unsigned_value(u64::from(*value))),
        DynamicValue::U64(value) => Ok(unsigned_value(*value)),
        DynamicValue::I8(value) => Ok(integer_value(i64::from(*value))),
        DynamicValue::I16(value) => Ok(integer_value(i64::from(*value))),
        DynamicValue::I32(value) => Ok(integer_value(i64::from(*value))),
        DynamicValue::I64(value) => Ok(integer_value(*value)),
        DynamicValue::F32(value) => float_value(f64::from(*value)),
        DynamicValue::F64(value) => float_value(*value),
        DynamicValue::Bool(value) => Ok(JsonValue::Bool(*value)),
        DynamicValue::String(value) => Ok(JsonValue::String(value.to_string())),
        DynamicValue::Bytes(bytes) => Ok(bytes_to_json(bytes)),
        DynamicValue::Vec(values) => primitive_values_to_json(
            values
                .clone()
                .into_dyn_vec()
                .map_err(|err| AppError::InvalidPayload(err.to_string()))?,
        ),
    }
}

fn primitive_values_to_json(values: Vec<PrimitiveValue>) -> AppResult<JsonValue> {
    values
        .into_iter()
        .map(primitive_value_to_json)
        .collect::<AppResult<Vec<_>>>()
        .map(JsonValue::Array)
}

fn primitive_value_to_json(value: PrimitiveValue) -> AppResult<JsonValue> {
    match value {
        PrimitiveValue::U8(value) => Ok(unsigned_value(u64::from(value))),
        PrimitiveValue::U16(value) => Ok(unsigned_value(u64::from(value))),
        PrimitiveValue::U32(value) => Ok(unsigned_value(u64::from(value))),
        PrimitiveValue::U64(value) => Ok(unsigned_value(value)),
        PrimitiveValue::I8(value) => Ok(integer_value(i64::from(value))),
        PrimitiveValue::I16(value) => Ok(integer_value(i64::from(value))),
        PrimitiveValue::I32(value) => Ok(integer_value(i64::from(value))),
        PrimitiveValue::I64(value) => Ok(integer_value(value)),
        PrimitiveValue::F32(value) => float_value(f64::from(value)),
        PrimitiveValue::F64(value) => float_value(value),
        PrimitiveValue::Bool(value) => Ok(JsonValue::Bool(value)),
    }
}

fn unsigned_value(value: u64) -> JsonValue {
    JsonValue::Number(Number::from(value))
}

fn integer_value(value: i64) -> JsonValue {
    JsonValue::Number(Number::from(value))
}

fn float_value(value: f64) -> AppResult<JsonValue> {
    Number::from_f64(value)
        .map(JsonValue::Number)
        .ok_or_else(|| AppError::InvalidPayload(format!("non-finite float value {value}")))
}

fn bytes_to_json(bytes: &[u8]) -> JsonValue {
    JsonValue::Array(
        bytes
            .iter()
            .map(|byte| JsonValue::Number(Number::from(u64::from(*byte))))
            .collect(),
    )
}

fn record_name_matches(record_name: &str, requested_name: Option<&str>) -> bool {
    let Some(requested_name) = requested_name else {
        return true;
    };
    record_name == requested_name || record_name.rsplit('.').next() == Some(requested_name)
}

fn record_name<'schema>(schema: &'schema RootSchema, payload: &[u8]) -> AppResult<&'schema str> {
    match schema {
        RootSchema::Struct(schema) => Ok(schema.name()),
        RootSchema::Enum {
            variants,
            tag_encoding,
            ..
        } => {
            let tag = read_tag(*tag_encoding, payload)?;
            variants
                .get(tag)
                .map(|schema| schema.name())
                .ok_or_else(|| AppError::InvalidPayload(format!("invalid event tag {tag}")))
        }
    }
}

fn read_tag(tag_encoding: PrimitiveTy, payload: &[u8]) -> AppResult<usize> {
    match tag_encoding {
        PrimitiveTy::U8 => Ok(usize::from(read_value::<u8>(payload)?)),
        PrimitiveTy::U16 => Ok(usize::from(read_value::<u16>(payload)?)),
        PrimitiveTy::U32 => usize::try_from(read_value::<u32>(payload)?)
            .map_err(|_| AppError::InvalidPayload("event tag does not fit usize".to_string())),
        PrimitiveTy::U64 => usize::try_from(read_value::<u64>(payload)?)
            .map_err(|_| AppError::InvalidPayload("event tag does not fit usize".to_string())),
        PrimitiveTy::I8 => signed_tag(read_value::<i8>(payload)?),
        PrimitiveTy::I16 => signed_tag(read_value::<i16>(payload)?),
        PrimitiveTy::I32 => signed_tag(read_value::<i32>(payload)?),
        PrimitiveTy::I64 => signed_tag(read_value::<i64>(payload)?),
        PrimitiveTy::F32 | PrimitiveTy::F64 | PrimitiveTy::Bool => Err(AppError::InvalidSchema(
            "event tag encoding must be an integer".to_string(),
        )),
    }
}

fn signed_tag(value: impl TryInto<usize>) -> AppResult<usize> {
    value
        .try_into()
        .map_err(|_| AppError::InvalidPayload("event tag is negative or too large".to_string()))
}

fn read_value<'de, T>(payload: &'de [u8]) -> Result<T, ReadError>
where
    T: wincode::SchemaRead<'de, wincode::config::DefaultConfig, Dst = T>,
{
    wincode::deserialize(payload)
}

fn run() -> AppResult<()> {
    let args = Args::parse()?;
    let shutdown_requested = install_shutdown_signal_handlers()?;

    let queue_path = args.events_dir.join(&args.queue_name);
    let schema_path = event_schema_path(&args.events_dir, &args.queue_name);
    let schema_bytes = fs::read(&schema_path)?;
    let prepared = prepare_schema(&schema_bytes)?;

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
        "queue={} schema={} root={} payload_size={} consumer_index={}",
        queue_path.display(),
        schema_path.display(),
        prepared.decoder.name(),
        consumer.payload_size(),
        consumer.index()
    );

    while !shutdown_requested.load(Ordering::Relaxed) {
        match consumer.read_timeout(args.poll_timeout) {
            Ok(payload) => {
                let payload = payload.as_slice();
                let record_name = record_name(&prepared.schema, payload)?;
                if record_name_matches(record_name, args.object_name.as_deref()) {
                    let fields = prepared
                        .decoder
                        .fields(payload)
                        .map_err(|err| AppError::InvalidPayload(err.to_string()))?;
                    println!(
                        "{}",
                        serde_json::to_string(&record_to_json(record_name, fields)?)?
                    );
                }
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
