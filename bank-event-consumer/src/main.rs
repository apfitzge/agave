use {
    flatbuffers::{Follow, ForwardsUOffset, Table},
    flatbuffers_reflection::{
        get_any_root,
        reflection::{
            BaseType, Enum as ReflectionEnum, EnumVal, Field, Object, Schema, Type,
            root_as_schema as root_as_reflection_schema,
        },
    },
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
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
};

const DEFAULT_QUEUE_NAME: &str = "bank_events";
const FLATBUFFER_SIZE_PREFIX_SIZE: usize = 4;

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
  --object <NAME>       reflected table name to decode
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

fn select_root_object<'schema>(
    schema: &Schema<'schema>,
    requested_object: Option<&str>,
) -> AppResult<Object<'schema>> {
    if let Some(name) = requested_object {
        let object = find_object(schema, name)?;
        return Ok(object);
    }

    if let Some(object) = schema.root_table() {
        return Ok(object);
    }

    Err(AppError::InvalidSchema(
        "schema has no root table; pass --object".to_string(),
    ))
}

fn decode_table<'schema>(
    schema: &Schema<'schema>,
    object: Object<'schema>,
    table: Table<'_>,
) -> AppResult<Value> {
    if object.is_struct() {
        return Err(AppError::InvalidSchema(format!(
            "{} is a struct; root table decoding expected a table",
            object.name()
        )));
    }

    let mut map = Map::new();
    let fields = object.fields();
    for index in 0..fields.len() {
        let field = fields.get(index);
        if field.deprecated() {
            continue;
        }

        let field_offset = table.vtable().get(field.offset()) as usize;
        if field_offset == 0 {
            continue;
        }

        let field_loc = table
            .loc()
            .checked_add(field_offset)
            .ok_or_else(|| AppError::InvalidPayload("field offset overflow".to_string()))?;
        map.insert(
            field.name().to_string(),
            decode_table_field(schema, object, field, table, field_loc)?,
        );
    }

    Ok(Value::Object(map))
}

fn decode_table_field(
    schema: &Schema<'_>,
    object: Object<'_>,
    field: Field<'_>,
    table: Table<'_>,
    loc: usize,
) -> AppResult<Value> {
    let type_ = field.type_();
    let payload = table.buf();
    match type_.base_type() {
        BaseType::Obj => {
            let object = object_for_type(schema, type_)?;
            if object.is_struct() {
                decode_struct(schema, object, payload, loc)
            } else {
                let table = unsafe { ForwardsUOffset::<Table>::follow(payload, loc) };
                decode_table(schema, object, table)
            }
        }
        BaseType::String => {
            let value = unsafe { ForwardsUOffset::<&str>::follow(payload, loc) };
            Ok(Value::String(value.to_string()))
        }
        BaseType::Union => decode_union(schema, object, field, table, loc),
        BaseType::Vector | BaseType::Vector64 => Err(AppError::InvalidSchema(format!(
            "unsupported table field type {}",
            base_type_name(type_.base_type())
        ))),
        BaseType::Array => Err(AppError::InvalidSchema(
            "arrays are only supported inside structs".to_string(),
        )),
        base_type => decode_scalar(schema, base_type, type_.index(), payload, loc),
    }
}

fn decode_union(
    schema: &Schema<'_>,
    object: Object<'_>,
    field: Field<'_>,
    table: Table<'_>,
    loc: usize,
) -> AppResult<Value> {
    let union_enum = enum_for_index(schema, field.type_().index())?;
    if !union_enum.is_union() {
        return Err(AppError::InvalidSchema(format!(
            "{} is not a union enum",
            union_enum.name()
        )));
    }

    let union_type_field = find_field(object, &format!("{}_type", field.name()))?;
    let union_type_value = read_table_u8_field(table, union_type_field)?;
    if union_type_value == 0 {
        return Ok(Value::Null);
    }

    let union_enum_value =
        find_enum_value(union_enum, i64::from(union_type_value)).ok_or_else(|| {
            AppError::InvalidPayload(format!(
                "union {} has unknown variant value {}",
                union_enum.name(),
                union_type_value
            ))
        })?;
    let union_type = union_enum_value.union_type().ok_or_else(|| {
        AppError::InvalidSchema(format!(
            "union variant {} has no reflected payload type",
            union_enum_value.name()
        ))
    })?;

    match union_type.base_type() {
        BaseType::Obj => {
            let union_object = object_for_type(schema, union_type)?;
            let value_loc = forwards_uoffset_target(table.buf(), loc)?;
            if union_object.is_struct() {
                decode_struct(schema, union_object, table.buf(), value_loc)
            } else {
                let union_table = unsafe { Table::new(table.buf(), value_loc) };
                decode_table(schema, union_object, union_table)
            }
        }
        BaseType::String => {
            let value = unsafe { ForwardsUOffset::<&str>::follow(table.buf(), loc) };
            Ok(Value::String(value.to_string()))
        }
        unsupported => Err(AppError::InvalidSchema(format!(
            "unsupported union payload type {}",
            base_type_name(unsupported)
        ))),
    }
}

fn decode_struct(
    schema: &Schema<'_>,
    object: Object<'_>,
    payload: &[u8],
    loc: usize,
) -> AppResult<Value> {
    let size = object_size(object)?;
    read_slice(payload, loc, size)?;

    let mut map = Map::new();
    let fields = object.fields();
    for index in 0..fields.len() {
        let field = fields.get(index);
        if field.deprecated() {
            continue;
        }

        let field_loc = loc
            .checked_add(usize::from(field.offset()))
            .ok_or_else(|| AppError::InvalidPayload("field offset overflow".to_string()))?;
        map.insert(
            field.name().to_string(),
            decode_type(schema, field.type_(), payload, field_loc)?,
        );
    }

    Ok(Value::Object(map))
}

fn decode_type(
    schema: &Schema<'_>,
    type_: Type<'_>,
    payload: &[u8],
    loc: usize,
) -> AppResult<Value> {
    match type_.base_type() {
        BaseType::Obj => {
            let object = object_for_type(schema, type_)?;
            if !object.is_struct() {
                return Err(AppError::InvalidSchema(format!(
                    "{} is a table; raw queue payload decoding only supports structs",
                    object.name()
                )));
            }
            decode_struct(schema, object, payload, loc)
        }
        BaseType::Array => decode_array(schema, type_, payload, loc),
        base_type => decode_scalar(schema, base_type, type_.index(), payload, loc),
    }
}

fn decode_array(
    schema: &Schema<'_>,
    type_: Type<'_>,
    payload: &[u8],
    loc: usize,
) -> AppResult<Value> {
    let len = usize::from(type_.fixed_length());
    let stride = array_stride(schema, type_)?;
    let mut values = Vec::with_capacity(len);

    for index in 0..len {
        let element_loc = loc
            .checked_add(index.checked_mul(stride).ok_or_else(|| {
                AppError::InvalidPayload("array element offset overflow".to_string())
            })?)
            .ok_or_else(|| AppError::InvalidPayload("array element offset overflow".to_string()))?;
        let value = match type_.element() {
            BaseType::Obj => {
                let object = object_for_type(schema, type_)?;
                if !object.is_struct() {
                    return Err(AppError::InvalidSchema(format!(
                        "{} is a table; raw queue payload decoding only supports structs",
                        object.name()
                    )));
                }
                decode_struct(schema, object, payload, element_loc)?
            }
            element_type => {
                decode_scalar(schema, element_type, type_.index(), payload, element_loc)?
            }
        };
        values.push(value);
    }

    Ok(Value::Array(values))
}

fn array_stride(schema: &Schema<'_>, type_: Type<'_>) -> AppResult<usize> {
    if type_.element_size() != 0 {
        return usize_from_u32(type_.element_size(), "array element size");
    }

    match type_.element() {
        BaseType::Obj => object_size(object_for_type(schema, type_)?),
        element_type => scalar_size(element_type),
    }
}

fn decode_scalar(
    schema: &Schema<'_>,
    base_type: BaseType,
    enum_index: i32,
    payload: &[u8],
    loc: usize,
) -> AppResult<Value> {
    match base_type {
        BaseType::Bool => Ok(Value::Bool(read_u8(payload, loc)? != 0)),
        BaseType::Byte => integer_value(schema, enum_index, i64::from(read_i8(payload, loc)?)),
        BaseType::UByte | BaseType::UType => {
            unsigned_value(schema, enum_index, u64::from(read_u8(payload, loc)?))
        }
        BaseType::Short => integer_value(schema, enum_index, i64::from(read_i16(payload, loc)?)),
        BaseType::UShort => unsigned_value(schema, enum_index, u64::from(read_u16(payload, loc)?)),
        BaseType::Int => integer_value(schema, enum_index, i64::from(read_i32(payload, loc)?)),
        BaseType::UInt => unsigned_value(schema, enum_index, u64::from(read_u32(payload, loc)?)),
        BaseType::Long => integer_value(schema, enum_index, read_i64(payload, loc)?),
        BaseType::ULong => unsigned_value(schema, enum_index, read_u64(payload, loc)?),
        BaseType::Float => float_value(f64::from(read_f32(payload, loc)?)),
        BaseType::Double => float_value(read_f64(payload, loc)?),
        unsupported => Err(AppError::InvalidSchema(format!(
            "unsupported raw struct scalar type {}",
            base_type_name(unsupported)
        ))),
    }
}

fn integer_value(schema: &Schema<'_>, enum_index: i32, value: i64) -> AppResult<Value> {
    if let Some(name) = enum_name(schema, enum_index, value) {
        return Ok(Value::String(name));
    }
    Ok(Value::Number(Number::from(value)))
}

fn unsigned_value(schema: &Schema<'_>, enum_index: i32, value: u64) -> AppResult<Value> {
    if let Ok(signed_value) = i64::try_from(value) {
        if let Some(name) = enum_name(schema, enum_index, signed_value) {
            return Ok(Value::String(name));
        }
    }
    Ok(Value::Number(Number::from(value)))
}

fn float_value(value: f64) -> AppResult<Value> {
    Number::from_f64(value)
        .map(Value::Number)
        .ok_or_else(|| AppError::InvalidPayload(format!("non-finite float value {value}")))
}

fn enum_name(schema: &Schema<'_>, enum_index: i32, value: i64) -> Option<String> {
    if enum_index < 0 {
        return None;
    }

    let enums = schema.enums();
    let enum_index = usize::try_from(enum_index).ok()?;
    if enum_index >= enums.len() {
        return None;
    }

    let enum_def = enums.get(enum_index);
    let values = enum_def.values();
    for index in 0..values.len() {
        let enum_value = values.get(index);
        if enum_value.value() == value {
            return Some(enum_value.name().to_string());
        }
    }

    None
}

fn enum_for_index<'schema>(
    schema: &Schema<'schema>,
    enum_index: i32,
) -> AppResult<ReflectionEnum<'schema>> {
    if enum_index < 0 {
        return Err(AppError::InvalidSchema(format!(
            "invalid enum index {enum_index}"
        )));
    }

    let enums = schema.enums();
    let enum_index = usize::try_from(enum_index)
        .map_err(|_| AppError::InvalidSchema(format!("invalid enum index {enum_index}")))?;
    if enum_index >= enums.len() {
        return Err(AppError::InvalidSchema(format!(
            "invalid enum index {enum_index}"
        )));
    }

    Ok(enums.get(enum_index))
}

fn find_enum_value<'schema>(
    enum_def: ReflectionEnum<'schema>,
    value: i64,
) -> Option<EnumVal<'schema>> {
    let values = enum_def.values();
    for index in 0..values.len() {
        let enum_value = values.get(index);
        if enum_value.value() == value {
            return Some(enum_value);
        }
    }

    None
}

fn find_field<'schema>(object: Object<'schema>, name: &str) -> AppResult<Field<'schema>> {
    let fields = object.fields();
    for index in 0..fields.len() {
        let field = fields.get(index);
        if field.name() == name {
            return Ok(field);
        }
    }

    Err(AppError::InvalidSchema(format!(
        "{} is missing field {name}",
        object.name()
    )))
}

fn read_table_u8_field(table: Table<'_>, field: Field<'_>) -> AppResult<u8> {
    let field_offset = table.vtable().get(field.offset()) as usize;
    if field_offset == 0 {
        return u8::try_from(field.default_integer()).map_err(|_| {
            AppError::InvalidSchema(format!(
                "{} default value {} does not fit in ubyte",
                field.name(),
                field.default_integer()
            ))
        });
    }

    let field_loc = table
        .loc()
        .checked_add(field_offset)
        .ok_or_else(|| AppError::InvalidPayload("field offset overflow".to_string()))?;
    read_u8(table.buf(), field_loc)
}

fn forwards_uoffset_target(payload: &[u8], loc: usize) -> AppResult<usize> {
    let offset = read_u32(payload, loc)? as usize;
    if offset == 0 {
        return Err(AppError::InvalidPayload(
            "union payload offset is zero".to_string(),
        ));
    }

    loc.checked_add(offset)
        .ok_or_else(|| AppError::InvalidPayload("uoffset overflow".to_string()))
}

fn find_object<'schema>(schema: &Schema<'schema>, name: &str) -> AppResult<Object<'schema>> {
    let objects = schema.objects();
    for index in 0..objects.len() {
        let object = objects.get(index);
        if object.name() == name || object.name().rsplit('.').next() == Some(name) {
            return Ok(object);
        }
    }

    Err(AppError::InvalidSchema(format!("missing object {name}")))
}

fn object_for_type<'schema>(
    schema: &Schema<'schema>,
    type_: Type<'schema>,
) -> AppResult<Object<'schema>> {
    let index = type_.index();
    if index < 0 {
        return Err(AppError::InvalidSchema(format!(
            "{} does not reference an object",
            base_type_name(type_.base_type())
        )));
    }

    let objects = schema.objects();
    let index = usize::try_from(index)
        .map_err(|_| AppError::InvalidSchema(format!("invalid object index {index}")))?;
    if index >= objects.len() {
        return Err(AppError::InvalidSchema(format!(
            "invalid object index {index}"
        )));
    }

    Ok(objects.get(index))
}

fn object_size(object: Object<'_>) -> AppResult<usize> {
    usize::try_from(object.bytesize()).map_err(|_| {
        AppError::InvalidSchema(format!(
            "{} has negative bytesize {}",
            object.name(),
            object.bytesize()
        ))
    })
}

fn scalar_size(base_type: BaseType) -> AppResult<usize> {
    match base_type {
        BaseType::Bool | BaseType::Byte | BaseType::UByte | BaseType::UType => Ok(1),
        BaseType::Short | BaseType::UShort => Ok(2),
        BaseType::Int | BaseType::UInt | BaseType::Float => Ok(4),
        BaseType::Long | BaseType::ULong | BaseType::Double => Ok(8),
        unsupported => Err(AppError::InvalidSchema(format!(
            "type {} has no fixed scalar size",
            base_type_name(unsupported)
        ))),
    }
}

fn usize_from_u32(value: u32, name: &str) -> AppResult<usize> {
    usize::try_from(value).map_err(|_| AppError::InvalidSchema(format!("{name} is too large")))
}

fn base_type_name(base_type: BaseType) -> &'static str {
    base_type.variant_name().unwrap_or("unknown")
}

fn read_slice(payload: &[u8], offset: usize, len: usize) -> AppResult<&[u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| AppError::InvalidPayload("read offset overflow".to_string()))?;
    payload.get(offset..end).ok_or_else(|| {
        AppError::InvalidPayload(format!(
            "read of {len} bytes at offset {offset} exceeds payload len {}",
            payload.len()
        ))
    })
}

fn payload_flatbuffer(payload: &[u8]) -> AppResult<&[u8]> {
    let len_bytes = read_slice(payload, 0, FLATBUFFER_SIZE_PREFIX_SIZE)?;
    let len = u32::from_le_bytes(len_bytes.try_into().expect("slice length checked")) as usize;
    let size_prefixed_flatbuffer = read_slice(payload, 0, FLATBUFFER_SIZE_PREFIX_SIZE + len)?;
    Ok(&size_prefixed_flatbuffer[FLATBUFFER_SIZE_PREFIX_SIZE..])
}

fn read_u8(payload: &[u8], offset: usize) -> AppResult<u8> {
    Ok(*read_slice(payload, offset, 1)?
        .first()
        .expect("slice length checked"))
}

fn read_i8(payload: &[u8], offset: usize) -> AppResult<i8> {
    Ok(read_u8(payload, offset)? as i8)
}

fn read_u16(payload: &[u8], offset: usize) -> AppResult<u16> {
    Ok(u16::from_le_bytes(
        read_slice(payload, offset, 2)?
            .try_into()
            .expect("slice length checked"),
    ))
}

fn read_i16(payload: &[u8], offset: usize) -> AppResult<i16> {
    Ok(i16::from_le_bytes(
        read_slice(payload, offset, 2)?
            .try_into()
            .expect("slice length checked"),
    ))
}

fn read_u32(payload: &[u8], offset: usize) -> AppResult<u32> {
    Ok(u32::from_le_bytes(
        read_slice(payload, offset, 4)?
            .try_into()
            .expect("slice length checked"),
    ))
}

fn read_i32(payload: &[u8], offset: usize) -> AppResult<i32> {
    Ok(i32::from_le_bytes(
        read_slice(payload, offset, 4)?
            .try_into()
            .expect("slice length checked"),
    ))
}

fn read_u64(payload: &[u8], offset: usize) -> AppResult<u64> {
    Ok(u64::from_le_bytes(
        read_slice(payload, offset, 8)?
            .try_into()
            .expect("slice length checked"),
    ))
}

fn read_i64(payload: &[u8], offset: usize) -> AppResult<i64> {
    Ok(i64::from_le_bytes(
        read_slice(payload, offset, 8)?
            .try_into()
            .expect("slice length checked"),
    ))
}

fn read_f32(payload: &[u8], offset: usize) -> AppResult<f32> {
    Ok(f32::from_le_bytes(
        read_slice(payload, offset, 4)?
            .try_into()
            .expect("slice length checked"),
    ))
}

fn read_f64(payload: &[u8], offset: usize) -> AppResult<f64> {
    Ok(f64::from_le_bytes(
        read_slice(payload, offset, 8)?
            .try_into()
            .expect("slice length checked"),
    ))
}

fn run() -> AppResult<()> {
    let args = Args::parse()?;
    let shutdown_requested = install_shutdown_signal_handlers()?;

    let queue_path = args.events_dir.join(&args.queue_name);
    let schema_path = args.events_dir.join(format!("{}.bfbs", args.queue_name));
    let schema_bytes = fs::read(&schema_path)?;
    let schema = root_as_reflection_schema(&schema_bytes)
        .map_err(|err| AppError::InvalidSchema(format!("failed to read schema: {err}")))?;

    let queue_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&queue_path)?;
    let mut consumer = if args.from_backlog {
        unsafe { SliceConsumer::join_from_backlog(&queue_file) }?
    } else {
        unsafe { SliceConsumer::join(&queue_file) }?
    };
    let object = select_root_object(&schema, args.object_name.as_deref())?;

    eprintln!(
        "queue={} schema={} object={} payload_size={} consumer_index={}",
        queue_path.display(),
        schema_path.display(),
        object.name(),
        consumer.payload_size(),
        consumer.index()
    );

    while !shutdown_requested.load(Ordering::Relaxed) {
        match consumer.read_timeout(args.poll_timeout) {
            Ok(payload) => {
                let flatbuffer = payload_flatbuffer(payload.as_slice())?;
                let table = unsafe { get_any_root(flatbuffer) };
                let value = decode_table(&schema, object, table)?;
                drop(payload);
                println!("{}", serde_json::to_string(&value)?);
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

#[cfg(test)]
mod tests {
    use super::*;

    mod generated {
        #![allow(clippy::all)]
        #![allow(dead_code)]
        #![allow(missing_docs)]
        #![allow(non_camel_case_types)]
        #![allow(non_snake_case)]
        #![allow(non_upper_case_globals)]
        #![allow(unsafe_op_in_unsafe_fn)]

        include!("../../ledger/src/broadcast_events/generated/bank_events_generated.rs");
    }

    const BANK_EVENTS_SCHEMA: &[u8] =
        include_bytes!("../../ledger/src/broadcast_events/schemas/bank_events.bfbs");

    #[test]
    fn decodes_reflected_root_table_to_json() {
        let schema = root_as_reflection_schema(BANK_EVENTS_SCHEMA).unwrap();
        let object = select_root_object(&schema, None).unwrap();
        assert_eq!(object.name().rsplit('.').next(), Some("BankEvent"));

        let payload = timestamp_only_payload(42);
        let flatbuffer = payload_flatbuffer(&payload).unwrap();
        let table = unsafe { get_any_root(flatbuffer) };
        let value = decode_table(&schema, object, table).unwrap();
        assert_eq!(value["timestamp"], Value::from(42_u64));
        assert!(value.get("payload_type").is_none());
        assert!(value.get("payload").is_none());
    }

    #[test]
    fn decodes_reflected_union_payload_to_json() {
        let schema = root_as_reflection_schema(BANK_EVENTS_SCHEMA).unwrap();
        let object = select_root_object(&schema, None).unwrap();

        let payload = new_bank_payload();
        let flatbuffer = payload_flatbuffer(&payload).unwrap();
        let table = unsafe { get_any_root(flatbuffer) };
        let value = decode_table(&schema, object, table).unwrap();

        assert_eq!(value["timestamp"], Value::from(42_u64));
        assert_eq!(value["payload_type"], Value::from("NewBankEvent"));
        assert_eq!(value["payload"]["slot"], Value::from(2_u64));
        assert_eq!(value["payload"]["parent_slot"], Value::from(1_u64));
        assert_eq!(
            value["payload"]["parent_hash"]["bytes"],
            Value::Array((0_u64..32).map(Value::from).collect())
        );
    }

    fn timestamp_only_payload(timestamp: u64) -> Vec<u8> {
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let start = builder.start_table();
        builder.push_slot::<u64>(4, timestamp, 0);
        let root = builder.end_table(start);
        builder.finish_size_prefixed(root, None);

        let flatbuffer = builder.finished_data();
        let mut payload = vec![0; 128];
        payload[..flatbuffer.len()].copy_from_slice(flatbuffer);
        payload
    }

    fn new_bank_payload() -> Vec<u8> {
        use generated::agave::ledger::broadcast_events as bank_events;

        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let parent_hash_bytes = std::array::from_fn(|index| index as u8);
        let parent_hash = bank_events::Hash::new(&parent_hash_bytes);
        let payload = bank_events::NewBankEvent::create(
            &mut builder,
            &bank_events::NewBankEventArgs {
                slot: 2,
                parent_slot: 1,
                parent_hash: Some(&parent_hash),
            },
        );
        let root = bank_events::BankEvent::create(
            &mut builder,
            &bank_events::BankEventArgs {
                timestamp: 42,
                payload_type: bank_events::BankEventPayload::NewBankEvent,
                payload: Some(payload.as_union_value()),
            },
        );
        bank_events::finish_size_prefixed_bank_event_buffer(&mut builder, root);

        let flatbuffer = builder.finished_data();
        let mut queue_payload = vec![0; 128];
        queue_payload[..flatbuffer.len()].copy_from_slice(flatbuffer);
        queue_payload
    }
}
