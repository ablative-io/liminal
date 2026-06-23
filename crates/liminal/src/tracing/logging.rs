#![allow(clippy::module_name_repetitions)]

use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::ptr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicPtr, AtomicU8, Ordering};
use std::time::Instant;

use crate::tracing::{TraceContext, current_trace_context};

// Zero-overhead "is an emitter installed?" sentinel: an atomic pointer that is
// null until an emitter is installed, then points at a known-valid static token
// (never at heap data). Mirrors the span-collector marker in span.rs. The actual
// emitter lives in the OnceLock; this marker only gates the no-op fast path.
static LOG_EMITTER_MARKER: AtomicPtr<()> = AtomicPtr::new(ptr::null_mut());
static LOG_EMITTER_TOKEN: () = ();
static GLOBAL_LOG_EMITTER: OnceLock<Box<dyn LogEmitter>> = OnceLock::new();
static MIN_LOG_LEVEL: AtomicU8 = AtomicU8::new(MIN_LOG_LEVEL_UNSET);

const MIN_LOG_LEVEL_UNSET: u8 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LogValue {
    String(String),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

#[derive(Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub timestamp: Instant,
    pub level: LogLevel,
    pub target: &'static str,
    pub message: String,
    pub trace_context: Option<TraceContext>,
    pub fields: Vec<(String, LogValue)>,
}

impl LogEntry {
    #[must_use]
    pub const fn trace_id(&self) -> Option<u128> {
        match self.trace_context {
            Some(context) => Some(context.trace_id()),
            None => None,
        }
    }

    #[must_use]
    pub const fn span_id(&self) -> Option<u64> {
        match self.trace_context {
            Some(context) => Some(context.span_id()),
            None => None,
        }
    }
}

pub trait LogEmitter: std::fmt::Debug + Send + Sync + 'static {
    fn emit(&self, entry: &LogEntry);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopEmitter;

impl LogEmitter for NoopEmitter {
    fn emit(&self, entry: &LogEntry) {
        let _ = entry;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogEmitterInstallError {
    AlreadyInstalled,
}

impl Display for LogEmitterInstallError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::AlreadyInstalled => {
                formatter.write_str("global log emitter is already installed")
            }
        }
    }
}

impl Error for LogEmitterInstallError {}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntryBuilder {
    level: LogLevel,
    target: &'static str,
    message: String,
    fields: Vec<(String, LogValue)>,
}

impl LogEntryBuilder {
    #[must_use]
    pub fn new(level: LogLevel, target: &'static str, message: impl Into<String>) -> Self {
        Self {
            level,
            target,
            message: message.into(),
            fields: Vec::new(),
        }
    }

    #[must_use]
    pub fn field(mut self, key: impl Into<String>, value: LogValue) -> Self {
        self.fields.push((key.into(), value));
        self
    }

    #[must_use]
    pub fn build(self) -> LogEntry {
        LogEntry {
            timestamp: Instant::now(),
            level: self.level,
            target: self.target,
            message: self.message,
            trace_context: current_trace_context(),
            fields: self.fields,
        }
    }
}

/// # Errors
///
/// Returns an error when a global log emitter has already been installed.
pub fn install_log_emitter<Emitter>(emitter: Emitter) -> Result<(), LogEmitterInstallError>
where
    Emitter: LogEmitter,
{
    install_boxed_log_emitter(Box::new(emitter))
}

/// # Errors
///
/// Returns an error when a global log emitter has already been installed.
pub fn install_boxed_log_emitter(
    emitter: Box<dyn LogEmitter>,
) -> Result<(), LogEmitterInstallError> {
    match GLOBAL_LOG_EMITTER.set(emitter) {
        Ok(()) => {
            // Publish the marker after the emitter is in the OnceLock, so any
            // thread that observes a non-null marker also observes the emitter.
            LOG_EMITTER_MARKER.store(
                ptr::addr_of!(LOG_EMITTER_TOKEN).cast_mut(),
                Ordering::Release,
            );
            Ok(())
        }
        Err(emitter) => {
            drop(emitter);
            Err(LogEmitterInstallError::AlreadyInstalled)
        }
    }
}

#[must_use]
pub fn log_emitter_enabled() -> bool {
    !LOG_EMITTER_MARKER.load(Ordering::Acquire).is_null()
}

#[must_use]
pub fn global_log_emitter() -> Option<&'static dyn LogEmitter> {
    if log_emitter_enabled() {
        GLOBAL_LOG_EMITTER.get().map(Box::as_ref)
    } else {
        None
    }
}

pub fn set_min_log_level(level: LogLevel) {
    MIN_LOG_LEVEL.store(log_level_filter_value(level), Ordering::Release);
}

pub fn clear_min_log_level() {
    MIN_LOG_LEVEL.store(MIN_LOG_LEVEL_UNSET, Ordering::Release);
}

#[must_use]
pub fn min_log_level() -> Option<LogLevel> {
    log_level_from_filter_value(MIN_LOG_LEVEL.load(Ordering::Acquire))
}

#[must_use]
pub fn level_enabled(level: LogLevel) -> bool {
    let minimum = MIN_LOG_LEVEL.load(Ordering::Acquire);
    minimum == MIN_LOG_LEVEL_UNSET || log_level_filter_value(level) >= minimum
}

#[must_use]
pub fn log_enabled(level: LogLevel) -> bool {
    log_emitter_enabled() && level_enabled(level)
}

pub fn emit_entry(entry: &LogEntry) {
    if let Some(emitter) = enabled_log_emitter(entry.level) {
        emitter.emit(entry);
    }
}

pub fn log(level: LogLevel, target: &'static str, message: impl Into<String>) {
    log_with_fields(level, target, message, |builder| builder);
}

pub fn log_with_fields<Fields>(
    level: LogLevel,
    target: &'static str,
    message: impl Into<String>,
    fields: Fields,
) where
    Fields: FnOnce(LogEntryBuilder) -> LogEntryBuilder,
{
    let Some(emitter) = enabled_log_emitter(level) else {
        return;
    };

    let entry = fields(LogEntryBuilder::new(level, target, message)).build();
    emitter.emit(&entry);
}

fn enabled_log_emitter(level: LogLevel) -> Option<&'static dyn LogEmitter> {
    if !log_emitter_enabled() || !level_enabled(level) {
        return None;
    }

    GLOBAL_LOG_EMITTER.get().map(Box::as_ref)
}

const fn log_level_filter_value(level: LogLevel) -> u8 {
    match level {
        LogLevel::Trace => 1,
        LogLevel::Debug => 2,
        LogLevel::Info => 3,
        LogLevel::Warn => 4,
        LogLevel::Error => 5,
    }
}

const fn log_level_from_filter_value(value: u8) -> Option<LogLevel> {
    match value {
        MIN_LOG_LEVEL_UNSET => None,
        1 => Some(LogLevel::Trace),
        2 => Some(LogLevel::Debug),
        3 => Some(LogLevel::Info),
        4 => Some(LogLevel::Warn),
        5 => Some(LogLevel::Error),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use std::sync::{Arc, Mutex, PoisonError};

    use super::{
        LogEmitter, LogEntry, LogEntryBuilder, LogLevel, LogValue, NoopEmitter,
        clear_min_log_level, install_log_emitter, level_enabled, log, min_log_level,
        set_min_log_level,
    };
    use crate::tracing::{SpanGuard, TraceContext};

    #[test]
    fn log_levels_are_ordered_by_severity() {
        assert!(LogLevel::Trace < LogLevel::Debug);
        assert!(LogLevel::Debug < LogLevel::Info);
        assert!(LogLevel::Info < LogLevel::Warn);
        assert!(LogLevel::Warn < LogLevel::Error);
    }

    #[test]
    fn public_logging_types_are_debug() {
        fn assert_debug<T: std::fmt::Debug>() {}

        assert_debug::<super::LogLevel>();
        assert_debug::<super::LogValue>();
        assert_debug::<super::LogEntry>();
        assert_debug::<super::LogEntryBuilder>();
        assert_debug::<super::NoopEmitter>();
        assert_debug::<super::LogEmitterInstallError>();
    }

    #[test]
    fn builder_constructs_structured_entries() {
        let entry = LogEntryBuilder::new(LogLevel::Info, module_path!(), "user login")
            .field("user_id", LogValue::String("alice".to_owned()))
            .field("attempt", LogValue::I64(-1))
            .field("count", LogValue::U64(2))
            .field("ratio", LogValue::F64(0.5))
            .field("success", LogValue::Bool(true))
            .build();

        assert_eq!(entry.level, LogLevel::Info);
        assert_eq!(entry.target, module_path!());
        assert_eq!(entry.message, "user login");
        assert_eq!(entry.trace_context, None);
        assert_eq!(entry.fields.len(), 5);
        assert_eq!(
            entry.fields[0],
            ("user_id".to_owned(), LogValue::String("alice".to_owned()))
        );
        assert_eq!(entry.fields[1], ("attempt".to_owned(), LogValue::I64(-1)));
        assert_eq!(entry.fields[2], ("count".to_owned(), LogValue::U64(2)));
        assert!(
            matches!(entry.fields[3], (ref key, LogValue::F64(value)) if key == "ratio" && (value - 0.5).abs() < f64::EPSILON)
        );
        assert_eq!(
            entry.fields[4],
            ("success".to_owned(), LogValue::Bool(true))
        );
    }

    #[test]
    fn builder_captures_active_trace_context_at_build_time() {
        let builder = LogEntryBuilder::new(LogLevel::Debug, module_path!(), "inside span");
        let guard = SpanGuard::start_conversation("conversation");

        let entry = builder.build();

        assert_eq!(entry.trace_context, Some(guard.context()));
        assert_eq!(entry.trace_id(), Some(guard.context().trace_id()));
        assert_eq!(entry.span_id(), Some(guard.context().span_id()));
    }

    #[test]
    fn builder_records_no_context_after_span_leaves_scope() {
        {
            let guard = SpanGuard::start_conversation("conversation");
            let entry = LogEntryBuilder::new(LogLevel::Info, module_path!(), "inside span").build();
            assert_eq!(entry.trace_context, Some(guard.context()));
        }

        let entry = LogEntryBuilder::new(LogLevel::Info, module_path!(), "outside span").build();
        assert_eq!(entry.trace_context, None);
        assert_eq!(entry.trace_id(), None);
        assert_eq!(entry.span_id(), None);
    }

    #[test]
    fn noop_emitter_discards_entries() {
        let emitter = NoopEmitter;
        let entry = LogEntryBuilder::new(LogLevel::Warn, module_path!(), "discarded").build();

        emitter.emit(&entry);
    }

    #[test]
    fn minimum_level_filter_discards_lower_severity_entries() {
        clear_min_log_level();
        assert_eq!(min_log_level(), None);
        assert!(level_enabled(LogLevel::Trace));
        assert!(level_enabled(LogLevel::Debug));

        set_min_log_level(LogLevel::Info);

        assert_eq!(min_log_level(), Some(LogLevel::Info));
        assert!(!level_enabled(LogLevel::Trace));
        assert!(!level_enabled(LogLevel::Debug));
        assert!(level_enabled(LogLevel::Info));
        assert!(level_enabled(LogLevel::Warn));
        assert!(level_enabled(LogLevel::Error));

        clear_min_log_level();
    }

    /// Shared log of `(trace context, message)` pairs the recording emitter captures.
    type RecordedLog = Arc<Mutex<Vec<(Option<TraceContext>, String)>>>;

    /// Records the trace context and message of every emitted entry, so a test
    /// can assert what the global `log` path actually emitted.
    #[derive(Debug)]
    struct RecordingEmitter(RecordedLog);

    impl LogEmitter for RecordingEmitter {
        fn emit(&self, entry: &LogEntry) {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((entry.trace_context, entry.message.clone()));
        }
    }

    // Single owner of the process-global log emitter: the emitter is a write-once
    // OnceLock, so the no-op (no emitter), filtered, and emit-with-context paths are
    // all exercised here in sequence rather than in separate, racy tests.
    #[test]
    fn global_log_path_is_no_op_until_installed_then_emits_with_trace_context() {
        // 1. No emitter installed: the call does zero work — neither the fields
        //    closure nor the message conversion runs.
        clear_min_log_level();
        let probed = Cell::new(false);
        let mut fields_called = false;
        super::log_with_fields(
            LogLevel::Info,
            module_path!(),
            MessageProbe::new(&probed),
            |builder| {
                fields_called = true;
                builder.field("called", LogValue::Bool(true))
            },
        );
        assert!(
            !fields_called,
            "no-op log must not invoke the fields closure"
        );
        assert!(!probed.get(), "no-op log must not build the message");

        // 2. Install the recording emitter (process-global, once).
        let recorded = Arc::new(Mutex::new(Vec::new()));
        assert!(
            install_log_emitter(RecordingEmitter(Arc::clone(&recorded))).is_ok(),
            "no emitter installed yet in this test binary"
        );

        // 3. Filtered: emitter installed but level below the floor → still no work,
        //    and nothing is emitted to the sink.
        set_min_log_level(LogLevel::Info);
        let filtered_probe = Cell::new(false);
        let mut filtered_fields = false;
        super::log_with_fields(
            LogLevel::Debug,
            module_path!(),
            MessageProbe::new(&filtered_probe),
            |builder| {
                filtered_fields = true;
                builder.field("called", LogValue::Bool(true))
            },
        );
        assert!(
            !filtered_fields,
            "filtered log must not invoke the fields closure"
        );
        assert!(
            !filtered_probe.get(),
            "filtered log must not build the message"
        );
        assert!(
            recorded
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty(),
            "filtered log must not reach the emitter"
        );

        // 4. Enabled, inside a span: the entry the emitter receives carries the
        //    live trace context captured at emit time (end-to-end log -> emit wiring).
        clear_min_log_level();
        let context = {
            let guard = SpanGuard::start_conversation("conversation");
            log(LogLevel::Info, module_path!(), "inside span");
            guard.context()
        };

        let entries = recorded.lock().unwrap_or_else(PoisonError::into_inner);
        assert_eq!(
            entries.len(),
            1,
            "exactly one entry emitted via the global path"
        );
        assert_eq!(
            entries[0].0,
            Some(context),
            "emitted entry must carry the active trace context"
        );
        assert_eq!(entries[0].1, "inside span");
        drop(entries);
        clear_min_log_level();
    }

    #[derive(Debug)]
    struct MessageProbe<'a>(&'a Cell<bool>);

    impl<'a> MessageProbe<'a> {
        const fn new(converted: &'a Cell<bool>) -> Self {
            Self(converted)
        }
    }

    impl From<MessageProbe<'_>> for String {
        fn from(probe: MessageProbe<'_>) -> Self {
            probe.0.set(true);
            Self::new()
        }
    }
}
