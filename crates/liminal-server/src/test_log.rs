//! Test-only log capture: a [`MakeWriter`] over `Arc<Mutex<Vec<u8>>>` so unit
//! tests can assert on `tracing` output emitted on the test's own thread via
//! [`tracing::subscriber::with_default`].

use std::io::Write;
use std::sync::{Arc, Mutex};

use tracing_subscriber::fmt::MakeWriter;

/// Cloneable capture buffer usable as a `tracing_subscriber` fmt writer.
#[derive(Clone, Default)]
pub(crate) struct CapturedLog {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl CapturedLog {
    /// Everything written so far, lossily decoded for assertion messages.
    pub(crate) fn contents(&self) -> String {
        let buffer = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        String::from_utf8_lossy(&buffer).into_owned()
    }

    /// Runs `operation` with a plain fmt subscriber writing into this buffer
    /// installed as the THREAD-default subscriber — only events emitted on the
    /// calling thread are captured.
    pub(crate) fn capture<T>(&self, operation: impl FnOnce() -> T) -> T {
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::TRACE)
            .with_ansi(false)
            .with_writer(self.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, operation)
    }
}

impl<'writer> MakeWriter<'writer> for CapturedLog {
    type Writer = CapturedLogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        CapturedLogWriter {
            buffer: Arc::clone(&self.buffer),
        }
    }
}

/// Per-event writer handed out by [`CapturedLog::make_writer`]; appends to the
/// shared buffer under its mutex.
pub(crate) struct CapturedLogWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl Write for CapturedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
