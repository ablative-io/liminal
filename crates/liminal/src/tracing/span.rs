use std::cell::Cell;
use std::error::Error;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::ptr;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::time::{Duration, Instant};

use crate::tracing::TraceContext;

static SPAN_COLLECTOR_MARKER: AtomicPtr<()> = AtomicPtr::new(ptr::null_mut());
static SPAN_COLLECTOR_TOKEN: () = ();
static GLOBAL_SPAN_COLLECTOR: OnceLock<Box<dyn SpanCollector>> = OnceLock::new();

thread_local! {
    static ACTIVE_TRACE_CONTEXT: Cell<Option<TraceContext>> = const { Cell::new(None) };
}

#[derive(Debug)]
pub struct Span {
    name: String,
    context: TraceContext,
    parent: Option<TraceContext>,
    start: Instant,
}

impl Span {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        context: TraceContext,
        parent: Option<TraceContext>,
    ) -> Self {
        Self {
            name: name.into(),
            context,
            parent,
            start: Instant::now(),
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn context(&self) -> TraceContext {
        self.context
    }

    #[must_use]
    pub const fn parent(&self) -> Option<TraceContext> {
        self.parent
    }

    #[must_use]
    pub const fn start(&self) -> Instant {
        self.start
    }

    #[must_use]
    pub fn finish(self) -> FinishedSpan {
        let duration = self.start.elapsed();
        let finished = FinishedSpan::new(self.name, self.context, self.parent, duration);

        if let Some(collector) = global_span_collector() {
            collector.on_span(finished.clone());
        }

        finished
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FinishedSpan {
    name: String,
    context: TraceContext,
    parent: Option<TraceContext>,
    duration: Duration,
}

impl FinishedSpan {
    const fn new(
        name: String,
        context: TraceContext,
        parent: Option<TraceContext>,
        duration: Duration,
    ) -> Self {
        Self {
            name,
            context,
            parent,
            duration,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn context(&self) -> TraceContext {
        self.context
    }

    #[must_use]
    pub const fn parent(&self) -> Option<TraceContext> {
        self.parent
    }

    #[must_use]
    pub const fn duration(&self) -> Duration {
        self.duration
    }
}

#[derive(Debug)]
pub struct ConversationSpan {
    span: Span,
}

impl ConversationSpan {
    #[must_use]
    pub fn new(conversation_id: impl Into<String>) -> Self {
        Self::root(conversation_id)
    }

    #[must_use]
    pub fn root(conversation_id: impl Into<String>) -> Self {
        Self {
            span: Span::new(conversation_id, TraceContext::new_root(), None),
        }
    }

    #[must_use]
    pub fn child(&self, conversation_id: impl Into<String>) -> Self {
        Self::with_parent(conversation_id, self.context())
    }

    #[must_use]
    pub fn with_parent(conversation_id: impl Into<String>, parent: TraceContext) -> Self {
        Self {
            span: Span::new(conversation_id, parent.child(), Some(parent)),
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        self.span.name()
    }

    #[must_use]
    pub const fn context(&self) -> TraceContext {
        self.span.context()
    }

    #[must_use]
    pub const fn parent(&self) -> Option<TraceContext> {
        self.span.parent()
    }

    #[must_use]
    pub const fn message_context(&self) -> TraceContext {
        self.context()
    }

    #[must_use]
    pub fn finish(self) -> FinishedSpan {
        self.span.finish()
    }
}

#[derive(Debug)]
pub struct SpanGuard {
    span: Option<ConversationSpan>,
    name: String,
    context: TraceContext,
    parent: Option<TraceContext>,
    previous_context: Option<TraceContext>,
    context_restored: bool,
}

impl SpanGuard {
    #[must_use]
    pub fn start_conversation(conversation_id: impl Into<String>) -> Self {
        Self::new(conversation_id)
    }

    #[must_use]
    pub fn new(conversation_id: impl Into<String>) -> Self {
        Self::from_conversation(ConversationSpan::root(conversation_id))
    }

    #[must_use]
    pub fn child_conversation(&self, conversation_id: impl Into<String>) -> Self {
        Self::from_conversation(ConversationSpan::with_parent(conversation_id, self.context))
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn context(&self) -> TraceContext {
        self.context
    }

    #[must_use]
    pub const fn parent(&self) -> Option<TraceContext> {
        self.parent
    }

    #[must_use]
    pub const fn message_context(&self) -> TraceContext {
        self.context
    }

    #[must_use]
    pub fn finish(mut self) -> FinishedSpan {
        let finished = self.finish_active_span();
        self.restore_context();
        finished
    }

    fn from_conversation(conversation: ConversationSpan) -> Self {
        let name = conversation.name().to_owned();
        let context = conversation.context();
        let parent = conversation.parent();
        let previous_context = replace_current_trace_context(Some(context));

        Self {
            span: Some(conversation),
            name,
            context,
            parent,
            previous_context,
            context_restored: false,
        }
    }

    fn finish_active_span(&mut self) -> FinishedSpan {
        match self.span.take() {
            Some(span) => span.finish(),
            None => Span::new(self.name.clone(), self.context, self.parent).finish(),
        }
    }

    fn restore_context(&mut self) {
        if !self.context_restored {
            replace_current_trace_context(self.previous_context);
            self.context_restored = true;
        }
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        if self.span.is_some() {
            drop(self.finish_active_span());
        }
        self.restore_context();
    }
}

pub trait SpanCollector: std::fmt::Debug + Send + Sync + 'static {
    fn on_span(&self, span: FinishedSpan);
}

#[derive(Debug, Clone, Copy, Default)]
pub struct NoopCollector;

impl SpanCollector for NoopCollector {
    fn on_span(&self, span: FinishedSpan) {
        drop(span);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanCollectorInstallError {
    AlreadyInstalled,
}

impl Display for SpanCollectorInstallError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::AlreadyInstalled => {
                formatter.write_str("global span collector is already installed")
            }
        }
    }
}

impl Error for SpanCollectorInstallError {}

/// # Errors
///
/// Returns an error when a global span collector has already been installed.
pub fn install_span_collector<Collector>(
    collector: Collector,
) -> Result<(), SpanCollectorInstallError>
where
    Collector: SpanCollector,
{
    install_boxed_span_collector(Box::new(collector))
}

/// # Errors
///
/// Returns an error when a global span collector has already been installed.
pub fn install_boxed_span_collector(
    collector: Box<dyn SpanCollector>,
) -> Result<(), SpanCollectorInstallError> {
    match GLOBAL_SPAN_COLLECTOR.set(collector) {
        Ok(()) => {
            SPAN_COLLECTOR_MARKER.store(
                ptr::addr_of!(SPAN_COLLECTOR_TOKEN).cast_mut(),
                Ordering::Release,
            );
            Ok(())
        }
        Err(_collector) => Err(SpanCollectorInstallError::AlreadyInstalled),
    }
}

#[must_use]
pub fn span_collector_enabled() -> bool {
    !SPAN_COLLECTOR_MARKER.load(Ordering::Acquire).is_null()
}

#[must_use]
pub fn global_span_collector() -> Option<&'static dyn SpanCollector> {
    if span_collector_enabled() {
        GLOBAL_SPAN_COLLECTOR.get().map(Box::as_ref)
    } else {
        None
    }
}

#[must_use]
pub fn current_trace_context() -> Option<TraceContext> {
    ACTIVE_TRACE_CONTEXT.with(Cell::get)
}

fn replace_current_trace_context(context: Option<TraceContext>) -> Option<TraceContext> {
    ACTIVE_TRACE_CONTEXT.with(|active| active.replace(context))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::{
        ConversationSpan, NoopCollector, Span, SpanCollector, SpanGuard, current_trace_context,
    };
    use crate::tracing::TraceContext;

    #[test]
    fn span_finish_returns_finished_span() {
        let context = TraceContext::new_root();
        let parent = Some(TraceContext::new_root());
        let outer_start = Instant::now();
        let span = Span::new("conversation-1", context, parent);

        let finished = span.finish();
        let outer_elapsed = outer_start.elapsed();

        assert_eq!(finished.name(), "conversation-1");
        assert_eq!(finished.context(), context);
        assert_eq!(finished.parent(), parent);
        assert!(finished.duration() <= outer_elapsed);
    }

    #[test]
    fn conversation_span_creates_root_and_child_contexts() {
        let parent = ConversationSpan::new("parent");
        let child = parent.child("child");

        assert_eq!(parent.name(), "parent");
        assert_eq!(parent.parent(), None);
        assert_eq!(parent.message_context(), parent.context());
        assert_eq!(child.parent(), Some(parent.context()));
        assert_eq!(child.context().trace_id(), parent.context().trace_id());
        assert_ne!(child.context().span_id(), parent.context().span_id());
        assert_eq!(child.message_context(), child.context());
    }

    #[test]
    fn span_guard_sets_and_restores_current_context() {
        assert_eq!(current_trace_context(), None);

        let guard = SpanGuard::start_conversation("root");
        assert_eq!(current_trace_context(), Some(guard.context()));

        {
            let child = guard.child_conversation("child");
            assert_eq!(child.parent(), Some(guard.context()));
            assert_eq!(current_trace_context(), Some(child.context()));
        }

        assert_eq!(current_trace_context(), Some(guard.context()));
        drop(guard);
        assert_eq!(current_trace_context(), None);
    }

    #[test]
    fn noop_collector_discards_spans() {
        let collector = NoopCollector;
        let span = Span::new("discard", TraceContext::new_root(), None).finish();

        collector.on_span(span);
    }

    #[test]
    fn finished_span_is_clone_debug() {
        fn assert_clone_debug<T: Clone + std::fmt::Debug>() {}

        assert_clone_debug::<super::FinishedSpan>();
    }

    #[test]
    fn span_start_is_now() {
        let before = Instant::now();
        let span = Span::new("timed", TraceContext::new_root(), None);
        let after = Instant::now();

        assert!(span.start() >= before);
        assert!(span.start() <= after + Duration::from_millis(1));
    }
}
