use rand::RngExt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceContext {
    pub trace_id: u128,
    pub span_id: u64,
}

impl TraceContext {
    #[must_use]
    pub fn new_root() -> Self {
        Self {
            trace_id: next_trace_id(),
            span_id: next_span_id_except(None),
        }
    }

    #[must_use]
    pub fn child(&self) -> Self {
        Self {
            trace_id: self.trace_id,
            span_id: next_span_id_except(Some(self.span_id)),
        }
    }

    #[must_use]
    pub const fn from_ids(trace_id: u128, span_id: u64) -> Self {
        Self { trace_id, span_id }
    }

    #[must_use]
    pub const fn trace_id(&self) -> u128 {
        self.trace_id
    }

    #[must_use]
    pub const fn span_id(&self) -> u64 {
        self.span_id
    }

    #[must_use]
    pub fn to_traceparent(&self) -> String {
        format!("00-{:032x}-{:016x}-01", self.trace_id, self.span_id)
    }

    #[must_use]
    pub fn from_traceparent(header: &str) -> Option<Self> {
        let mut parts = header.split('-');
        let version = parts.next()?;
        let trace_id = parts.next()?;
        let span_id = parts.next()?;
        let flags = parts.next()?;

        if parts.next().is_some()
            || version != "00"
            || flags != "01"
            || !is_hex_with_len(trace_id, 32)
            || !is_hex_with_len(span_id, 16)
        {
            return None;
        }

        let trace_id = u128::from_str_radix(trace_id, 16).ok()?;
        let span_id = u64::from_str_radix(span_id, 16).ok()?;

        if trace_id == 0 || span_id == 0 {
            return None;
        }

        Some(Self::from_ids(trace_id, span_id))
    }
}

fn next_trace_id() -> u128 {
    loop {
        let candidate = thread_local_random::<u128>();
        if candidate != 0 {
            return candidate;
        }
    }
}

fn next_span_id_except(excluded: Option<u64>) -> u64 {
    loop {
        let candidate = thread_local_random::<u64>();
        if candidate != 0 && Some(candidate) != excluded {
            return candidate;
        }
    }
}

fn thread_local_random<T>() -> T
where
    rand::distr::StandardUniform: rand::distr::Distribution<T>,
{
    let mut rng = rand::rng();
    rng.random()
}

fn is_hex_with_len(value: &str, len: usize) -> bool {
    value.len() == len && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::TraceContext;

    #[test]
    fn new_root_generates_non_zero_ids() {
        let context = TraceContext::new_root();

        assert_ne!(context.trace_id(), 0);
        assert_ne!(context.span_id(), 0);
    }

    #[test]
    fn child_preserves_trace_id_and_changes_span_id() {
        let parent = TraceContext::new_root();
        let child = parent.child();

        assert_eq!(child.trace_id(), parent.trace_id());
        assert_ne!(child.span_id(), parent.span_id());
        assert_ne!(child.span_id(), 0);
    }

    #[test]
    fn roots_have_distinct_trace_ids() {
        let first = TraceContext::new_root();
        let second = TraceContext::new_root();

        assert_ne!(first.trace_id(), second.trace_id());
    }

    #[test]
    fn from_ids_round_trips() {
        let context = TraceContext::new_root();

        assert_eq!(
            TraceContext::from_ids(context.trace_id(), context.span_id()),
            context
        );
    }

    #[test]
    fn traceparent_round_trips() {
        let context = TraceContext::new_root();
        let traceparent = context.to_traceparent();

        assert_eq!(traceparent.len(), 55);
        assert_eq!(TraceContext::from_traceparent(&traceparent), Some(context));
    }

    #[test]
    fn invalid_traceparents_return_none() {
        assert_eq!(TraceContext::from_traceparent(""), None);
        assert_eq!(
            TraceContext::from_traceparent(
                "01-00000000000000000000000000000001-0000000000000001-01"
            ),
            None
        );
        assert_eq!(
            TraceContext::from_traceparent(
                "00-00000000000000000000000000000001-0000000000000001-00"
            ),
            None
        );
        assert_eq!(
            TraceContext::from_traceparent("00-not-0000000000000001-01"),
            None
        );
        assert_eq!(
            TraceContext::from_traceparent(
                "00-00000000000000000000000000000000-0000000000000001-01"
            ),
            None
        );
        assert_eq!(
            TraceContext::from_traceparent(
                "00-00000000000000000000000000000001-0000000000000000-01"
            ),
            None
        );
    }
}
