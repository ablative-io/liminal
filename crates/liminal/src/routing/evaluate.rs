use std::cmp::Ordering;

use crate::routing::{ComparisonOp, FieldPath, FieldValue, Predicate};

/// Borrowed view of a message field value used by routing predicate evaluation.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FieldValueRef<'a> {
    /// Borrowed UTF-8 text value.
    Text(&'a str),
    /// Signed integer value.
    Integer(i64),
    /// Floating-point value.
    Float(f64),
    /// Boolean value.
    Boolean(bool),
    /// Explicit null value.
    Null,
}

/// Provides borrowed access to fields from a message being routed.
pub trait FieldAccessor: std::fmt::Debug {
    /// Returns a borrowed field value for `path`, or `None` when absent.
    fn field(&self, path: &FieldPath) -> Option<FieldValueRef<'_>>;
}

/// Evaluates a predicate against borrowed message fields.
#[must_use]
pub fn evaluate(predicate: &Predicate, accessor: &dyn FieldAccessor) -> bool {
    match predicate {
        Predicate::Comparison { field, op, value } => accessor
            .field(field)
            .is_some_and(|field_value| compare_values(field_value, *op, value)),
        Predicate::And(children) => children.iter().all(|child| evaluate(child, accessor)),
        Predicate::Or(children) => children.iter().any(|child| evaluate(child, accessor)),
        Predicate::Not(child) => !evaluate(child, accessor),
        Predicate::Range {
            field,
            lower,
            upper,
        } => accessor.field(field).is_some_and(|field_value| {
            compare_values(field_value, ComparisonOp::Gte, lower)
                && compare_values(field_value, ComparisonOp::Lte, upper)
        }),
        Predicate::Exists { field } => accessor.field(field).is_some(),
    }
}

fn compare_values(field_value: FieldValueRef<'_>, op: ComparisonOp, literal: &FieldValue) -> bool {
    match (field_value, literal) {
        (FieldValueRef::Text(left), FieldValue::Text(right)) => {
            compare_ordering(left.cmp(right.as_str()), op)
        }
        (FieldValueRef::Integer(left), FieldValue::Integer(right)) => {
            compare_ordering(left.cmp(right), op)
        }
        (FieldValueRef::Float(left), FieldValue::Float(right)) => left
            .partial_cmp(right)
            .is_some_and(|ordering| compare_ordering(ordering, op)),
        (FieldValueRef::Boolean(left), FieldValue::Boolean(right)) => {
            compare_equality(left == *right, op)
        }
        (FieldValueRef::Null, FieldValue::Null) => compare_equality(true, op),
        _ => false,
    }
}

const fn compare_ordering(ordering: Ordering, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Eq => ordering.is_eq(),
        ComparisonOp::Ne => !ordering.is_eq(),
        ComparisonOp::Gt => ordering.is_gt(),
        ComparisonOp::Lt => ordering.is_lt(),
        ComparisonOp::Gte => ordering.is_ge(),
        ComparisonOp::Lte => ordering.is_le(),
    }
}

const fn compare_equality(equal: bool, op: ComparisonOp) -> bool {
    match op {
        ComparisonOp::Eq => equal,
        ComparisonOp::Ne => !equal,
        ComparisonOp::Gt | ComparisonOp::Lt | ComparisonOp::Gte | ComparisonOp::Lte => false,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::{FieldAccessor, FieldValueRef, evaluate};
    use crate::routing::{ComparisonOp, FieldPath, FieldValue, Predicate};

    #[derive(Debug)]
    struct StaticAccessor<'a> {
        field: &'a str,
        value: FieldValueRef<'a>,
    }

    impl<'a> StaticAccessor<'a> {
        const fn new(field: &'a str, value: FieldValueRef<'a>) -> Self {
            Self { field, value }
        }
    }

    impl FieldAccessor for StaticAccessor<'_> {
        fn field(&self, path: &FieldPath) -> Option<FieldValueRef<'_>> {
            path.segments().eq([self.field]).then_some(self.value)
        }
    }

    #[derive(Debug)]
    struct CountingAccessor {
        count: Cell<usize>,
    }

    impl CountingAccessor {
        const fn new() -> Self {
            Self {
                count: Cell::new(0),
            }
        }

        fn count(&self) -> usize {
            self.count.get()
        }
    }

    impl FieldAccessor for CountingAccessor {
        fn field(&self, path: &FieldPath) -> Option<FieldValueRef<'_>> {
            self.count.set(self.count.get() + 1);

            if path.segments().eq(["truth"]) {
                Some(FieldValueRef::Boolean(true))
            } else if path.segments().eq(["falsehood"]) {
                Some(FieldValueRef::Boolean(false))
            } else if path.segments().eq(["third"]) {
                Some(FieldValueRef::Boolean(true))
            } else {
                None
            }
        }
    }

    fn integer_comparison(op: ComparisonOp, value: i64) -> Predicate {
        Predicate::Comparison {
            field: FieldPath::new("amount"),
            op,
            value: FieldValue::Integer(value),
        }
    }

    fn boolean_comparison(field: &str, value: bool) -> Predicate {
        Predicate::Comparison {
            field: FieldPath::new(field),
            op: ComparisonOp::Eq,
            value: FieldValue::Boolean(value),
        }
    }

    fn amount_range() -> Predicate {
        Predicate::Range {
            field: FieldPath::new("amount"),
            lower: FieldValue::Integer(100),
            upper: FieldValue::Integer(200),
        }
    }

    #[test]
    fn integer_greater_than_comparison_matches() {
        let predicate = integer_comparison(ComparisonOp::Gt, 1_000);
        let accessor = StaticAccessor::new("amount", FieldValueRef::Integer(1_500));

        assert!(evaluate(&predicate, &accessor));
    }

    #[test]
    fn integer_greater_than_comparison_rejects_lower_value() {
        let predicate = integer_comparison(ComparisonOp::Gt, 1_000);
        let accessor = StaticAccessor::new("amount", FieldValueRef::Integer(500));

        assert!(!evaluate(&predicate, &accessor));
    }

    #[test]
    fn exists_returns_true_for_present_field() {
        let predicate = Predicate::Exists {
            field: FieldPath::new("region"),
        };
        let accessor = StaticAccessor::new("region", FieldValueRef::Text("eu"));

        assert!(evaluate(&predicate, &accessor));
    }

    #[test]
    fn exists_returns_false_for_missing_field() {
        let predicate = Predicate::Exists {
            field: FieldPath::new("missing"),
        };
        let accessor = StaticAccessor::new("region", FieldValueRef::Text("eu"));

        assert!(!evaluate(&predicate, &accessor));
    }

    #[test]
    fn comparison_returns_false_for_missing_field() {
        let predicate = Predicate::Comparison {
            field: FieldPath::new("missing"),
            op: ComparisonOp::Eq,
            value: FieldValue::Text(String::from("x")),
        };
        let accessor = StaticAccessor::new("region", FieldValueRef::Text("eu"));

        assert!(!evaluate(&predicate, &accessor));
    }

    #[test]
    fn and_short_circuits_at_first_false() {
        let predicate = Predicate::And(vec![
            boolean_comparison("truth", true),
            boolean_comparison("falsehood", true),
            boolean_comparison("third", true),
        ]);
        let accessor = CountingAccessor::new();

        assert!(!evaluate(&predicate, &accessor));
        assert_eq!(accessor.count(), 2);
    }

    #[test]
    fn or_short_circuits_at_first_true() {
        let predicate = Predicate::Or(vec![
            boolean_comparison("falsehood", true),
            boolean_comparison("truth", true),
            boolean_comparison("third", true),
        ]);
        let accessor = CountingAccessor::new();

        assert!(evaluate(&predicate, &accessor));
        assert_eq!(accessor.count(), 2);
    }

    #[test]
    fn not_negates_child_predicate() {
        let true_predicate = Predicate::Not(Box::new(boolean_comparison("truth", true)));
        let false_predicate = Predicate::Not(Box::new(boolean_comparison("falsehood", true)));
        let accessor = CountingAccessor::new();

        assert!(!evaluate(&true_predicate, &accessor));
        assert!(evaluate(&false_predicate, &accessor));
    }

    #[test]
    fn empty_boolean_combinators_have_vacuous_values() {
        assert!(evaluate(
            &Predicate::And(Vec::new()),
            &StaticAccessor::new("amount", FieldValueRef::Integer(1))
        ));
        assert!(!evaluate(
            &Predicate::Or(Vec::new()),
            &StaticAccessor::new("amount", FieldValueRef::Integer(1))
        ));
    }

    #[test]
    fn range_includes_middle_and_bounds() {
        for value in [150, 100, 200] {
            let accessor = StaticAccessor::new("amount", FieldValueRef::Integer(value));

            assert!(evaluate(&amount_range(), &accessor));
        }
    }

    #[test]
    fn range_rejects_value_below_lower_bound() {
        let accessor = StaticAccessor::new("amount", FieldValueRef::Integer(50));

        assert!(!evaluate(&amount_range(), &accessor));
    }

    #[test]
    fn range_rejects_missing_field() {
        let accessor = StaticAccessor::new("region", FieldValueRef::Text("eu"));

        assert!(!evaluate(&amount_range(), &accessor));
    }

    #[test]
    fn range_rejects_type_mismatch() {
        let accessor = StaticAccessor::new("amount", FieldValueRef::Text("150"));

        assert!(!evaluate(&amount_range(), &accessor));
    }
}
