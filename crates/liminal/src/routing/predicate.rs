/// Comparison operators supported by declarative routing predicates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ComparisonOp {
    /// Field value must equal the comparison value.
    Eq,
    /// Field value must not equal the comparison value.
    Ne,
    /// Field value must be greater than the comparison value.
    Gt,
    /// Field value must be less than the comparison value.
    Lt,
    /// Field value must be greater than or equal to the comparison value.
    Gte,
    /// Field value must be less than or equal to the comparison value.
    Lte,
}

/// Literal value that can be compared by declarative routing predicates.
#[derive(Clone, Debug, PartialEq)]
pub enum FieldValue {
    /// UTF-8 text value.
    Text(String),
    /// Signed integer value.
    Integer(i64),
    /// Floating-point value.
    Float(f64),
    /// Boolean value.
    Boolean(bool),
    /// Explicit null value.
    Null,
}

/// Dot-notation path to a message field.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldPath(String);

impl FieldPath {
    /// Creates a field path from a dot-notation string.
    #[must_use]
    pub fn new(path: impl Into<String>) -> Self {
        Self(path.into())
    }

    /// Returns borrowed field path segments separated by dots.
    pub fn segments(&self) -> impl Iterator<Item = &str> + '_ {
        self.0.split('.')
    }
}

/// Declarative routing predicate shape.
///
/// Predicate evaluation is implemented by later routing briefs. When evaluated,
/// any predicate that references a missing message field must evaluate to false
/// without returning an error.
#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    /// Compare a field with a literal value using a comparison operator.
    Comparison {
        /// Message field path to compare.
        field: FieldPath,
        /// Comparison operator to apply.
        op: ComparisonOp,
        /// Literal comparison value.
        value: FieldValue,
    },
    /// Logical AND over zero or more child predicates.
    And(Vec<Self>),
    /// Logical OR over zero or more child predicates.
    Or(Vec<Self>),
    /// Logical negation of a single child predicate.
    Not(Box<Self>),
    /// Inclusive range check for numeric or text values.
    Range {
        /// Message field path to compare.
        field: FieldPath,
        /// Inclusive lower bound.
        lower: FieldValue,
        /// Inclusive upper bound.
        upper: FieldValue,
    },
    /// Test whether a field is present in the message.
    Exists {
        /// Message field path to check.
        field: FieldPath,
    },
}

#[cfg(test)]
mod tests {
    use super::{ComparisonOp, FieldPath, FieldValue, Predicate};

    #[test]
    fn comparison_predicate_constructs() {
        let predicate = Predicate::Comparison {
            field: FieldPath::new("amount"),
            op: ComparisonOp::Gt,
            value: FieldValue::Integer(1_000),
        };

        assert_eq!(
            predicate,
            Predicate::Comparison {
                field: FieldPath::new("amount"),
                op: ComparisonOp::Gt,
                value: FieldValue::Integer(1_000),
            }
        );
    }

    #[test]
    fn comparison_debug_round_trips_all_variants() {
        assert_eq!(format!("{:?}", ComparisonOp::Eq), "Eq");
        assert_eq!(format!("{:?}", ComparisonOp::Ne), "Ne");
        assert_eq!(format!("{:?}", ComparisonOp::Gt), "Gt");
        assert_eq!(format!("{:?}", ComparisonOp::Lt), "Lt");
        assert_eq!(format!("{:?}", ComparisonOp::Gte), "Gte");
        assert_eq!(format!("{:?}", ComparisonOp::Lte), "Lte");
    }

    #[test]
    fn boolean_combinators_construct() {
        let amount = Predicate::Comparison {
            field: FieldPath::new("amount"),
            op: ComparisonOp::Gt,
            value: FieldValue::Integer(1_000),
        };
        let region = Predicate::Comparison {
            field: FieldPath::new("region"),
            op: ComparisonOp::Eq,
            value: FieldValue::Text(String::from("eu")),
        };

        let and = Predicate::And(vec![amount.clone(), region.clone()]);
        let or = Predicate::Or(vec![amount.clone(), region.clone()]);
        let not = Predicate::Not(Box::new(region.clone()));
        let nested = Predicate::And(vec![amount.clone(), Predicate::Or(vec![region.clone()])]);

        assert_eq!(and, Predicate::And(vec![amount.clone(), region.clone()]));
        assert_eq!(or, Predicate::Or(vec![amount.clone(), region.clone()]));
        assert_eq!(not, Predicate::Not(Box::new(region.clone())));
        assert_eq!(
            nested,
            Predicate::And(vec![amount, Predicate::Or(vec![region])])
        );
    }

    #[test]
    fn empty_boolean_combinators_construct() {
        assert_eq!(Predicate::And(Vec::new()), Predicate::And(Vec::new()));
        assert_eq!(Predicate::Or(Vec::new()), Predicate::Or(Vec::new()));
    }

    #[test]
    fn range_and_exists_predicates_construct() {
        let integer_range = Predicate::Range {
            field: FieldPath::new("amount"),
            lower: FieldValue::Integer(100),
            upper: FieldValue::Integer(200),
        };
        let text_range = Predicate::Range {
            field: FieldPath::new("name"),
            lower: FieldValue::Text(String::from("a")),
            upper: FieldValue::Text(String::from("z")),
        };
        let exists = Predicate::Exists {
            field: FieldPath::new("region"),
        };

        assert_eq!(
            integer_range,
            Predicate::Range {
                field: FieldPath::new("amount"),
                lower: FieldValue::Integer(100),
                upper: FieldValue::Integer(200),
            }
        );
        assert_eq!(
            text_range,
            Predicate::Range {
                field: FieldPath::new("name"),
                lower: FieldValue::Text(String::from("a")),
                upper: FieldValue::Text(String::from("z")),
            }
        );
        assert_eq!(
            exists,
            Predicate::Exists {
                field: FieldPath::new("region"),
            }
        );
    }

    #[test]
    fn field_path_segments_are_borrowed_dot_parts() {
        let nested = FieldPath::new("user.address.city");
        let nested_segments: Vec<_> = nested.segments().collect();
        let single = FieldPath::new("name");
        let single_segments: Vec<_> = single.segments().collect();

        assert_eq!(nested_segments, ["user", "address", "city"]);
        assert_eq!(single_segments, ["name"]);
    }

    #[test]
    fn routing_root_re_exports_predicate_types() {
        use crate::routing::{
            ComparisonOp as RootComparisonOp, FieldPath as RootFieldPath,
            FieldValue as RootFieldValue, Predicate as RootPredicate,
        };

        let predicate = RootPredicate::Comparison {
            field: RootFieldPath::new("amount"),
            op: RootComparisonOp::Gte,
            value: RootFieldValue::Integer(100),
        };

        assert_eq!(
            predicate,
            RootPredicate::Comparison {
                field: RootFieldPath::new("amount"),
                op: RootComparisonOp::Gte,
                value: RootFieldValue::Integer(100),
            }
        );
    }
}
