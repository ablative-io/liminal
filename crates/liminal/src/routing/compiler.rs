//! Compiles declarative [`Predicate`] trees into executable routing functions.
//!
//! ADR-003 unifies routing on a single runtime model: predicates compile to
//! routing functions and the engine always executes functions. This module
//! performs that compilation. It applies index-aware optimizations -- ordering
//! field extraction so cheaper, more selective clauses are evaluated first --
//! while guaranteeing the compiled form yields the identical boolean decision
//! as direct [`evaluate`](crate::routing::evaluate::evaluate) for every input.
//!
//! Compilation borrows the predicate and never mutates it; the produced
//! [`CompiledFunction`] owns its optimized plan and evaluates against borrowed
//! message fields with no per-message heap allocation.

use std::collections::BTreeMap;

use crate::routing::evaluate::compare_values;
use crate::routing::{ComparisonOp, FieldAccessor, FieldPath, FieldValue, Predicate};

/// An executable routing function compiled from a [`Predicate`].
///
/// Produced by [`compile`]. Evaluating a compiled function against a message
/// yields the same boolean routing decision as direct predicate evaluation.
#[derive(Clone, Debug, PartialEq)]
pub struct CompiledFunction {
    plan: Plan,
}

impl CompiledFunction {
    /// Evaluates the compiled routing function against borrowed message fields.
    ///
    /// Returns the same result as [`evaluate`](crate::routing::evaluate::evaluate)
    /// applied to the originating predicate.
    #[must_use]
    pub fn evaluate(&self, accessor: &dyn FieldAccessor) -> bool {
        eval_plan(&self.plan, accessor)
    }
}

/// Compiles a borrowed predicate into an optimized [`CompiledFunction`].
///
/// The supplied predicate is borrowed and left unchanged.
#[must_use]
pub fn compile(predicate: &Predicate) -> CompiledFunction {
    CompiledFunction {
        plan: compile_plan(predicate),
    }
}

/// Optimized internal representation of a compiled predicate.
///
/// Boolean combinators store their children in evaluation order after
/// index-aware reordering; leaf nodes mirror their predicate counterparts.
#[derive(Clone, Debug, PartialEq)]
enum Plan {
    Comparison {
        field: FieldPath,
        op: ComparisonOp,
        value: FieldValue,
    },
    Range {
        field: FieldPath,
        lower: FieldValue,
        upper: FieldValue,
    },
    Exists {
        field: FieldPath,
    },
    All(Vec<Self>),
    Any(Vec<Self>),
    Not(Box<Self>),
}

fn compile_plan(predicate: &Predicate) -> Plan {
    match predicate {
        Predicate::Comparison { field, op, value } => Plan::Comparison {
            field: field.clone(),
            op: *op,
            value: value.clone(),
        },
        Predicate::Range {
            field,
            lower,
            upper,
        } => Plan::Range {
            field: field.clone(),
            lower: lower.clone(),
            upper: upper.clone(),
        },
        Predicate::Exists { field } => Plan::Exists {
            field: field.clone(),
        },
        Predicate::And(children) => Plan::All(optimize_clauses(children)),
        Predicate::Or(children) => Plan::Any(optimize_clauses(children)),
        Predicate::Not(child) => Plan::Not(Box::new(compile_plan(child))),
    }
}

/// Compiles and reorders the clauses of a boolean combinator.
///
/// Clauses are ordered by ascending evaluation cost so existence checks run
/// before value comparisons, with ties broken by descending field-reference
/// frequency so fields shared across more clauses are extracted first. The sort
/// is stable and the children are pure, so reordering never changes the
/// combinator's boolean result.
fn optimize_clauses(children: &[Predicate]) -> Vec<Plan> {
    let mut compiled: Vec<Plan> = children.iter().map(compile_plan).collect();
    let frequency = field_frequency(&compiled);
    compiled.sort_by(|left, right| {
        cost(left)
            .cmp(&cost(right))
            .then_with(|| frequency_rank(right, &frequency).cmp(&frequency_rank(left, &frequency)))
    });
    compiled
}

/// Relative evaluation cost used to order clauses for short-circuiting.
fn cost(plan: &Plan) -> u32 {
    match plan {
        Plan::Exists { .. } => 1,
        Plan::Comparison { .. } => 2,
        Plan::Range { .. } => 3,
        Plan::Not(child) => cost(child),
        Plan::All(children) | Plan::Any(children) => {
            children.iter().map(cost).sum::<u32>().saturating_add(1)
        }
    }
}

/// The field a leaf clause extracts, if it references a single field directly.
fn primary_field(plan: &Plan) -> Option<&FieldPath> {
    match plan {
        Plan::Comparison { field, .. } | Plan::Range { field, .. } | Plan::Exists { field } => {
            Some(field)
        }
        Plan::Not(child) => primary_field(child),
        Plan::All(_) | Plan::Any(_) => None,
    }
}

fn field_key(field: &FieldPath) -> String {
    field.segments().collect::<Vec<_>>().join(".")
}

fn field_frequency(clauses: &[Plan]) -> BTreeMap<String, u32> {
    let mut counts = BTreeMap::new();
    for clause in clauses {
        if let Some(field) = primary_field(clause) {
            *counts.entry(field_key(field)).or_insert(0) += 1;
        }
    }
    counts
}

fn frequency_rank(plan: &Plan, frequency: &BTreeMap<String, u32>) -> u32 {
    primary_field(plan)
        .and_then(|field| frequency.get(&field_key(field)).copied())
        .unwrap_or(0)
}

fn eval_plan(plan: &Plan, accessor: &dyn FieldAccessor) -> bool {
    match plan {
        Plan::Comparison { field, op, value } => accessor
            .field(field)
            .is_some_and(|field_value| compare_values(field_value, *op, value)),
        Plan::Range {
            field,
            lower,
            upper,
        } => accessor.field(field).is_some_and(|field_value| {
            compare_values(field_value, ComparisonOp::Gte, lower)
                && compare_values(field_value, ComparisonOp::Lte, upper)
        }),
        Plan::Exists { field } => accessor.field(field).is_some(),
        Plan::All(children) => children.iter().all(|child| eval_plan(child, accessor)),
        Plan::Any(children) => children.iter().any(|child| eval_plan(child, accessor)),
        Plan::Not(child) => !eval_plan(child, accessor),
    }
}

#[cfg(test)]
mod tests {
    use super::{Plan, compile};
    use crate::routing::evaluate::evaluate;
    use crate::routing::{
        ComparisonOp, FieldAccessor, FieldPath, FieldValue, FieldValueRef, Predicate,
    };

    /// Accessor backed by a fixed set of dot-path/value pairs.
    #[derive(Debug)]
    struct MapAccessor {
        entries: Vec<(&'static str, FieldValueRef<'static>)>,
    }

    impl MapAccessor {
        const fn new(entries: Vec<(&'static str, FieldValueRef<'static>)>) -> Self {
            Self { entries }
        }
    }

    impl FieldAccessor for MapAccessor {
        fn field(&self, path: &FieldPath) -> Option<FieldValueRef<'_>> {
            let key = path.segments().collect::<Vec<_>>().join(".");
            self.entries
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| *value)
        }
    }

    fn comparison(field: &str, op: ComparisonOp, value: FieldValue) -> Predicate {
        Predicate::Comparison {
            field: FieldPath::new(field),
            op,
            value,
        }
    }

    fn exists(field: &str) -> Predicate {
        Predicate::Exists {
            field: FieldPath::new(field),
        }
    }

    fn predicate_corpus() -> Vec<Predicate> {
        let amount_gt = comparison("amount", ComparisonOp::Gt, FieldValue::Integer(1_000));
        let amount_le = comparison("amount", ComparisonOp::Lte, FieldValue::Integer(100));
        let region_eq = comparison(
            "region",
            ComparisonOp::Eq,
            FieldValue::Text(String::from("eu")),
        );
        let flag_eq = comparison("flag", ComparisonOp::Eq, FieldValue::Boolean(true));
        let amount_range = Predicate::Range {
            field: FieldPath::new("amount"),
            lower: FieldValue::Integer(100),
            upper: FieldValue::Integer(1_000),
        };

        vec![
            amount_gt.clone(),
            region_eq.clone(),
            amount_range.clone(),
            exists("region"),
            exists("missing"),
            Predicate::And(vec![exists("region"), amount_gt.clone()]),
            Predicate::Or(vec![amount_le, region_eq.clone()]),
            Predicate::Not(Box::new(amount_gt)),
            Predicate::Not(Box::new(Predicate::Not(Box::new(Predicate::Not(
                Box::new(flag_eq.clone()),
            ))))),
            Predicate::And(Vec::new()),
            Predicate::Or(Vec::new()),
            Predicate::And(vec![
                exists("amount"),
                Predicate::Or(vec![region_eq, flag_eq]),
                amount_range,
            ]),
            comparison(
                "amount",
                ComparisonOp::Eq,
                FieldValue::Text(String::from("x")),
            ),
            comparison("amount", ComparisonOp::Eq, FieldValue::Null),
        ]
    }

    fn accessor_corpus() -> Vec<MapAccessor> {
        vec![
            MapAccessor::new(vec![("amount", FieldValueRef::Integer(1_500))]),
            MapAccessor::new(vec![("amount", FieldValueRef::Integer(50))]),
            MapAccessor::new(vec![("amount", FieldValueRef::Integer(500))]),
            MapAccessor::new(vec![("region", FieldValueRef::Text("eu"))]),
            MapAccessor::new(vec![("region", FieldValueRef::Text("us"))]),
            MapAccessor::new(vec![("flag", FieldValueRef::Boolean(true))]),
            MapAccessor::new(vec![("amount", FieldValueRef::Text("1500"))]),
            MapAccessor::new(Vec::new()),
            MapAccessor::new(vec![
                ("amount", FieldValueRef::Integer(750)),
                ("region", FieldValueRef::Text("eu")),
                ("flag", FieldValueRef::Boolean(false)),
            ]),
            MapAccessor::new(vec![
                ("amount", FieldValueRef::Integer(2_000)),
                ("region", FieldValueRef::Text("us")),
                ("flag", FieldValueRef::Boolean(true)),
            ]),
        ]
    }

    #[test]
    fn compiled_matches_direct_evaluation_for_all_combinations() {
        let predicates = predicate_corpus();
        let accessors = accessor_corpus();
        let mut combinations = 0_usize;

        for predicate in &predicates {
            let compiled = compile(predicate);
            for accessor in &accessors {
                assert_eq!(
                    compiled.evaluate(accessor),
                    evaluate(predicate, accessor),
                    "compiled diverged for {predicate:?}"
                );
                combinations += 1;
            }
        }

        assert!(
            combinations >= 100,
            "expected >=100 combinations, ran {combinations}"
        );
    }

    #[test]
    fn compile_borrows_predicate_unchanged() {
        let predicate = Predicate::And(vec![
            comparison("amount", ComparisonOp::Gt, FieldValue::Integer(10)),
            exists("region"),
        ]);
        let snapshot = predicate.clone();

        let _ = compile(&predicate);

        assert_eq!(predicate, snapshot);
    }

    #[test]
    fn and_places_existence_check_before_comparison() {
        let predicate = Predicate::And(vec![
            comparison("amount", ComparisonOp::Gt, FieldValue::Integer(10)),
            exists("region"),
        ]);

        let compiled = compile(&predicate);

        assert!(matches!(
            &compiled.plan,
            Plan::All(clauses) if matches!(clauses.as_slice(), [Plan::Exists { .. }, ..])
        ));
    }

    #[test]
    fn and_extracts_more_frequent_field_first_among_equal_cost_clauses() {
        let predicate = Predicate::And(vec![
            comparison(
                "region",
                ComparisonOp::Eq,
                FieldValue::Text(String::from("eu")),
            ),
            comparison("amount", ComparisonOp::Gt, FieldValue::Integer(10)),
            comparison("amount", ComparisonOp::Lt, FieldValue::Integer(100)),
        ]);

        let compiled = compile(&predicate);

        assert!(matches!(
            &compiled.plan,
            Plan::All(clauses)
                if matches!(
                    clauses.first(),
                    Some(Plan::Comparison { field, .. }) if field.segments().eq(["amount"])
                )
        ));
    }

    #[test]
    fn reordering_preserves_result_for_existence_and_comparison() {
        let ordered = Predicate::And(vec![
            comparison("amount", ComparisonOp::Gt, FieldValue::Integer(10)),
            exists("region"),
        ]);
        let compiled = compile(&ordered);

        let present = MapAccessor::new(vec![
            ("amount", FieldValueRef::Integer(50)),
            ("region", FieldValueRef::Text("eu")),
        ]);
        let missing_region = MapAccessor::new(vec![("amount", FieldValueRef::Integer(50))]);

        assert!(compiled.evaluate(&present));
        assert!(!compiled.evaluate(&missing_region));
    }
}
