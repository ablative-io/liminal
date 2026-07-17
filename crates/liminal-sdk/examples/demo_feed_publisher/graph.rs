use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::jcs::{JcsError, require_canonical, to_jcs_bytes};

const VALUE_MAX: u8 = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Position {
    x: u8,
    y: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Node {
    id: String,
    label: String,
    position: Position,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Edge {
    source: String,
    target: String,
    directed: bool,
    weight: u8,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GraphDelta {
    NodeAdded { node_id: String },
    NodeRemoved { node_id: String },
    ValueChanged { node_id: String, value: u8 },
}

impl GraphDelta {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, GraphError> {
        let value = match self {
            Self::NodeAdded { node_id } => {
                validate_node_id(node_id)?;
                json!({"node": {"id": node_id}, "type": "node-added"})
            }
            Self::NodeRemoved { node_id } => {
                validate_node_id(node_id)?;
                json!({"nodeId": node_id, "type": "node-removed"})
            }
            Self::ValueChanged { node_id, value } => {
                validate_node_id(node_id)?;
                validate_value(*value)?;
                json!({"nodeId": node_id, "type": "value-changed", "value": value})
            }
        };
        Ok(to_jcs_bytes(&value)?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, GraphError> {
        let Value::Object(mut object) = require_canonical(bytes)? else {
            return Err(GraphError::InvalidDelta("delta must be an object"));
        };
        let delta_type = take_string(&mut object, "type")?;
        let delta = match delta_type.as_str() {
            "node-added" => {
                let Value::Object(mut node) = object
                    .remove("node")
                    .ok_or(GraphError::InvalidDelta("node-added requires node"))?
                else {
                    return Err(GraphError::InvalidDelta("node must be an object"));
                };
                let node_id = take_string(&mut node, "id")?;
                if !node.is_empty() {
                    return Err(GraphError::InvalidDelta(
                        "demo node-added carries only an id",
                    ));
                }
                Self::NodeAdded { node_id }
            }
            "node-removed" => Self::NodeRemoved {
                node_id: take_string(&mut object, "nodeId")?,
            },
            "value-changed" => {
                let value = object
                    .remove("value")
                    .and_then(|value| value.as_u64())
                    .ok_or(GraphError::InvalidDelta(
                        "value-changed requires an integer value",
                    ))?;
                Self::ValueChanged {
                    node_id: take_string(&mut object, "nodeId")?,
                    value: u8::try_from(value)
                        .map_err(|_| GraphError::InvalidDelta("value-changed value exceeds u8"))?,
                }
            }
            _ => return Err(GraphError::InvalidDelta("unknown delta type")),
        };
        if !object.is_empty() {
            return Err(GraphError::InvalidDelta("delta has unknown members"));
        }
        match &delta {
            Self::NodeAdded { node_id } | Self::NodeRemoved { node_id } => {
                validate_node_id(node_id)?;
            }
            Self::ValueChanged { node_id, value } => {
                validate_node_id(node_id)?;
                validate_value(*value)?;
            }
        }
        Ok(delta)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("graph state arithmetic exceeded its typed integer bounds")]
    ArithmeticOverflow,
    #[error("node id must match ASCII [a-z0-9-]+, got {0:?}")]
    InvalidNodeId(String),
    #[error("graph-view-value must remain in [0, 100], got {0}")]
    ValueOutOfDomain(u8),
    #[error("graph has no node available for a value pulse")]
    EmptyGraph,
    #[error("delta violates frame:graph-view@v1: {0}")]
    InvalidDelta(&'static str),
    #[error(transparent)]
    Jcs(#[from] JcsError),
}

/// Frame-owned seam for `frame:graph-view@v1` canonical snapshots and deltas.
/// Fractional schema numbers require RFC-8785 float formatting in a follow-up;
/// this demo intentionally emits only integer-valued numbers in `[0, 100]`.
pub trait GraphState {
    fn snapshot_bytes(&self) -> Result<Vec<u8>, GraphError>;
    fn refresh_snapshot_bytes(&mut self) -> Result<Vec<u8>, GraphError>;
    fn advance_delta_bytes(&mut self) -> Result<Vec<u8>, GraphError>;
}

/// Deterministic graph-view authority: value pulses are deltas; graph layout is snapshots.
#[derive(Debug)]
pub struct GraphViewState {
    nodes: BTreeMap<String, Node>,
    edges: BTreeSet<Edge>,
    values: BTreeMap<String, u8>,
    tick: u64,
    refresh: u64,
}

impl GraphViewState {
    pub fn new() -> Result<Self, GraphError> {
        let nodes = [
            node("core", "Core", 15, 50)?,
            node("relay", "Relay", 50, 20)?,
            node("shell", "Shell", 85, 50)?,
        ]
        .into_iter()
        .map(|node| (node.id.clone(), node))
        .collect();
        let edges = BTreeSet::from([
            edge("core", "relay", 2, false)?,
            edge("relay", "shell", 3, true)?,
        ]);
        let values = BTreeMap::from([
            ("core".to_owned(), 20),
            ("relay".to_owned(), 50),
            ("shell".to_owned(), 80),
        ]);
        Ok(Self {
            nodes,
            edges,
            values,
            tick: 0,
            refresh: 0,
        })
    }

    fn refresh_layout(&mut self) -> Result<(), GraphError> {
        self.refresh = self
            .refresh
            .checked_add(1)
            .ok_or(GraphError::ArithmeticOverflow)?;
        for (index, node) in self.nodes.values_mut().enumerate() {
            let index = u64::try_from(index).map_err(|_| GraphError::ArithmeticOverflow)?;
            node.position = Position {
                x: u8::try_from((17 + index * 31 + self.refresh * 7) % 101)
                    .map_err(|_| GraphError::ArithmeticOverflow)?,
                y: u8::try_from((61 + index * 23 + self.refresh * 11) % 101)
                    .map_err(|_| GraphError::ArithmeticOverflow)?,
            };
        }

        let node_number = self
            .refresh
            .checked_add(3)
            .ok_or(GraphError::ArithmeticOverflow)?;
        let node_id = format!("node-{node_number}");
        let new_node = node(
            &node_id,
            &format!("Node {node_number}"),
            u8::try_from((self.refresh * 19) % 101).map_err(|_| GraphError::ArithmeticOverflow)?,
            u8::try_from((self.refresh * 37) % 101).map_err(|_| GraphError::ArithmeticOverflow)?,
        )?;
        let previous_id = if node_number == 4 {
            "shell".to_owned()
        } else {
            format!("node-{}", node_number - 1)
        };
        self.nodes.insert(node_id.clone(), new_node);
        self.values.insert(node_id.clone(), 50);
        self.edges.insert(edge(
            &previous_id,
            &node_id,
            u8::try_from(self.refresh % 4 + 1).map_err(|_| GraphError::ArithmeticOverflow)?,
            self.refresh % 2 == 0,
        )?);
        Ok(())
    }
}

impl GraphState for GraphViewState {
    fn snapshot_bytes(&self) -> Result<Vec<u8>, GraphError> {
        let nodes: Vec<_> = self.nodes.values().map(node_value).collect();
        let edges: Vec<_> = self.edges.iter().map(edge_value).collect();
        let values: Vec<_> = self
            .values
            .iter()
            .map(|(node_id, value)| json!({"nodeId": node_id, "value": value}))
            .collect();
        Ok(to_jcs_bytes(&json!({
            "graph": {"edges": edges, "format": "graph-input", "nodes": nodes},
            "values": values,
        }))?)
    }

    fn refresh_snapshot_bytes(&mut self) -> Result<Vec<u8>, GraphError> {
        self.refresh_layout()?;
        self.snapshot_bytes()
    }

    fn advance_delta_bytes(&mut self) -> Result<Vec<u8>, GraphError> {
        self.tick = self
            .tick
            .checked_add(1)
            .ok_or(GraphError::ArithmeticOverflow)?;
        let node_count = self.values.len();
        if node_count == 0 {
            return Err(GraphError::EmptyGraph);
        }
        let index = usize::try_from(
            self.tick % u64::try_from(node_count).map_err(|_| GraphError::ArithmeticOverflow)?,
        )
        .map_err(|_| GraphError::ArithmeticOverflow)?;
        let node_id = self
            .values
            .keys()
            .nth(index)
            .cloned()
            .ok_or(GraphError::EmptyGraph)?;
        let phase = u8::try_from(self.tick % 201).map_err(|_| GraphError::ArithmeticOverflow)?;
        let value = if phase <= VALUE_MAX {
            phase
        } else {
            200_u8
                .checked_sub(phase)
                .ok_or(GraphError::ArithmeticOverflow)?
        };
        self.values.insert(node_id.clone(), value);
        let delta = GraphDelta::ValueChanged { node_id, value };
        let bytes = delta.canonical_bytes()?;
        if GraphDelta::decode(&bytes)? != delta {
            return Err(GraphError::InvalidDelta("delta round-trip mismatch"));
        }
        Ok(bytes)
    }
}

fn node(id: &str, label: &str, x: u8, y: u8) -> Result<Node, GraphError> {
    validate_node_id(id)?;
    validate_value(x)?;
    validate_value(y)?;
    Ok(Node {
        id: id.to_owned(),
        label: label.to_owned(),
        position: Position { x, y },
    })
}

fn edge(source: &str, target: &str, weight: u8, directed: bool) -> Result<Edge, GraphError> {
    validate_node_id(source)?;
    validate_node_id(target)?;
    validate_value(weight)?;
    Ok(Edge {
        source: source.to_owned(),
        target: target.to_owned(),
        directed,
        weight,
    })
}

fn validate_node_id(id: &str) -> Result<(), GraphError> {
    if !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        Ok(())
    } else {
        Err(GraphError::InvalidNodeId(id.to_owned()))
    }
}

const fn validate_value(value: u8) -> Result<(), GraphError> {
    if value <= VALUE_MAX {
        Ok(())
    } else {
        Err(GraphError::ValueOutOfDomain(value))
    }
}

fn node_value(node: &Node) -> Value {
    json!({
        "id": node.id,
        "label": node.label,
        "x": node.position.x,
        "y": node.position.y,
    })
}

fn edge_value(edge: &Edge) -> Value {
    json!({
        "directed": edge.directed,
        "source": edge.source,
        "target": edge.target,
        "weight": edge.weight,
    })
}

fn take_string(
    object: &mut serde_json::Map<String, Value>,
    key: &'static str,
) -> Result<String, GraphError> {
    object
        .remove(key)
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .ok_or(GraphError::InvalidDelta("required string member is absent"))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{GraphDelta, GraphState, GraphViewState};
    use crate::jcs::require_canonical;

    #[test]
    fn snapshot_matches_graph_view_shape() -> Result<(), Box<dyn std::error::Error>> {
        let graph = GraphViewState::new()?;
        let snapshot = require_canonical(&graph.snapshot_bytes()?)?;
        assert_eq!(snapshot["graph"]["format"], "graph-input");
        assert!(snapshot["graph"]["nodes"].is_array());
        assert!(snapshot["graph"]["edges"].is_array());
        assert!(snapshot["values"].is_array());
        Ok(())
    }

    #[test]
    fn delta_family_is_exact_and_contains_no_edge_or_position_update()
    -> Result<(), Box<dyn std::error::Error>> {
        let cases = [
            (
                GraphDelta::NodeAdded {
                    node_id: "new-node".to_owned(),
                },
                br#"{"node":{"id":"new-node"},"type":"node-added"}"#.as_slice(),
            ),
            (
                GraphDelta::NodeRemoved {
                    node_id: "old-node".to_owned(),
                },
                br#"{"nodeId":"old-node","type":"node-removed"}"#.as_slice(),
            ),
            (
                GraphDelta::ValueChanged {
                    node_id: "core".to_owned(),
                    value: 9,
                },
                br#"{"nodeId":"core","type":"value-changed","value":9}"#.as_slice(),
            ),
        ];
        let mut types = BTreeSet::new();
        for (delta, expected) in cases {
            let bytes = delta.canonical_bytes()?;
            assert_eq!(bytes, expected);
            let parsed = require_canonical(&bytes)?;
            types.insert(
                parsed["type"]
                    .as_str()
                    .ok_or("delta type must be a string")?
                    .to_owned(),
            );
            assert!(parsed.get("edges").is_none());
            assert!(parsed.get("x").is_none());
            assert!(parsed.get("y").is_none());
            if parsed["type"] == "node-added" {
                assert!(parsed.get("value").is_none());
            }
        }
        assert_eq!(
            types,
            BTreeSet::from([
                "node-added".to_owned(),
                "node-removed".to_owned(),
                "value-changed".to_owned(),
            ])
        );
        Ok(())
    }

    #[test]
    fn per_tick_deltas_are_value_pulses_in_domain() -> Result<(), Box<dyn std::error::Error>> {
        let mut graph = GraphViewState::new()?;
        let before = require_canonical(&graph.snapshot_bytes()?)?;
        for _ in 0..250 {
            let delta = require_canonical(&graph.advance_delta_bytes()?)?;
            assert_eq!(delta["type"], "value-changed");
            let value = delta["value"]
                .as_u64()
                .ok_or("value pulse must be an integer")?;
            assert!(value <= 100);
        }
        let after = require_canonical(&graph.snapshot_bytes()?)?;
        assert_eq!(before["graph"], after["graph"]);
        Ok(())
    }

    #[test]
    fn refresh_changes_graph_only_through_snapshot() -> Result<(), Box<dyn std::error::Error>> {
        let mut graph = GraphViewState::new()?;
        let before = require_canonical(&graph.snapshot_bytes()?)?;
        let refreshed = require_canonical(&graph.refresh_snapshot_bytes()?)?;
        assert_ne!(before["graph"]["nodes"], refreshed["graph"]["nodes"]);
        assert_ne!(before["graph"]["edges"], refreshed["graph"]["edges"]);
        assert!(
            refreshed["graph"]["nodes"]
                .as_array()
                .ok_or("nodes must be an array")?
                .len()
                > before["graph"]["nodes"]
                    .as_array()
                    .ok_or("nodes must be an array")?
                    .len()
        );
        Ok(())
    }
}
