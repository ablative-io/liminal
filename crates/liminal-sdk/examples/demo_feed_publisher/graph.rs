use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::jcs::{JcsError, to_jcs_bytes};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Position {
    x: i64,
    y: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct Edge {
    from: String,
    to: String,
}

#[derive(Debug, thiserror::Error)]
pub enum GraphError {
    #[error("graph state arithmetic exceeded its typed integer bounds")]
    ArithmeticOverflow,
    #[error(transparent)]
    Jcs(#[from] JcsError),
}

/// Frame-owned seam for canonical pure-state snapshots and deltas.
///
/// `DemoGraph` is the placeholder GraphMother-shaped binding. Replace that type's
/// implementation in this module when the frame fake-feed schema is committed;
/// the cadence, envelope, authority, and transport layers do not need to change.
pub trait GraphState {
    fn snapshot_bytes(&self) -> Result<Vec<u8>, GraphError>;
    fn advance_delta_bytes(&mut self) -> Result<Vec<u8>, GraphError>;
}

/// Deterministic, visibly alive placeholder graph with stable ASCII node ids.
#[derive(Debug)]
pub struct DemoGraph {
    nodes: BTreeMap<String, Position>,
    edges: BTreeSet<Edge>,
    tick: u64,
}

impl DemoGraph {
    #[must_use]
    pub fn new() -> Self {
        let nodes = BTreeMap::from([
            ("node-1".to_owned(), Position { x: -80, y: 0 }),
            ("node-2".to_owned(), Position { x: 0, y: 60 }),
            ("node-3".to_owned(), Position { x: 80, y: 0 }),
        ]);
        let edges = BTreeSet::from([
            Edge {
                from: "node-1".to_owned(),
                to: "node-2".to_owned(),
            },
            Edge {
                from: "node-2".to_owned(),
                to: "node-3".to_owned(),
            },
        ]);
        Self {
            nodes,
            edges,
            tick: 0,
        }
    }
}

impl GraphState for DemoGraph {
    fn snapshot_bytes(&self) -> Result<Vec<u8>, GraphError> {
        let nodes: Vec<_> = self
            .nodes
            .iter()
            .map(|(id, position)| node_value(id, *position))
            .collect();
        let edges: Vec<_> = self.edges.iter().map(edge_value).collect();
        Ok(to_jcs_bytes(&json!({"edges": edges, "nodes": nodes}))?)
    }

    fn advance_delta_bytes(&mut self) -> Result<Vec<u8>, GraphError> {
        self.tick = self
            .tick
            .checked_add(1)
            .ok_or(GraphError::ArithmeticOverflow)?;

        let mut moved_nodes = Vec::with_capacity(self.nodes.len());
        for (index, (id, position)) in self.nodes.iter_mut().enumerate() {
            let direction = if index % 2 == 0 { 1 } else { -1 };
            position.x = position
                .x
                .checked_add(direction * 3)
                .ok_or(GraphError::ArithmeticOverflow)?;
            let index = u64::try_from(index).map_err(|_| GraphError::ArithmeticOverflow)?;
            let vertical_phase = self
                .tick
                .checked_add(index)
                .ok_or(GraphError::ArithmeticOverflow)?;
            let vertical_step = if vertical_phase % 2 == 0 { 2 } else { -2 };
            position.y = position
                .y
                .checked_add(vertical_step)
                .ok_or(GraphError::ArithmeticOverflow)?;
            moved_nodes.push(node_value(id, *position));
        }

        let node_number = u64::try_from(self.nodes.len())
            .map_err(|_| GraphError::ArithmeticOverflow)?
            .checked_add(1)
            .ok_or(GraphError::ArithmeticOverflow)?;
        let new_id = format!("node-{node_number}");
        let new_position = Position {
            x: i64::try_from((self.tick % 181) * 29 % 181)
                .map_err(|_| GraphError::ArithmeticOverflow)?
                - 90,
            y: i64::try_from((self.tick % 141) * 47 % 141)
                .map_err(|_| GraphError::ArithmeticOverflow)?
                - 70,
        };
        let previous_number = node_number
            .checked_sub(1)
            .ok_or(GraphError::ArithmeticOverflow)?;
        let previous_id = format!("node-{previous_number}");
        let new_edge = Edge {
            from: previous_id,
            to: new_id.clone(),
        };
        self.nodes.insert(new_id.clone(), new_position);
        self.edges.insert(new_edge.clone());

        Ok(to_jcs_bytes(&json!({
            "added-edges": [edge_value(&new_edge)],
            "added-nodes": [node_value(&new_id, new_position)],
            "moved-nodes": moved_nodes,
        }))?)
    }
}

fn node_value(id: &str, position: Position) -> Value {
    json!({"id": id, "x": position.x, "y": position.y})
}

fn edge_value(edge: &Edge) -> Value {
    json!({"from": edge.from, "to": edge.to})
}

#[cfg(test)]
mod tests {
    use super::{DemoGraph, GraphState};
    use crate::jcs::require_canonical;

    #[test]
    fn generator_moves_positions_and_adds_nodes() -> Result<(), Box<dyn std::error::Error>> {
        let mut graph = DemoGraph::new();
        let before = require_canonical(&graph.snapshot_bytes()?)?;
        let delta = require_canonical(&graph.advance_delta_bytes()?)?;
        let after = require_canonical(&graph.snapshot_bytes()?)?;

        let before_nodes = before["nodes"]
            .as_array()
            .ok_or("snapshot nodes must be an array")?;
        let after_nodes = after["nodes"]
            .as_array()
            .ok_or("snapshot nodes must be an array")?;
        let moved_nodes = delta["moved-nodes"]
            .as_array()
            .ok_or("delta moved-nodes must be an array")?;
        assert_eq!(after_nodes.len(), before_nodes.len() + 1);
        assert!(!moved_nodes.is_empty());
        assert_ne!(before_nodes[0]["x"], after_nodes[0]["x"]);
        Ok(())
    }
}
