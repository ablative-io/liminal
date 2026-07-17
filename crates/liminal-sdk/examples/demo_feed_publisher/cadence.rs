use std::time::Duration;

use liminal_sdk::{OBSERVABILITY_CHANNEL, PushWriter, SdkError};

use crate::authority::{AuthorityError, FeedAuthority};
use crate::envelope::{ContractId, EnvelopeCodec, EnvelopeError, EnvelopeHeader, FrameKind};
use crate::graph::{GraphError, GraphState};

/// Demo-only content pacing: four updates per second keeps motion legible without
/// making this wall clock a protocol, authority, reconnect, retry, or delivery timer.
pub const TICK_INTERVAL: Duration = Duration::from_millis(250);

/// Re-emit a full snapshot after twenty successful deltas, bounding a late joiner's
/// demo resynchronization wait to roughly five seconds at `TICK_INTERVAL`.
pub const SNAPSHOT_PERIOD: usize = 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The opaque publish frame was written; this is not a delivery acknowledgement.
    Written,
}

#[derive(Debug, thiserror::Error)]
pub enum PublishError {
    #[error("SDK opaque publish failed: {0}")]
    Sdk(#[from] SdkError),
}

pub trait PublishSink {
    fn publish(&mut self, channel: &str, payload: Vec<u8>) -> Result<PublishOutcome, PublishError>;
}

impl PublishSink for PushWriter {
    fn publish(&mut self, channel: &str, payload: Vec<u8>) -> Result<PublishOutcome, PublishError> {
        Self::publish(self, channel, payload)?;
        Ok(PublishOutcome::Written)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CadenceError {
    #[error("demo feed channel cannot be empty")]
    EmptyChannel,
    #[error("demo feed must not publish to reserved observability channel {0:?}")]
    ReservedChannel(&'static str),
    #[error("snapshot period must be greater than zero")]
    ZeroSnapshotPeriod,
    #[error("initial snapshot has already been emitted")]
    AlreadyStarted,
    #[error("initial snapshot must be emitted before a delta")]
    NotStarted,
    #[error(transparent)]
    Authority(#[from] AuthorityError),
    #[error(transparent)]
    Envelope(#[from] EnvelopeError),
    #[error(transparent)]
    Graph(#[from] GraphError),
    #[error(transparent)]
    Publish(#[from] PublishError),
    #[error("encoded envelope failed its own round-trip invariant")]
    EncodedFrameMismatch,
}

/// Deterministic snapshot/delta policy; it owns no wall clock or reconnect policy.
/// A standalone resync-request input is the named post-demo alternative.
pub struct CadenceEngine<C, G, S> {
    channel: String,
    contract: ContractId,
    authority: FeedAuthority,
    codec: C,
    graph: G,
    sink: S,
    snapshot_period: usize,
    deltas_since_snapshot: usize,
    started: bool,
}

impl<C, G, S> CadenceEngine<C, G, S>
where
    C: EnvelopeCodec,
    G: GraphState,
    S: PublishSink,
{
    pub fn new(
        channel: String,
        contract: ContractId,
        authority: FeedAuthority,
        codec: C,
        graph: G,
        sink: S,
        snapshot_period: usize,
    ) -> Result<Self, CadenceError> {
        if channel.is_empty() {
            return Err(CadenceError::EmptyChannel);
        }
        if channel == OBSERVABILITY_CHANNEL {
            return Err(CadenceError::ReservedChannel(OBSERVABILITY_CHANNEL));
        }
        if snapshot_period == 0 {
            return Err(CadenceError::ZeroSnapshotPeriod);
        }
        Ok(Self {
            channel,
            contract,
            authority,
            codec,
            graph,
            sink,
            snapshot_period,
            deltas_since_snapshot: 0,
            started: false,
        })
    }

    pub fn emit_initial_snapshot(&mut self) -> Result<PublishOutcome, CadenceError> {
        if self.started {
            return Err(CadenceError::AlreadyStarted);
        }
        let state = self.graph.snapshot_bytes()?;
        let outcome = self.publish(FrameKind::Snapshot, &state)?;
        self.started = true;
        Ok(outcome)
    }

    pub fn emit_tick(&mut self) -> Result<PublishOutcome, CadenceError> {
        if !self.started {
            return Err(CadenceError::NotStarted);
        }
        let delta = self.graph.advance_delta_bytes()?;
        let delta_outcome = self.publish(FrameKind::Delta, &delta)?;
        self.deltas_since_snapshot += 1;
        if self.deltas_since_snapshot == self.snapshot_period {
            let snapshot = self.graph.snapshot_bytes()?;
            let snapshot_outcome = self.publish(FrameKind::Snapshot, &snapshot)?;
            self.deltas_since_snapshot = 0;
            Ok(snapshot_outcome)
        } else {
            Ok(delta_outcome)
        }
    }

    fn publish(&mut self, kind: FrameKind, state: &[u8]) -> Result<PublishOutcome, CadenceError> {
        let sequence = self.authority.next_sequence(&self.channel)?;
        let header = EnvelopeHeader {
            contract_id: self.contract.clone(),
            generation: self.authority.generation().get(),
            seq: sequence.get(),
            kind,
        };
        let payload = self.codec.encode(&header, state)?;
        let decoded = self.codec.decode(&payload)?;
        if decoded.header != header || decoded.state != state {
            return Err(CadenceError::EncodedFrameMismatch);
        }
        Ok(self.sink.publish(&self.channel, payload)?)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CadenceEngine, CadenceError, PublishError, PublishOutcome, PublishSink, SNAPSHOT_PERIOD,
    };
    use crate::authority::{FeedAuthority, GenerationStore, GenerationStoreError};
    use crate::envelope::{ContractId, EnvelopeCodec, FrameKind, PlaceholderEnvelopeCodec};
    use crate::graph::DemoGraph;

    #[derive(Default)]
    struct MemoryGenerationStore(Option<u64>);

    impl GenerationStore for MemoryGenerationStore {
        fn load(&mut self) -> Result<Option<u64>, GenerationStoreError> {
            Ok(self.0)
        }

        fn persist(&mut self, generation: u64) -> Result<(), GenerationStoreError> {
            self.0 = Some(generation);
            Ok(())
        }
    }

    #[derive(Default)]
    struct FakeSink {
        frames: Vec<Vec<u8>>,
        channels: Vec<String>,
        fail_at: Option<usize>,
    }

    impl PublishSink for FakeSink {
        fn publish(
            &mut self,
            channel: &str,
            payload: Vec<u8>,
        ) -> Result<PublishOutcome, PublishError> {
            if self.fail_at == Some(self.frames.len()) {
                return Err(PublishError::Sdk(liminal_sdk::SdkError::Connection {
                    description: "injected sink failure".to_owned(),
                }));
            }
            self.channels.push(channel.to_owned());
            self.frames.push(payload);
            Ok(PublishOutcome::Written)
        }
    }

    fn engine(
        sink: FakeSink,
        period: usize,
    ) -> Result<
        CadenceEngine<PlaceholderEnvelopeCodec, DemoGraph, FakeSink>,
        Box<dyn std::error::Error>,
    > {
        let mut store = MemoryGenerationStore::default();
        Ok(CadenceEngine::new(
            "frame.demo.graph-view".to_owned(),
            ContractId::new("frame", "graph-view", 1)?,
            FeedAuthority::start(&mut store)?,
            PlaceholderEnvelopeCodec,
            DemoGraph::new(),
            sink,
            period,
        )?)
    }

    #[test]
    fn initial_snapshot_precedes_deltas_and_sequences_advance()
    -> Result<(), Box<dyn std::error::Error>> {
        let codec = PlaceholderEnvelopeCodec;
        let mut cadence = engine(FakeSink::default(), 20)?;
        cadence.emit_initial_snapshot()?;
        cadence.emit_tick()?;
        let first = codec.decode(&cadence.sink.frames[0])?;
        let second = codec.decode(&cadence.sink.frames[1])?;
        assert_eq!(first.header.kind, FrameKind::Snapshot);
        assert_eq!(second.header.kind, FrameKind::Delta);
        assert_eq!((first.header.seq, second.header.seq), (1, 2));
        assert_eq!(first.header.generation, second.header.generation);
        assert!(
            cadence
                .sink
                .channels
                .iter()
                .all(|channel| channel == "frame.demo.graph-view")
        );
        Ok(())
    }

    #[test]
    fn periodic_snapshot_follows_exact_delta_period() -> Result<(), Box<dyn std::error::Error>> {
        let codec = PlaceholderEnvelopeCodec;
        let mut cadence = engine(FakeSink::default(), SNAPSHOT_PERIOD)?;
        cadence.emit_initial_snapshot()?;
        for _ in 0..SNAPSHOT_PERIOD {
            cadence.emit_tick()?;
        }
        let headers: Result<Vec<_>, _> = cadence
            .sink
            .frames
            .iter()
            .map(|frame| codec.decode(frame).map(|decoded| decoded.header))
            .collect();
        let headers = headers?;
        assert_eq!(headers.len(), SNAPSHOT_PERIOD + 2);
        assert_eq!(
            headers.first().map(|header| header.kind),
            Some(FrameKind::Snapshot)
        );
        assert_eq!(
            headers.last().map(|header| header.kind),
            Some(FrameKind::Snapshot)
        );
        assert!(
            headers[1..=SNAPSHOT_PERIOD]
                .iter()
                .all(|header| header.kind == FrameKind::Delta)
        );
        let expected_sequences: Result<Vec<_>, _> =
            (1..=headers.len()).map(u64::try_from).collect();
        let sequences: Vec<_> = headers.iter().map(|header| header.seq).collect();
        assert_eq!(sequences, expected_sequences?);
        Ok(())
    }

    #[test]
    fn sink_failure_is_a_typed_error() -> Result<(), Box<dyn std::error::Error>> {
        let sink = FakeSink {
            frames: Vec::new(),
            channels: Vec::new(),
            fail_at: Some(0),
        };
        let mut cadence = engine(sink, 20)?;
        let error = cadence.emit_initial_snapshot();
        assert!(matches!(error, Err(CadenceError::Publish(_))));
        Ok(())
    }

    #[test]
    fn reserved_observability_channel_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let mut store = MemoryGenerationStore::default();
        let result = CadenceEngine::new(
            liminal_sdk::OBSERVABILITY_CHANNEL.to_owned(),
            ContractId::new("frame", "graph-view", 1)?,
            FeedAuthority::start(&mut store)?,
            PlaceholderEnvelopeCodec,
            DemoGraph::new(),
            FakeSink::default(),
            20,
        );
        assert!(matches!(result, Err(CadenceError::ReservedChannel(_))));
        Ok(())
    }
}
