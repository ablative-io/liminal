use std::time::Duration;

use liminal_sdk::{OBSERVABILITY_CHANNEL, PushWriter, SdkError};

use crate::authority::{AuthorityError, Cut, FeedAuthority};
use crate::envelope::{
    ComponentId, ContractId, EnvelopeCodec, EnvelopeError, EnvelopeHeader, FrameKind,
};
use crate::graph::{GraphError, GraphState};

/// Demo-only content pacing: four updates per second keeps motion legible without
/// making this wall clock a protocol, authority, reconnect, retry, or delivery timer.
pub const TICK_INTERVAL: Duration = Duration::from_millis(250);

/// Replace the whole graph after twenty deltas: at four updates per second a new
/// generation baseline bounds passive demo resynchronization to roughly five seconds.
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

/// Deterministic cut and snapshot/delta policy; it owns no wall clock or reconnect policy.
/// A standalone authority-snapshot request is the named post-demo resync mechanism.
pub struct CadenceEngine<C, G, S> {
    channel: String,
    component_id: ComponentId,
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
        component_id: ComponentId,
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
        let contract = ContractId::new("frame", "graph-view", 1)?;
        Ok(Self {
            channel,
            component_id,
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
        let cut = self.authority.mint_baseline(&self.channel)?;
        let outcome = self.publish(FrameKind::Snapshot, cut, &state)?;
        self.started = true;
        Ok(outcome)
    }

    pub fn emit_tick(&mut self) -> Result<PublishOutcome, CadenceError> {
        if !self.started {
            return Err(CadenceError::NotStarted);
        }
        let delta = self.graph.advance_delta_bytes()?;
        let cut = self.authority.mint_delta(&self.channel)?;
        let delta_outcome = self.publish(FrameKind::Delta, cut, &delta)?;
        self.deltas_since_snapshot += 1;
        if self.deltas_since_snapshot == self.snapshot_period {
            let snapshot = self.graph.refresh_snapshot_bytes()?;
            self.authority.advance_generation()?;
            let baseline = self.authority.mint_baseline(&self.channel)?;
            let snapshot_outcome = self.publish(FrameKind::Snapshot, baseline, &snapshot)?;
            self.deltas_since_snapshot = 0;
            Ok(snapshot_outcome)
        } else {
            Ok(delta_outcome)
        }
    }

    fn publish(
        &mut self,
        kind: FrameKind,
        cut: Cut,
        state: &[u8],
    ) -> Result<PublishOutcome, CadenceError> {
        let header = EnvelopeHeader {
            component_id: self.component_id.clone(),
            contract_id: self.contract.clone(),
            generation: cut.generation().get(),
            kind,
            seq: cut.seq(),
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
    use std::cell::Cell;
    use std::rc::Rc;

    use super::{
        CadenceEngine, CadenceError, PublishError, PublishOutcome, PublishSink, SNAPSHOT_PERIOD,
    };
    use crate::authority::{FeedAuthority, GenerationStore, GenerationStoreError};
    use crate::envelope::{ComponentId, EnvelopeCodec, FrameEnvelopeCodec, FrameKind};
    use crate::graph::GraphViewState;

    #[derive(Clone, Default)]
    struct MemoryGenerationStore(Rc<Cell<Option<u64>>>);

    impl GenerationStore for MemoryGenerationStore {
        fn load(&mut self) -> Result<Option<u64>, GenerationStoreError> {
            Ok(self.0.get())
        }

        fn persist(&mut self, generation: u64) -> Result<(), GenerationStoreError> {
            self.0.set(Some(generation));
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
        CadenceEngine<FrameEnvelopeCodec, GraphViewState, FakeSink>,
        Box<dyn std::error::Error>,
    > {
        Ok(CadenceEngine::new(
            "frame.demo.graph-view".to_owned(),
            ComponentId::new("graph-view-demo")?,
            FeedAuthority::start(MemoryGenerationStore::default())?,
            FrameEnvelopeCodec,
            GraphViewState::new()?,
            sink,
            period,
        )?)
    }

    #[test]
    fn baseline_snapshot_precedes_monotonic_deltas() -> Result<(), Box<dyn std::error::Error>> {
        let mut cadence = engine(FakeSink::default(), SNAPSHOT_PERIOD)?;
        cadence.emit_initial_snapshot()?;
        cadence.emit_tick()?;
        let first = FrameEnvelopeCodec.decode(&cadence.sink.frames[0])?;
        let second = FrameEnvelopeCodec.decode(&cadence.sink.frames[1])?;
        assert_eq!(first.header.kind, FrameKind::Snapshot);
        assert_eq!(second.header.kind, FrameKind::Delta);
        assert_eq!((first.header.generation, first.header.seq), (1, 0));
        assert_eq!((second.header.generation, second.header.seq), (1, 1));
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
    fn periodic_snapshot_bumps_generation_and_resets_sequence()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut cadence = engine(FakeSink::default(), SNAPSHOT_PERIOD)?;
        cadence.emit_initial_snapshot()?;
        for _ in 0..SNAPSHOT_PERIOD {
            cadence.emit_tick()?;
        }
        let headers: Result<Vec<_>, _> = cadence
            .sink
            .frames
            .iter()
            .map(|frame| {
                FrameEnvelopeCodec
                    .decode(frame)
                    .map(|decoded| decoded.header)
            })
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
        assert_eq!(
            headers
                .first()
                .map(|header| (header.generation, header.seq)),
            Some((1, 0))
        );
        assert_eq!(
            headers.last().map(|header| (header.generation, header.seq)),
            Some((2, 0))
        );
        assert!(
            headers[1..=SNAPSHOT_PERIOD]
                .iter()
                .enumerate()
                .all(|(index, header)| {
                    header.kind == FrameKind::Delta
                        && header.generation == 1
                        && usize::try_from(header.seq) == Ok(index + 1)
                })
        );
        let cuts: Vec<_> = headers
            .iter()
            .map(|header| (header.generation, header.seq))
            .collect();
        assert!(cuts.windows(2).all(|pair| pair[0] < pair[1]));
        Ok(())
    }

    #[test]
    fn sink_failure_is_typed_and_its_cut_is_not_reused() -> Result<(), Box<dyn std::error::Error>> {
        let sink = FakeSink {
            frames: Vec::new(),
            channels: Vec::new(),
            fail_at: Some(1),
        };
        let mut cadence = engine(sink, SNAPSHOT_PERIOD)?;
        cadence.emit_initial_snapshot()?;
        let error = cadence.emit_tick();
        assert!(matches!(error, Err(CadenceError::Publish(_))));
        cadence.sink.fail_at = None;
        cadence.emit_tick()?;
        let after_gap = FrameEnvelopeCodec.decode(&cadence.sink.frames[1])?;
        assert_eq!((after_gap.header.generation, after_gap.header.seq), (1, 2));
        Ok(())
    }

    #[test]
    fn reserved_observability_channel_is_refused() -> Result<(), Box<dyn std::error::Error>> {
        let result = CadenceEngine::new(
            liminal_sdk::OBSERVABILITY_CHANNEL.to_owned(),
            ComponentId::new("graph-view-demo")?,
            FeedAuthority::start(MemoryGenerationStore::default())?,
            FrameEnvelopeCodec,
            GraphViewState::new()?,
            FakeSink::default(),
            SNAPSHOT_PERIOD,
        );
        assert!(matches!(result, Err(CadenceError::ReservedChannel(_))));
        Ok(())
    }
}
