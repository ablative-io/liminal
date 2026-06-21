use super::{
    RecordedSignalDelivery, SignalDeliverer, SignalOperation, SignalPayload, SignalRecorder,
};
use crate::aion::AionSurfaceError;
use crate::conversation::ParticipantPid;

#[derive(Debug)]
pub(super) struct NoopSignalDeliverer;

impl SignalDeliverer for NoopSignalDeliverer {
    fn deliver(
        &self,
        workflow_pid: ParticipantPid,
        signal: SignalPayload,
    ) -> Result<(), AionSurfaceError> {
        let _ = (workflow_pid, signal);
        Ok(())
    }
}

#[derive(Debug)]
pub(super) struct NoopSignalRecorder;

impl SignalRecorder for NoopSignalRecorder {
    fn replay_deliveries(
        &self,
        channel_name: &str,
        workflow_id: &str,
    ) -> Result<Vec<RecordedSignalDelivery>, AionSurfaceError> {
        let _ = (channel_name, workflow_id);
        Ok(Vec::new())
    }

    fn record(&self, operation: SignalOperation) -> Result<(), AionSurfaceError> {
        let _ = operation;
        Ok(())
    }
}
