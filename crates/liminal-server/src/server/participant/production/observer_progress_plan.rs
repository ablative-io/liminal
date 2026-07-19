//! Read-only durable-prefix validation and running-maximum repair planning.

use liminal_protocol::wire::DeliverySeq;

use super::observer_progress::{ObserverProgressConformanceError, ObserverProgressSourceWitness};

/// Read-only durable observer state used to construct one mutation plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ObserverProgressPreflight {
    Untracked,
    Tracked(DeliverySeq),
}

/// Complete observer mutation plan, constructed before any append.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct ObserverProgressReconcilePlan {
    track_baseline: bool,
    advances: Vec<DeliverySeq>,
    validated_maximum: DeliverySeq,
}

impl ObserverProgressReconcilePlan {
    pub(super) const fn track_baseline(&self) -> bool {
        self.track_baseline
    }

    pub(super) fn advances(&self) -> &[DeliverySeq] {
        &self.advances
    }

    pub(super) const fn validated_maximum(&self) -> DeliverySeq {
        self.validated_maximum
    }
}

/// Plans Track plus the strict running-maximum suffix without mutating an owner.
pub(super) fn plan_observer_progress_reconcile(
    witnesses: &[ObserverProgressSourceWitness],
    authoritative_maximum: DeliverySeq,
    preflight: ObserverProgressPreflight,
) -> Result<ObserverProgressReconcilePlan, ObserverProgressConformanceError> {
    let mut running_maximum = 0;
    let mut running_maxima = Vec::new();
    for witness in witnesses {
        let progress = witness.progress();
        if progress > running_maximum {
            running_maximum = progress;
            running_maxima.push((witness.merged_ordinal(), progress));
        }
    }
    if running_maximum != authoritative_maximum {
        return Err(ObserverProgressConformanceError::FinalProgressMismatch);
    }

    let (track_baseline, durable_progress) = match preflight {
        ObserverProgressPreflight::Untracked => (true, 0),
        ObserverProgressPreflight::Tracked(progress) => (false, progress),
    };
    if running_maximum == 0 && durable_progress > 0 {
        return Err(ObserverProgressConformanceError::AdvanceWithoutRunningMaximumWitness);
    }
    if durable_progress > running_maximum {
        return Err(ObserverProgressConformanceError::AheadOfValidatedSourceMaximum);
    }

    let suffix_start = if durable_progress == 0 {
        0
    } else {
        running_maxima
            .iter()
            .position(|(_, progress)| *progress == durable_progress)
            .map(|index| index + 1)
            .ok_or(ObserverProgressConformanceError::AdvanceWithoutRunningMaximumWitness)?
    };
    let advances = running_maxima
        .into_iter()
        .skip(suffix_start)
        .map(|(_, progress)| progress)
        .collect();
    Ok(ObserverProgressReconcilePlan {
        track_baseline,
        advances,
        validated_maximum: running_maximum,
    })
}
