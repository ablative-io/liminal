#![allow(clippy::expect_used, clippy::panic)]

use crate::wire::{
    AttachAttemptToken, AttachEnvelope, BindingEpoch, ConnectionIncarnation, DetachAttemptToken,
    DetachEnvelope, EnrollmentEnvelope, EnrollmentToken, Generation, LeaveAttemptToken,
    LeaveEnvelope, ObserverBackpressure, RecordAdmissionEnvelope,
};

use super::observer::{ObserverCheckedOperation, ObserverFloorDecision, check_observer_floor};

const OBSERVER_PROGRESS: u64 = 40;
const BLOCKING_FLOOR: u128 = 42;

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("test generation is nonzero")
}

fn binding_epoch() -> BindingEpoch {
    BindingEpoch::new(ConnectionIncarnation::new(7, 9), generation(3))
}

fn enrollment() -> EnrollmentEnvelope {
    EnrollmentEnvelope {
        conversation_id: 11,
        enrollment_token: EnrollmentToken::new([0x11; 16]),
    }
}

fn attach() -> AttachEnvelope {
    AttachEnvelope {
        conversation_id: 12,
        participant_id: 2,
        capability_generation: generation(3),
        attach_attempt_token: AttachAttemptToken::new([0x12; 16]),
        accept_marker_delivery_seq: Some(30),
    }
}

fn detach() -> DetachEnvelope {
    DetachEnvelope {
        conversation_id: 13,
        participant_id: 3,
        capability_generation: generation(4),
        detach_attempt_token: DetachAttemptToken::new([0x13; 16]),
    }
}

fn leave() -> LeaveEnvelope {
    LeaveEnvelope {
        conversation_id: 14,
        participant_id: 4,
        capability_generation: generation(5),
        leave_attempt_token: LeaveAttemptToken::new([0x14; 16]),
    }
}

fn record() -> RecordAdmissionEnvelope {
    RecordAdmissionEnvelope {
        conversation_id: 15,
        participant_id: 5,
        capability_generation: generation(6),
        record_admission_attempt_token: crate::wire::RecordAdmissionAttemptToken::new([0xA7; 16]),
    }
}

fn refused(operation: ObserverCheckedOperation) -> ObserverBackpressure {
    match check_observer_floor(operation, OBSERVER_PROGRESS, BLOCKING_FLOOR) {
        ObserverFloorDecision::Respond(response) => response,
        ObserverFloorDecision::Eligible(_) => panic!("strict excess must refuse"),
    }
}

fn assert_state(state: crate::wire::ObserverBackpressureState) {
    assert_eq!(state.backpressure_epoch(), OBSERVER_PROGRESS);
    assert_eq!(state.observer_progress(), OBSERVER_PROGRESS);
}

#[test]
fn every_eligible_operation_constructs_its_exact_refusal() {
    let enrollment_request = enrollment();
    let ObserverBackpressure::Enrollment { request, state } = refused(
        ObserverCheckedOperation::Enrollment(enrollment_request.clone()),
    ) else {
        panic!("enrollment must retain its variant");
    };
    assert_eq!(request, enrollment_request);
    assert_state(state);

    let attach_request = attach();
    let ObserverBackpressure::CredentialAttach { request, state } = refused(
        ObserverCheckedOperation::CredentialAttach(attach_request.clone()),
    ) else {
        panic!("attach must retain its variant");
    };
    assert_eq!(request, attach_request);
    assert_state(state);

    let detach_request = detach();
    let committed_binding_epoch = binding_epoch();
    let ObserverBackpressure::Detach {
        request,
        committed_binding_epoch: response_epoch,
        state,
    } = refused(ObserverCheckedOperation::InitialDetach {
        request: detach_request.clone(),
        committed_binding_epoch,
    })
    else {
        panic!("detach must retain its variant");
    };
    assert_eq!(request, detach_request);
    assert_eq!(response_epoch, committed_binding_epoch);
    assert_state(state);

    let leave_request = leave();
    let ObserverBackpressure::Leave {
        request,
        state,
        prior_terminal_cell_exists,
    } = refused(ObserverCheckedOperation::Leave {
        request: leave_request.clone(),
        prior_terminal_cell_exists: true,
    })
    else {
        panic!("Leave must retain its variant");
    };
    assert_eq!(request, leave_request);
    assert!(prior_terminal_cell_exists);
    assert_state(state);

    let record_request = record();
    let ObserverBackpressure::RecordAdmission { request, state } = refused(
        ObserverCheckedOperation::RecordAdmission(record_request.clone()),
    ) else {
        panic!("record admission must retain its variant");
    };
    assert_eq!(request, record_request);
    assert_state(state);
}

#[test]
fn equality_passes_and_returns_the_exact_opaque_permit() {
    let decision = check_observer_floor(
        ObserverCheckedOperation::RecordAdmission(record()),
        OBSERVER_PROGRESS,
        u128::from(OBSERVER_PROGRESS) + 1,
    );
    let ObserverFloorDecision::Eligible(permit) = decision else {
        panic!("equality must pass");
    };
    assert_eq!(permit.observer_progress(), OBSERVER_PROGRESS);
    assert_eq!(permit.cap_floor(), u128::from(OBSERVER_PROGRESS) + 1);
}

#[test]
fn lower_floor_passes_without_recalculating_it() {
    let decision = check_observer_floor(
        ObserverCheckedOperation::Enrollment(enrollment()),
        OBSERVER_PROGRESS,
        7,
    );
    let ObserverFloorDecision::Eligible(permit) = decision else {
        panic!("floor below the observer boundary must pass");
    };
    assert_eq!(permit.observer_progress(), OBSERVER_PROGRESS);
    assert_eq!(permit.cap_floor(), 7);
}

#[test]
fn checked_one_past_maximum_progress_is_representable() {
    let one_past_max = u128::from(u64::MAX) + 1;
    let eligible = check_observer_floor(
        ObserverCheckedOperation::RecordAdmission(record()),
        u64::MAX,
        one_past_max,
    );
    assert!(matches!(eligible, ObserverFloorDecision::Eligible(_)));

    let refused = check_observer_floor(
        ObserverCheckedOperation::RecordAdmission(record()),
        u64::MAX,
        one_past_max + 1,
    );
    let ObserverFloorDecision::Respond(ObserverBackpressure::RecordAdmission { state, .. }) =
        refused
    else {
        panic!("strict excess above checked one-past MAX must refuse");
    };
    assert_eq!(state.observer_progress(), u64::MAX);
    assert_eq!(state.backpressure_epoch(), u64::MAX);
}

#[test]
fn refusal_echoes_inputs_without_exposing_a_commit_permit() {
    let request = leave();
    let original = request.clone();
    let decision = check_observer_floor(
        ObserverCheckedOperation::Leave {
            request,
            prior_terminal_cell_exists: false,
        },
        6,
        8,
    );
    let ObserverFloorDecision::Respond(ObserverBackpressure::Leave {
        request,
        state,
        prior_terminal_cell_exists,
    }) = decision
    else {
        panic!("blocking Leave must return only its refusal");
    };
    assert_eq!(request, original);
    assert!(!prior_terminal_cell_exists);
    assert_eq!(state.observer_progress(), 6);
    assert_eq!(state.backpressure_epoch(), 6);
}
