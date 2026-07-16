#![allow(
    clippy::expect_used,
    clippy::items_after_statements,
    clippy::many_single_char_names,
    clippy::missing_const_for_fn,
    clippy::panic,
    clippy::similar_names,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::type_complexity
)]

mod support;

use std::{boxed::Box, collections::BTreeSet, process::Command, vec, vec::Vec};

use liminal_protocol::{
    algebra::{
        ResourceDimension, ResourceVector, WideResourceVector, floor_transition,
        mandatory_capacity, no_edge_legal, recovery_transfer, retained_baseline,
        zero_debt_admission,
    },
    lifecycle::{
        ActiveBinding, AllocatedParticipantSlot, AttachCommitParameters, AttachSecretProof,
        AttachedRecordPosition, BindingState, BindingTerminalDisposition, BoundParticipantCursor,
        ClosureDebt, ClosureState, CommittedBindingTerminal, CommittedBindingTerminalPosition,
        CursorFateSuccessor, CursorProgressFact, CursorProgressKey, DebtCompletion, DetachCell,
        DetachedAttachRefusal, EnrollmentCommitParameters, EnrollmentFingerprint, Event,
        IdentityState, LeaveCommitParameters, LeaveFingerprint, LeaveOnlyEdge, LiveMember,
        LiveMemberRestore, NonzeroDebtCursorEpisode, ObserverProjection, ParticipantCursorProgress,
        ParticipantSlotAllocatorProof, PendingBindingTerminalPosition, PhysicalCompaction,
        RecoveredBindingFateTransition, StoredEdge, commit_attach, commit_detach,
        commit_enrollment, commit_leave,
    },
    outcome::{ReconnectDelayResult, ReconnectRequiredEvent, ReconnectState},
    wire::{
        AttachAttemptToken, AttachEnvelope, AttachSecret, BindingEpoch, BindingRequiredEnvelope,
        ClientRequest, ClosureCapacityReason, ClosureCheckedEnvelope, ClosureRefusalReason,
        ClosureSnapshot, ConnectionIncarnation, ConversationSequenceExhausted,
        CredentialAttachRequest, DetachAttemptToken, DetachRequest, EnrollmentRequest,
        EnrollmentToken, Generation, LeaveAttemptToken, LeaveEnvelope, LeaveRequest, MarkerAck,
        MarkerAckEnvelope, MarkerClosureCapacityExceeded, MarkerMismatch, MarkerMismatchBody,
        MarkerNotDelivered, MarkerNotDeliveredReason, MarkerProofRequest, NoBinding,
        ObserverBackpressure, ObserverBackpressureState, ParticipantAck, ParticipantAckEnvelope,
        ParticipantFrame, ReceiverDirection, RecordAdmissionEnvelope, RecordTooLarge,
        RepaymentEdge, SequenceAllocatingEnvelope, SequenceBudget, ServerValue, decode, encode,
        encoded_len,
    },
};
use support::{marker_delivery, settled_leave_authority};

const BM: u64 = 16;
const P0: u64 = 0;
const P1: u64 = 1;
const LAW1_PIN: &str = "ce8814daa748373d8ffc66b3ff1664f1697a5f4e";
const LAW1_PATHS: [&str; 4] = [
    ":(glob)crates/*/src/**",
    ":(glob)sdks/**",
    ":(exclude,glob)sdks/*/test/**",
    ":(exclude,glob)sdks/*/tests/**",
];

fn git_output(arguments: &[&str], no_match_is_success: bool) -> String {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("protocol crate is nested under the workspace crates directory");
    let output = Command::new("git")
        .args(arguments)
        .current_dir(workspace_root)
        .output()
        .expect("git is available for the frozen LAW-1 source audit");
    assert!(
        output.status.success() || (no_match_is_success && output.status.code() == Some(1)),
        "git source audit failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("git source text is UTF-8")
}

fn law1_grep(pattern: &str, case_insensitive: bool, pathspecs: &[&str]) -> String {
    let mut arguments = vec!["grep", "-I", "-n"];
    if case_insensitive {
        arguments.push("-i");
    }
    arguments.extend(["-E", pattern, LAW1_PIN, "--"]);
    arguments.extend_from_slice(pathspecs);
    git_output(&arguments, true)
}

fn law1_count(pattern: &str, case_insensitive: bool, pathspecs: &[&str]) -> usize {
    law1_grep(pattern, case_insensitive, pathspecs)
        .lines()
        .count()
}

fn generation(value: u64) -> Generation {
    Generation::new(value).expect("acceptance generations are nonzero")
}

fn epoch(server: u64, ordinal: u64, generation_value: u64) -> BindingEpoch {
    BindingEpoch::new(
        ConnectionIncarnation::new(server, ordinal),
        generation(generation_value),
    )
}

fn secret(byte: u8) -> AttachSecret {
    AttachSecret::new([byte; 32])
}

fn attach_token(byte: u8) -> AttachAttemptToken {
    AttachAttemptToken::new([byte; 16])
}

fn leave_token(byte: u8) -> LeaveAttemptToken {
    LeaveAttemptToken::new([byte; 16])
}

fn detach_token(byte: u8) -> DetachAttemptToken {
    DetachAttemptToken::new([byte; 16])
}

fn uniform(units: u64) -> ResourceVector {
    ResourceVector::new(
        units,
        units
            .checked_mul(BM)
            .expect("uniform fixture bytes fit u64"),
    )
}

fn wide_uniform(units: u128) -> WideResourceVector {
    WideResourceVector::new(units, units * u128::from(BM))
}

fn closure_debt(units: u128) -> ClosureDebt {
    ClosureDebt::new(wide_uniform(units)).expect("fixture closure debt is nonzero")
}

fn attach_envelope(request: &CredentialAttachRequest) -> AttachEnvelope {
    AttachEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        attach_attempt_token: request.attach_attempt_token,
        accept_marker_delivery_seq: request.accept_marker_delivery_seq,
    }
}

fn leave_envelope(request: &LeaveRequest) -> LeaveEnvelope {
    LeaveEnvelope {
        conversation_id: request.conversation_id,
        participant_id: request.participant_id,
        capability_generation: request.capability_generation,
        leave_attempt_token: request.leave_attempt_token,
    }
}

fn encoded(frame: &ParticipantFrame) -> Vec<u8> {
    let mut bytes = vec![0; encoded_len(frame).expect("typed acceptance frame has a wire size")];
    let written = encode(frame, &mut bytes).expect("typed acceptance frame encodes");
    assert_eq!(written, bytes.len());
    bytes
}

fn assert_client_round_trip(request: ClientRequest) -> Vec<u8> {
    let frame = ParticipantFrame::ClientRequest(request);
    let bytes = encoded(&frame);
    assert_eq!(
        decode(&bytes, ReceiverDirection::Server).expect("canonical request decodes"),
        frame
    );
    bytes
}

fn assert_server_round_trip(value: ServerValue) -> Vec<u8> {
    let frame = ParticipantFrame::ServerValue(value);
    let bytes = encoded(&frame);
    assert_eq!(
        decode(&bytes, ReceiverDirection::Client).expect("canonical server value decodes"),
        frame
    );
    bytes
}

fn assert_uniform_baseline(
    retained_units: u64,
    marker_credits: u64,
    expected_units: u128,
) -> WideResourceVector {
    let baseline = retained_baseline(uniform(retained_units), 1, marker_credits, uniform(1))
        .expect("one-slot fixture has a valid marker-credit count");
    assert_eq!(baseline, wide_uniform(expected_units));
    baseline
}

fn assert_floor(
    current: u128,
    member_cursor: Option<u64>,
    high_watermark: u64,
    observer_progress: u64,
    cap_floor: u128,
    expected_preferred: u128,
    expected_result: u128,
) {
    let floor = floor_transition(
        current,
        member_cursor,
        high_watermark,
        observer_progress,
        cap_floor,
    );
    assert_eq!(floor.preferred_floor, expected_preferred);
    assert_eq!(floor.resulting_floor, expected_result);
    assert_eq!(floor.member_cursor, member_cursor.unwrap_or(high_watermark),);
}

fn extract_marker_progress(state: ClosureState) -> ParticipantCursorProgress {
    let ClosureState::Owed {
        edge: StoredEdge::ParticipantCursorProgress(progress),
        ..
    } = state
    else {
        panic!("marker delivery must select participant cursor progress")
    };
    progress
}

fn extract_marker_release(
    state: ClosureState,
) -> liminal_protocol::lifecycle::DetachedMarkerRelease {
    let ClosureState::Owed {
        edge: StoredEdge::DetachedMarkerRelease(edge),
        ..
    } = state
    else {
        panic!("undelivered binding fate must select detached marker release")
    };
    edge
}

fn extract_credential_recovery(
    successor: CursorFateSuccessor,
) -> liminal_protocol::lifecycle::DetachedCredentialRecovery {
    let CursorFateSuccessor::DetachedCredentialRecovery(edge) = successor else {
        panic!("marker-backed cursor fate must select detached credential recovery")
    };
    edge
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SlotProof {
    conversation_id: u64,
    participant_index: u64,
    identity_limit: u64,
}

impl ParticipantSlotAllocatorProof for SlotProof {
    fn conversation_id(&self) -> u64 {
        self.conversation_id
    }

    fn participant_index(&self) -> u64 {
        self.participant_index
    }

    fn identity_limit(&self) -> u64 {
        self.identity_limit
    }
}

fn restored_member(
    conversation_id: u64,
    generation_value: u64,
    cursor: u64,
    terminal: Option<CommittedBindingTerminal>,
) -> LiveMember<[u8; 32]> {
    LiveMember::restore(LiveMemberRestore {
        participant_id: P0,
        conversation_id,
        generation: generation(generation_value),
        attach_secret: secret(
            u8::try_from(generation_value).expect("fixture generation fits one secret byte"),
        ),
        cursor,
        enrollment_fingerprint: EnrollmentFingerprint::new([0xE0; 32]),
        latest_terminal: terminal,
    })
    .expect("fixture terminal history matches the restored member")
}

fn sequence_budget(
    high_watermark: u64,
    e: u64,
    t: u64,
    m: u64,
    rs: u64,
    rt: u64,
    l_times_t: u128,
    l_times_rt: u128,
    l_other_times_e: u128,
) -> SequenceBudget {
    SequenceBudget {
        high_watermark,
        remaining: u64::MAX - high_watermark,
        e,
        t,
        m,
        rs,
        rt,
        l_times_t,
        l_times_rt,
        l_other_times_e,
    }
}

#[test]
fn acceptance_case_45_uniform_marker_episode_and_per_participant_occurrences() {
    // Frozen PARTICIPANT-CONTRACT.md lines 4080-4226. The participant-scoped
    // occurrence arm follows docs/design/LP-EXTRACTION-GOAL.md Fix 2: it never
    // reconstructs the frozen document's defective fixed occurrence array.
    const CONVERSATION: u64 = 45;
    const MINIMUM_CONVERSATION: u64 = 4_545;
    const H: u64 = 100;
    let q = uniform(2);
    let k = uniform(2);
    let c1 = epoch(45, 1, 7);
    let c2 = epoch(45, 2, 8);
    let c3 = epoch(45, 3, 9);
    let c4 = epoch(45, 4, 10);

    let u45 = CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(7),
        attach_secret: secret(7),
        attach_attempt_token: attach_token(0x45),
        accept_marker_delivery_seq: None,
    };
    let v45 = CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(8),
        attach_secret: secret(8),
        attach_attempt_token: attach_token(0x46),
        accept_marker_delivery_seq: Some(H),
    };
    let w45 = CredentialAttachRequest {
        attach_attempt_token: attach_token(0x47),
        accept_marker_delivery_seq: None,
        ..v45
    };
    let x45 = CredentialAttachRequest {
        capability_generation: generation(9),
        attach_secret: secret(9),
        attach_attempt_token: attach_token(0x48),
        accept_marker_delivery_seq: None,
        ..v45
    };
    for request in [&u45, &v45, &w45, &x45] {
        assert_client_round_trip(ClientRequest::CredentialAttach(request.clone()));
    }
    assert_eq!(u45.accept_marker_delivery_seq, None);
    assert_eq!(v45.accept_marker_delivery_seq, Some(H));
    assert_eq!(c2.capability_generation, generation(8));
    assert_eq!(c3.capability_generation, generation(9));
    assert_eq!(c4.capability_generation, generation(10));

    let leave_c2 = LeaveRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(8),
        attach_secret: secret(8),
        leave_attempt_token: leave_token(0x45),
    };
    let leave_c3 = LeaveRequest {
        capability_generation: generation(9),
        attach_secret: secret(9),
        leave_attempt_token: leave_token(0x46),
        ..leave_c2
    };
    assert_ne!(
        assert_client_round_trip(ClientRequest::Leave(leave_c2.clone())),
        assert_client_round_trip(ClientRequest::Leave(leave_c3)),
    );

    // Ce=5 gives max ordinary cap-(2Q+I*marker)=(0,0), with Entries first.
    let minimum_cap = uniform(5);
    let static_reserved = uniform(5);
    let maximum_ordinary = ResourceVector::new(
        minimum_cap.entries - static_reserved.entries,
        minimum_cap.bytes - static_reserved.bytes,
    );
    assert_eq!(maximum_ordinary, uniform(0));
    let too_large = RecordTooLarge {
        request: RecordAdmissionEnvelope {
            conversation_id: MINIMUM_CONVERSATION,
            participant_id: P0,
            capability_generation: Generation::ONE,
        },
        dimension: ResourceDimension::Entries,
        encoded_record_charge: uniform(1),
        max_ordinary_record_charge: maximum_ordinary,
    };
    assert_server_round_trip(ServerValue::RecordTooLarge(too_large.clone()));
    assert_eq!(too_large.dimension, ResourceDimension::Entries);
    assert_eq!(too_large.max_ordinary_record_charge, uniform(0));

    // Prefix: h-2/h-1 plus marker h, F=h-2, o=h-3, cursor h-4.
    assert_eq!(H - 14, 86);
    assert_eq!(H - 8, 92);
    assert_uniform_baseline(3, 1, 3);
    assert_floor(
        u128::from(H - 3),
        Some(H - 4),
        H + 2,
        H - 3,
        u128::from(H - 2),
        u128::from(H - 3),
        u128::from(H - 2),
    );
    let post_u_baseline = assert_uniform_baseline(5, 1, 5);
    let post_u_capacity = mandatory_capacity(post_u_baseline, q, k, uniform(7));
    assert_eq!(post_u_capacity.debt, wide_uniform(2));
    assert!(post_u_capacity.is_legal());
    assert_eq!(
        sequence_budget(H + 2, 1, 1, 0, 1, 1, 1, 1, 0),
        SequenceBudget {
            high_watermark: H + 2,
            remaining: u64::MAX - (H + 2),
            e: 1,
            t: 1,
            m: 0,
            rs: 1,
            rt: 1,
            l_times_t: 1,
            l_times_rt: 1,
            l_other_times_e: 0,
        }
    );
    assert_eq!(
        [H + 3, H + 4, H + 5, H + 6, H + 7, H + 8],
        [103, 104, 105, 106, 107, 108]
    );
    assert_eq!([H - 6, H - 5, H - 4, H - 3], [94, 95, 96, 97]);

    // U45 retargets the undelivered marker C1->C2, then exact delivery derives
    // marker-backed PCP. Fate can therefore derive DCR only from that fact.
    let marker_c1 = marker_delivery(P0, c1, H).expect("validated C1 marker record restores");
    let marker_c2 = marker_c1
        .retarget(c2, 0, 1, 2)
        .expect("first charged cycle is within J=2");
    assert_eq!(marker_c2.binding_epoch(), c2);
    let debt_q = closure_debt(2);
    let delivered = marker_c2
        .delivered(debt_q, Event::marker_delivered(P0, c2, H))
        .expect("exact marker delivery matches C2");
    assert_eq!(
        delivered,
        marker_c2
            .delivered(debt_q, Event::marker_delivered(P0, c2, H))
            .expect("crash replay is deterministic")
    );
    let marker_progress = extract_marker_progress(delivered);
    assert_eq!(marker_progress.marker_delivery_seq(), Some(H));
    assert_eq!(marker_progress.through_seq(), H);
    let dcr = extract_credential_recovery(
        marker_progress
            .binding_fate(debt_q, Event::binding_fate_observed(P0, c2, H - 1))
            .expect("exact C2 fate consumes marker-backed PCP"),
    );
    assert_eq!(dcr.marker_delivery_seq(), H);
    assert_eq!(dcr.prior_binding_epoch(), c2);

    // V45 transfers one K charge, accepts h, installs OP h+4, then recovered
    // C3 fate preserves that OP and its sole typed DCursor successor.
    let transfer =
        recovery_transfer(wide_uniform(4), k, uniform(1)).expect("one Attached charge fits K");
    assert_eq!(transfer.baseline, wide_uniform(5));
    assert_eq!(transfer.remaining_recovery_claim, uniform(1));
    let post_v_capacity = mandatory_capacity(
        transfer.baseline,
        q,
        transfer.remaining_recovery_claim,
        uniform(7),
    );
    assert_eq!(post_v_capacity.debt, wide_uniform(1));
    assert!(post_v_capacity.is_legal());
    let post_v_debt = closure_debt(1);
    let post_v_projection = ObserverProjection::new(H + 4);
    let fenced = dcr
        .fenced_attach(
            debt_q,
            Event::fenced_recovery_committed(P0, H, c2, c3, H + 1),
            DebtCompletion::observer_projection(post_v_debt, post_v_projection),
        )
        .expect("V45 presents the delivered C2 marker and immediate successor C3");
    assert_eq!(fenced.marker_delivery_seq(), H);
    assert_eq!(fenced.new_binding_epoch(), c3);
    assert_eq!(
        fenced.next_state(),
        ClosureState::Owed {
            debt: post_v_debt,
            edge: StoredEdge::ObserverProjection(post_v_projection),
        }
    );
    let recovered_fate = fenced
        .recovered_binding_fate(Event::binding_fate_observed(P0, c3, H + 1))
        .expect("exact recovered C3 fate is tied to post-V45 OP");
    let pending_release = match post_v_projection
        .apply_recovered_binding_fate(post_v_debt, post_v_debt, recovered_fate)
        .expect("fate preserves the exact incomplete OP")
    {
        RecoveredBindingFateTransition::PendingStorage(pending) => pending,
        RecoveredBindingFateTransition::DetachedCursorRelease(_) => {
            panic!("OP is incomplete and cannot be covered immediately")
        }
    };
    let cursor_release_state = post_v_projection
        .complete_after_recovered_binding_fate(
            Event::projection_completed(H + 4),
            Some(post_v_debt),
            pending_release,
        )
        .expect("OP completion installs the sole DCursor suffix");
    let ClosureState::Owed {
        edge: StoredEdge::DetachedCursorRelease(cursor_release),
        ..
    } = cursor_release_state
    else {
        panic!("recovered fate suffix must be DCursor")
    };
    let cursor_leave_claim = cursor_release
        .validate_leave_claim(P0, uniform(1), uniform(1), 1)
        .expect("C3 has exactly one K-backed exit charge");
    assert_eq!(cursor_leave_claim.actual_charge(), uniform(1));
    assert_eq!(
        cursor_release
            .leave(
                post_v_debt,
                Event::detached_leave_committed(P0, H + 5),
                cursor_leave_claim,
                DebtCompletion::clear(),
            )
            .expect("K-backed C3 Leave is DCursor's sole successor"),
        ClosureState::Clear
    );
    assert_floor(
        u128::from(H + 3),
        None,
        H + 6,
        H + 4,
        u128::from(H + 5),
        u128::from(H + 5),
        u128::from(H + 5),
    );
    assert!(no_edge_legal(
        WideResourceVector::default(),
        wide_uniform(3),
        q,
        k,
        uniform(7),
    ));

    // If C2 fate wins before marker delivery, the real typed edge is DMR: no
    // fenced attach is eligible, exact h is MarkerNotDelivered, and Leave is its
    // sole owner transition. OP can be preserved and later cleared.
    let dmr_state = marker_c2
        .binding_fate(debt_q, Event::binding_fate_observed(P0, c2, H - 2))
        .expect("C2 fate precedes delivery");
    let dmr = extract_marker_release(dmr_state);
    assert_eq!(
        dmr.ordinary_attach_refusal(),
        DetachedAttachRefusal::RecoveryFence
    );
    assert_eq!(
        dmr.marker_attach_refusal(H),
        DetachedAttachRefusal::MarkerNotDelivered
    );
    let two_record_claim = dmr
        .validate_leave_claim(P0, uniform(2), k, 2)
        .expect("pending terminal plus Left consume both K records");
    let initial_projection = ObserverProjection::new(H + 2);
    let leave_preserves_projection = dmr
        .leave(
            debt_q,
            Event::detached_leave_committed(P0, H - 2),
            two_record_claim,
            DebtCompletion::observer_projection(debt_q, initial_projection),
        )
        .expect("pre-OP DMR Leave preserves the required projection");
    assert_eq!(
        leave_preserves_projection,
        ClosureState::Owed {
            debt: debt_q,
            edge: StoredEdge::ObserverProjection(initial_projection),
        }
    );
    let clear_successor = initial_projection
        .clear_after_completion(&Event::projection_completed(H + 2))
        .expect("exact OP completion can clear zero debt");
    assert_eq!(
        initial_projection
            .complete(debt_q, Event::projection_completed(H + 2), clear_successor,)
            .expect("completion consumes its predecessor-bound authority"),
        ClosureState::Clear
    );
    assert_floor(
        u128::from(H - 2),
        None,
        H + 4,
        H + 2,
        u128::from(H + 3),
        u128::from(H + 3),
        u128::from(H + 3),
    );

    let backpressure = ObserverBackpressure::Leave {
        request: leave_envelope(&leave_c2),
        state: ObserverBackpressureState::initial(H - 3),
        prior_terminal_cell_exists: false,
    };
    assert_server_round_trip(ServerValue::ObserverBackpressure(backpressure));
    assert_eq!(
        marker_progress
            .complete_ack(
                debt_q,
                Event::marker_acknowledged(P0, c2, H, H + 1),
                DebtCompletion::clear(),
            )
            .expect("marker ack after delivery clears PCP"),
        ClosureState::Clear
    );
    assert_floor(
        u128::from(H - 2),
        Some(H),
        H + 2,
        H,
        u128::from(H + 1),
        u128::from(H + 1),
        u128::from(H + 1),
    );

    // Fix 2 regression required by this extraction brief: two bound participants
    // independently consume the same retained h-2/h-1 suffix while debt is live.
    let p0_epoch = c2;
    let p1_epoch = epoch(45, 20, 8);
    let mut episode = NonzeroDebtCursorEpisode::new(
        CONVERSATION,
        debt_q,
        H - 3,
        H + 2,
        u128::from(H - 2),
        u128::from(H - 2),
        vec![
            BoundParticipantCursor::new(P0, p0_epoch, H - 4),
            BoundParticipantCursor::new(P1, p1_epoch, H - 4),
        ],
    )
    .expect("per-participant episode has valid retention authority");
    let mut encodings = Vec::new();
    for (participant_id, binding_epoch, boundary) in [
        (P0, p0_epoch, H - 2),
        (P1, p1_epoch, H - 2),
        (P0, p0_epoch, H - 1),
        (P1, p1_epoch, H - 1),
    ] {
        let request = ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id,
            capability_generation: generation(8),
            through_seq: boundary,
        };
        assert!(matches!(
            episode
                .acknowledge(participant_id, binding_epoch, &request, H + 2)
                .expect("authority and contiguous suffix are exact"),
            liminal_protocol::lifecycle::CumulativeAckOutcome::Committed(_)
        ));
        assert!(episode.retains(H - 2));
        assert!(episode.retains(H - 1));
        encodings.push(episode.encode().expect("variable facts serialize"));
    }
    assert_eq!(episode.facts().len(), 4);
    for participant_index in [P0, P1] {
        for boundary in [H - 2, H - 1] {
            assert_eq!(
                episode.facts().get(CursorProgressKey {
                    participant_index,
                    boundary,
                }),
                Some(CursorProgressFact::Consumed)
            );
        }
    }
    assert!(encodings.windows(2).all(|pair| pair[0] != pair[1]));
    assert_eq!(
        episode.participant(P0).expect("P0 remains bound").cursor(),
        H - 1
    );
    assert_eq!(
        episode.participant(P1).expect("P1 remains bound").cursor(),
        H - 1
    );

    // Optional W45 capacity closure: B=5, B+Q+K=(9,9Bm), with Entries
    // preceding Bytes. These use the real algebra and complete wire outcome.
    let baseline_no_credit = assert_uniform_baseline(4, 0, 5);
    let cap_arms = [
        (
            uniform(7),
            wide_uniform(2),
            WideResourceVector::new(2, u128::from(4 * BM)),
            ResourceDimension::Entries,
            9,
            7,
        ),
        (
            ResourceVector::new(9, 7 * BM),
            WideResourceVector::new(0, u128::from(2 * BM)),
            WideResourceVector::new(4, u128::from(2 * BM)),
            ResourceDimension::Bytes,
            u128::from(9 * BM),
            u128::from(7 * BM),
        ),
        (
            uniform(7),
            wide_uniform(2),
            wide_uniform(2),
            ResourceDimension::Entries,
            9,
            7,
        ),
    ];
    for (cap, expected_debt, k_headroom, dimension, required, limit) in cap_arms {
        let capacity = mandatory_capacity(baseline_no_credit, q, k, cap);
        assert_eq!(capacity.debt, expected_debt);
        let snapshot = ClosureSnapshot {
            marker_capacity_credits: 1,
            marker_anchors: 1,
            entry_debt: u64::try_from(expected_debt.entries)
                .expect("fixture entry debt fits wire width"),
            byte_debt: u64::try_from(expected_debt.bytes)
                .expect("fixture byte debt fits wire width"),
            repayment_edge: RepaymentEdge::MarkerDelivery {
                participant_id: P0,
                binding_epoch: c2,
                marker_delivery_seq: H,
            },
            edge_sequence_claims: 6,
            edge_order_position_claims: 4,
            edge_k_remaining: k,
            k_headroom,
            episode_churn_used: 0,
            delta_cycles: 1,
            episode_churn_limit: 2,
        };
        let refusal = MarkerClosureCapacityExceeded {
            request: ClosureCheckedEnvelope::CredentialAttach(attach_envelope(&w45)),
            snapshot,
            reason: ClosureRefusalReason::Capacity(ClosureCapacityReason {
                dimension,
                required,
                limit,
            }),
        };
        assert_server_round_trip(ServerValue::MarkerClosureCapacityExceeded(Box::new(
            refusal,
        )));
    }

    let post_v_snapshot = ClosureSnapshot {
        marker_capacity_credits: 0,
        marker_anchors: 0,
        entry_debt: 1,
        byte_debt: BM,
        repayment_edge: RepaymentEdge::ObserverProjection { through_seq: H + 4 },
        edge_sequence_claims: 6,
        edge_order_position_claims: 4,
        edge_k_remaining: uniform(1),
        k_headroom: wide_uniform(2),
        episode_churn_used: 1,
        delta_cycles: 0,
        episode_churn_limit: 2,
    };
    let x45_refusal = MarkerClosureCapacityExceeded {
        request: ClosureCheckedEnvelope::CredentialAttach(attach_envelope(&x45)),
        snapshot: post_v_snapshot,
        reason: ClosureRefusalReason::RecoveryFence,
    };
    let before = x45_refusal.clone();
    assert_server_round_trip(ServerValue::MarkerClosureCapacityExceeded(Box::new(
        x45_refusal,
    )));
    assert_eq!(before.snapshot, post_v_snapshot);
}

#[test]
fn acceptance_case_46_flat_exit_claim_survives_detach_reattach_and_leave_fate_races() {
    // Frozen PARTICIPANT-CONTRACT.md lines 4227-4233.
    const CONVERSATION: u64 = 46;
    let enrollment_request = EnrollmentRequest {
        conversation_id: CONVERSATION,
        enrollment_token: EnrollmentToken::new([0x46; 16]),
    };
    assert_client_round_trip(ClientRequest::Enrollment(enrollment_request.clone()));
    let allocated = AllocatedParticipantSlot::from_allocator(SlotProof {
        conversation_id: CONVERSATION,
        participant_index: P0,
        identity_limit: 1,
    })
    .expect("P0 is inside I=1");
    let enrolled = commit_enrollment(
        &enrollment_request,
        EnrollmentCommitParameters {
            allocated_slot: allocated,
            attach_secret: secret(1),
            origin_binding_epoch: epoch(46, 1, 1),
            attached_position: AttachedRecordPosition::new(1, 1),
            receipt_expires_at: 1_000,
            provenance_expires_at: 2_000,
            enrollment_fingerprint: EnrollmentFingerprint::new([0x46; 32]),
        },
    )
    .expect("public enrollment commits");
    let BindingState::Bound(binding_one) = enrolled.binding_state else {
        panic!("enrollment must bind generation one")
    };

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct ReserveTerms {
        live_members: u64,
        e: u64,
        t: u64,
        l_times_t: u128,
    }
    impl ReserveTerms {
        fn for_live_binding(binding_state: BindingState) -> Self {
            let t = u64::from(matches!(binding_state, BindingState::Bound(_)));
            Self {
                live_members: 1,
                e: 1,
                t,
                l_times_t: u128::from(t),
            }
        }
    }
    let enrolled_reserve = ReserveTerms::for_live_binding(BindingState::Bound(binding_one));
    assert_eq!(enrolled_reserve.e, enrolled_reserve.live_members);

    let detach_one_request = DetachRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: Generation::ONE,
        detach_attempt_token: detach_token(0x46),
    };
    assert_client_round_trip(ClientRequest::Detach(detach_one_request.clone()));
    let verified = binding_one
        .verify_detach_request(detach_one_request, [0xD1; 32])
        .expect("detach body names the active epoch");
    let detached_one = commit_detach(
        enrolled.member,
        verified,
        DetachCell::default(),
        CommittedBindingTerminalPosition::new(2, 2),
    )
    .expect("first public detach commits");
    let (member_one, terminal_one, binding_state, cell_one, _) = detached_one.into_parts();
    assert_eq!(binding_state, BindingState::Detached);
    assert_eq!(terminal_one.binding_epoch(), binding_one.binding_epoch);
    let detached_reserve = ReserveTerms::for_live_binding(binding_state);
    assert_eq!(detached_reserve.e, detached_reserve.live_members);
    assert_eq!(detached_reserve.e, enrolled_reserve.e);

    let attach_two_request = CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: Generation::ONE,
        attach_secret: secret(1),
        attach_attempt_token: attach_token(0x46),
        accept_marker_delivery_seq: None,
    };
    let binding_two = ActiveBinding {
        participant_id: P0,
        conversation_id: CONVERSATION,
        binding_epoch: epoch(46, 2, 2),
    };
    let verified_attach = member_one
        .verify_detached_attach(
            binding_state,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits an ordinary detached attach"),
            attach_two_request,
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: binding_two,
                attach_secret: secret(2),
                attached_position: AttachedRecordPosition::new(3, 3),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .expect("generation-one credential is current");
    let attached_two = commit_attach(verified_attach, DetachCell::Committed(cell_one))
        .expect("reattach terminalizes the old detach cell");
    let reattached_reserve = ReserveTerms::for_live_binding(attached_two.binding_state);
    assert_eq!(reattached_reserve, enrolled_reserve);

    let detach_two_request = DetachRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(2),
        detach_attempt_token: detach_token(0x47),
    };
    let verified = binding_two
        .verify_detach_request(detach_two_request, [0xD2; 32])
        .expect("second detach names generation two");
    let detached_two = commit_detach(
        attached_two.member,
        verified,
        attached_two.detach_cell,
        CommittedBindingTerminalPosition::new(4, 4),
    )
    .expect("second public detach replaces the terminalized cell");
    let (member_two, _, detached_state, cell_two, _) = detached_two.into_parts();
    assert_eq!(detached_state, BindingState::Detached);
    assert_eq!(detached_reserve.e, 1);

    // Rebind solely to establish the bound Leave/death race requested by case 46.
    let binding_three = ActiveBinding {
        participant_id: P0,
        conversation_id: CONVERSATION,
        binding_epoch: epoch(46, 3, 3),
    };
    let verified_attach = member_two
        .verify_detached_attach(
            detached_state,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits an ordinary detached attach"),
            CredentialAttachRequest {
                conversation_id: CONVERSATION,
                participant_id: P0,
                capability_generation: generation(2),
                attach_secret: secret(2),
                attach_attempt_token: attach_token(0x47),
                accept_marker_delivery_seq: None,
            },
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: binding_three,
                attach_secret: secret(3),
                attached_position: AttachedRecordPosition::new(5, 5),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .expect("race fixture rebind is authorized");
    let race_start = commit_attach(verified_attach, DetachCell::Committed(cell_two))
        .expect("race fixture attach commits");

    let leave_request = LeaveRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(3),
        attach_secret: secret(3),
        leave_attempt_token: leave_token(0x46),
    };
    assert_client_round_trip(ClientRequest::Leave(leave_request.clone()));

    // Leave-first: one Left commits and the identity is already Retired before
    // any connection-fate path can acquire live authority.
    let leave_authority = race_start
        .member
        .verify_leave_request(
            &leave_request,
            AttachSecretProof::Verified,
            [0x4C; 32],
            LeaveFingerprint::new([0x46; 32]),
        )
        .expect("bound Leave authority is exact");
    let prepared_leave =
        settled_leave_authority(&race_start.member, BindingState::Bound(binding_three), 6, 6)
            .expect("bound Leave consumes the exact X/A frontier handles");
    let leave_first = commit_leave(
        race_start.member.clone(),
        BindingState::Bound(binding_three),
        race_start.detach_cell,
        leave_authority,
        prepared_leave,
        LeaveCommitParameters {
            left_delivery_seq: 6,
        },
    )
    .expect("bound Leave commits one Left");
    let IdentityState::Retired(leave_first_tombstone) = leave_first.identity() else {
        panic!("Leave-first history must be a tombstone")
    };
    assert_eq!(
        leave_first_tombstone.committed_result().left_delivery_seq(),
        6
    );
    assert_eq!(
        leave_first_tombstone
            .committed_result()
            .ended_binding_epoch(),
        Some(binding_three.binding_epoch)
    );

    // Death-first: exact-epoch Died is durable before the detached Leave's Left.
    let died = match binding_three.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(6, 6),
    )) {
        liminal_protocol::lifecycle::DiedBindingTransition::Committed(terminal) => terminal,
        liminal_protocol::lifecycle::DiedBindingTransition::Pending(_) => {
            panic!("the death-first arm supplied a committed position")
        }
    };
    let death_member = race_start
        .member
        .clone()
        .with_committed_terminal(died.into())
        .expect("Died terminal belongs to generation-three member");
    let death_leave_authority = death_member
        .verify_leave_request(
            &leave_request,
            AttachSecretProof::Verified,
            [0x4C; 32],
            LeaveFingerprint::new([0x46; 32]),
        )
        .expect("detached Leave retains credential authority");
    let prepared_death_leave = settled_leave_authority(&death_member, BindingState::Detached, 7, 7)
        .expect("detached Leave consumes the exact X frontier handle");
    let (death_first, _death_first_frontiers) = commit_leave(
        death_member,
        BindingState::Detached,
        race_start.detach_cell,
        death_leave_authority,
        prepared_death_leave,
        LeaveCommitParameters {
            left_delivery_seq: 7,
        },
    )
    .expect("death-first detached Leave writes Left second")
    .into_parts();
    let IdentityState::Retired(death_first_tombstone) = death_first else {
        panic!("death-first history must finish retired")
    };
    assert_eq!(died.binding_epoch(), binding_three.binding_epoch);
    assert_eq!(died.delivery_seq(), 6);
    assert_eq!(
        death_first_tombstone
            .committed_result()
            .prior_terminal_delivery_seq(),
        Some(6)
    );
    assert_eq!(
        death_first_tombstone.committed_result().left_delivery_seq(),
        7
    );

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum CrashVisibleRecord {
        Died(u64),
        Left(u64),
    }
    let leave_first_left = leave_first_tombstone.committed_result().left_delivery_seq();
    let death_first_left = death_first_tombstone.committed_result().left_delivery_seq();
    let death_first_died = died.delivery_seq();
    let leave_first_prefixes = [vec![], vec![CrashVisibleRecord::Left(leave_first_left)]];
    let death_first_prefixes = [
        vec![],
        vec![CrashVisibleRecord::Died(death_first_died)],
        vec![
            CrashVisibleRecord::Died(death_first_died),
            CrashVisibleRecord::Left(death_first_left),
        ],
    ];
    assert!(leave_first_prefixes.iter().all(|prefix| {
        prefix.is_empty() || prefix.as_slice() == [CrashVisibleRecord::Left(leave_first_left)]
    }));
    assert!(
        death_first_prefixes
            .windows(2)
            .all(|pair| pair[0] != pair[1])
    );
    assert_eq!(
        death_first_prefixes
            .last()
            .expect("complete death-first history"),
        &[
            CrashVisibleRecord::Died(death_first_died),
            CrashVisibleRecord::Left(death_first_left),
        ]
    );
}

#[test]
fn acceptance_case_47_canonical_sequence_budgets_and_gap_free_boundary_histories() {
    // Frozen PARTICIPANT-CONTRACT.md lines 4234-4323.
    const C47_A: u64 = 4_701;
    const C47_BC: u64 = 4_702;
    const C47_D: u64 = 4_703;
    const MAX: u64 = u64::MAX;
    let h = MAX - 6;
    let q = uniform(2);
    let k = uniform(2);
    let e47_a = epoch(47, 0, 3);
    let e47_b = epoch(20, 1, 4);
    let e47_d = epoch(20, 2, 5);

    let budget_a = sequence_budget(MAX - 2, 1, 0, 0, 0, 0, 0, 0, 0);
    let budget_b = sequence_budget(h, 1, 1, 0, 1, 1, 1, 1, 0);
    let budget_c = sequence_budget(h + 1, 1, 0, 1, 0, 0, 0, 0, 0);
    let budget_d = sequence_budget(MAX - 3, 1, 1, 0, 0, 0, 1, 0, 0);
    assert_eq!(
        [
            budget_a.remaining,
            budget_b.remaining,
            budget_c.remaining,
            budget_d.remaining
        ],
        [2, 6, 5, 3]
    );

    // The case fixes the canonical budgets independently of the optional
    // operation that observes them. RecordAdmission is the registry's common
    // optional sequence-allocating envelope and exercises the real wire schema.
    for (conversation_id, generation_value, budget) in [
        (C47_A, 3, budget_a),
        (C47_BC, 4, budget_b),
        (C47_BC, 4, budget_c),
        (C47_D, 5, budget_d),
    ] {
        let outcome = ConversationSequenceExhausted {
            request: SequenceAllocatingEnvelope::RecordAdmission(RecordAdmissionEnvelope {
                conversation_id,
                participant_id: P0,
                capability_generation: generation(generation_value),
            }),
            sequence_budget: budget,
        };
        assert_server_round_trip(ServerValue::ConversationSequenceExhausted(Box::new(
            outcome,
        )));
    }

    // Arm A exact public boundary and DCursor Leave.
    let floor_a = MAX - 7;
    assert_floor(
        u128::from(floor_a),
        Some(MAX - 8),
        MAX - 2,
        MAX - 2,
        u128::from(floor_a),
        u128::from(floor_a),
        u128::from(floor_a),
    );
    let baseline_a = assert_uniform_baseline(6, 0, 7);
    let capacity_a = mandatory_capacity(baseline_a, q, k, uniform(10));
    assert_eq!(capacity_a.debt, wide_uniform(1));
    assert!(capacity_a.is_legal());
    // Reach e47A through a real ordinary attach, then consume that same commit's
    // sealed ordinary provenance through cursor progress and exact binding fate.
    let arm_a_attach_request = CredentialAttachRequest {
        conversation_id: C47_A,
        participant_id: P0,
        capability_generation: generation(2),
        attach_secret: secret(2),
        attach_attempt_token: attach_token(0xA6),
        accept_marker_delivery_seq: None,
    };
    let arm_a_verified = restored_member(C47_A, 2, 0, None)
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits an ordinary detached attach"),
            arm_a_attach_request,
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: ActiveBinding {
                    participant_id: P0,
                    conversation_id: C47_A,
                    binding_epoch: e47_a,
                },
                attach_secret: secret(3),
                attached_position: AttachedRecordPosition::new(1, 1),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .expect("the public Arm A prefix ordinarily binds generation 3");
    let arm_a_attach = commit_attach(arm_a_verified, DetachCell::<[u8; 32]>::default())
        .expect("ordinary Arm A attach commits");
    assert_eq!(arm_a_attach.outcome.persisted_cursor(), 0);
    assert_eq!(arm_a_attach.outcome.accepted_marker_delivery_seq(), None);
    let BindingState::Bound(active_e47_a) = arm_a_attach.binding_state else {
        panic!("ordinary attach must bind e47A")
    };
    let arm_a_attach = arm_a_attach
        .ordinary_cursor_progressed(
            Event::cursor_progressed(P0, e47_a, 0, MAX - 8, floor_a)
                .expect("Arm A advances the exact e47A cursor to MAX-8"),
        )
        .expect("Arm A cursor progress consumes the preceding attach state");
    let died_e47_a = match active_e47_a.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(MAX - 5, MAX - 2),
    )) {
        liminal_protocol::lifecycle::DiedBindingTransition::Committed(terminal) => terminal,
        liminal_protocol::lifecycle::DiedBindingTransition::Pending(_) => {
            panic!("Arm A terminal is durable")
        }
    };
    assert_eq!(died_e47_a.binding_epoch(), e47_a);
    assert_eq!(died_e47_a.delivery_seq(), MAX - 2);
    let ordinary_fate = arm_a_attach
        .ordinary_binding_fate(died_e47_a, floor_a)
        .expect("exact e47A death consumes the integrated attach history");
    assert_eq!(ordinary_fate.through_seq(), MAX - 8);
    assert_eq!(ordinary_fate.resulting_floor(), floor_a);
    let ClosureState::Owed {
        edge: StoredEdge::DetachedCursorRelease(dcursor_a),
        ..
    } = ordinary_fate.into_direct_state(closure_debt(1))
    else {
        panic!("ordinary no-marker fate must select Arm A DCursor")
    };
    assert_eq!(dcursor_a.last_dead_binding_epoch(), e47_a);
    let l47_a = LeaveRequest {
        conversation_id: C47_A,
        participant_id: P0,
        capability_generation: generation(3),
        attach_secret: secret(3),
        leave_attempt_token: leave_token(0xA7),
    };
    assert_client_round_trip(ClientRequest::Leave(l47_a));
    let claim_a = dcursor_a
        .validate_leave_claim(P0, uniform(1), k, 1)
        .expect("MAX-1 Left consumes one K record");
    assert_eq!(claim_a.actual_charge(), uniform(1));
    assert_eq!(
        dcursor_a
            .leave(
                closure_debt(1),
                Event::detached_leave_committed(P0, MAX - 1),
                claim_a,
                DebtCompletion::clear(),
            )
            .expect("DCursor's sole successor is exact detached Leave"),
        ClosureState::Clear
    );
    let transfer_a = recovery_transfer(wide_uniform(1), k, uniform(1))
        .expect("Left transfers one recovery charge");
    assert_eq!(transfer_a.baseline, wide_uniform(2));
    assert_eq!(transfer_a.remaining_recovery_claim, uniform(1));
    assert_floor(
        u128::from(floor_a),
        None,
        MAX - 1,
        MAX - 2,
        u128::from(MAX - 1),
        u128::from(MAX - 1),
        u128::from(MAX - 1),
    );
    assert!(no_edge_legal(
        WideResourceVector::default(),
        transfer_a.baseline,
        q,
        k,
        uniform(10),
    ));
    assert_eq!(MAX.checked_add(1), None);

    // Arm B exact coupled claims and its B/C capacity fixed point.
    let floor_b = h - 4;
    assert_eq!(
        [h + 1, h + 2, h + 3, h + 4, h + 5, h + 6],
        [MAX - 5, MAX - 4, MAX - 3, MAX - 2, MAX - 1, MAX]
    );
    assert_eq!(
        [h - 3, h - 2, h - 1, h],
        [MAX - 9, MAX - 8, MAX - 7, MAX - 6]
    );
    assert_eq!(h + 2, MAX - 4);
    assert_eq!(h + 3, MAX - 3);
    assert_floor(
        u128::from(floor_b),
        Some(floor_b - 1),
        h,
        floor_b,
        u128::from(floor_b),
        u128::from(floor_b),
        u128::from(floor_b),
    );
    let baseline_b = assert_uniform_baseline(5, 0, 6);
    let capacity_b = mandatory_capacity(baseline_b, q, k, uniform(8));
    assert_eq!(capacity_b.debt, wide_uniform(2));
    assert!(capacity_b.is_legal());
    let tentative = mandatory_capacity(wide_uniform(7), q, k, uniform(8));
    assert!(!tentative.absolute_fit);
    assert!(!tentative.debt_within_mandatory_bound);
    assert_floor(
        u128::from(floor_b),
        Some(floor_b - 1),
        h + 1,
        floor_b,
        u128::from(floor_b + 1),
        u128::from(floor_b),
        u128::from(floor_b + 1),
    );

    let op_b = ObserverProjection::new(h);
    let fate_event_b = Event::binding_fate_observed(P0, e47_b, floor_b + 1);
    assert_eq!(
        op_b.independent_event(closure_debt(2), fate_event_b, Some(closure_debt(2)))
            .expect("EOF-first preserves OP"),
        ClosureState::Owed {
            debt: closure_debt(2),
            edge: StoredEdge::ObserverProjection(op_b),
        }
    );
    let pc_c = PhysicalCompaction::new(floor_b + 1, floor_b + 1)
        .expect("Arm C compaction is a one-row range");
    assert_eq!(pc_c.from_floor(), floor_b + 1);
    assert_eq!(pc_c.through_seq(), floor_b + 1);
    let marker_c =
        marker_delivery(P0, e47_b, h + 2).expect("validated case-47 marker record restores");
    let dmr_c = extract_marker_release(
        marker_c
            .binding_fate(closure_debt(2), fate_event_b)
            .expect("undelivered fate releases the DCR branch"),
    );
    assert_eq!(dmr_c.marker_delivery_seq(), h + 2);
    assert_eq!(dmr_c.last_dead_binding_epoch(), e47_b);
    let baseline_c = assert_uniform_baseline(6, 1, 6);
    assert_eq!(baseline_c, wide_uniform(6));
    assert_floor(
        u128::from(floor_b + 1),
        Some(floor_b - 1),
        h + 1,
        h,
        u128::from(floor_b + 1),
        u128::from(floor_b),
        u128::from(floor_b + 1),
    );
    assert_floor(
        u128::from(floor_b + 1),
        None,
        h + 3,
        h,
        u128::from(h + 1),
        u128::from(h + 1),
        u128::from(h + 1),
    );
    let l47_c = LeaveRequest {
        conversation_id: C47_BC,
        participant_id: P0,
        capability_generation: generation(4),
        attach_secret: secret(4),
        leave_attempt_token: leave_token(0xC7),
    };
    assert_client_round_trip(ClientRequest::Leave(l47_c));
    let c_claim = dmr_c
        .validate_leave_claim(P0, uniform(1), k, 1)
        .expect("durable terminal leaves one-record exit charge");
    assert_eq!(
        dmr_c
            .leave(
                closure_debt(2),
                Event::detached_leave_committed(P0, h + 1),
                c_claim,
                DebtCompletion::clear(),
            )
            .expect("DMR detached Leave clears the episode"),
        ClosureState::Clear
    );
    assert!(no_edge_legal(
        WideResourceVector::default(),
        wide_uniform(3),
        q,
        k,
        uniform(8),
    ));

    // Arm D: empty suffix, exact reserve equality, bound Leave at MAX-2.
    assert_floor(
        u128::from(MAX - 2),
        Some(MAX - 3),
        MAX - 3,
        MAX - 3,
        u128::from(MAX - 2),
        u128::from(MAX - 2),
        u128::from(MAX - 2),
    );
    let baseline_d = assert_uniform_baseline(0, 0, 1);
    assert!(zero_debt_admission(baseline_d, q, k, uniform(10)));
    let l47_d = LeaveRequest {
        conversation_id: C47_D,
        participant_id: P0,
        capability_generation: generation(5),
        attach_secret: secret(5),
        leave_attempt_token: leave_token(0xD7),
    };
    assert_client_round_trip(ClientRequest::Leave(l47_d.clone()));
    let member_d = restored_member(C47_D, 5, MAX - 3, None);
    let verified_d = member_d
        .verify_leave_request(
            &l47_d,
            AttachSecretProof::Verified,
            [0xD7; 32],
            LeaveFingerprint::new([0xD7; 32]),
        )
        .expect("Arm D bound Leave authority matches");
    let binding_d = BindingState::Bound(ActiveBinding {
        participant_id: P0,
        conversation_id: C47_D,
        binding_epoch: e47_d,
    });
    let prepared_d = settled_leave_authority(&member_d, binding_d, MAX - 2, MAX - 2)
        .expect("Arm D consumes its pre-owned later X handle and relays A");
    let (left_d, _left_d_frontiers) = commit_leave(
        member_d,
        binding_d,
        DetachCell::<[u8; 32]>::default(),
        verified_d,
        prepared_d,
        LeaveCommitParameters {
            left_delivery_seq: MAX - 2,
        },
    )
    .expect("pre-owned E/T lets bound Leave commit without wrap")
    .into_parts();
    let IdentityState::Retired(tombstone_d) = left_d else {
        panic!("Arm D Leave must retire P0")
    };
    assert_eq!(tombstone_d.committed_result().left_delivery_seq(), MAX - 2);
    assert_eq!(
        tombstone_d.committed_result().ended_binding_epoch(),
        Some(e47_d)
    );
}

#[test]
fn acceptance_case_48_marker_ack_and_fenced_recovery_converge_for_both_observer_orders() {
    // Frozen PARTICIPANT-CONTRACT.md lines 4324-4438.
    const CONVERSATION: u64 = 48;
    const H: u64 = 100;
    let q = uniform(2);
    let k = uniform(2);
    let e6 = epoch(48, 6, 6);
    let e7 = epoch(48, 7, 7);
    let debt_q = closure_debt(2);

    let u48 = CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(5),
        attach_secret: secret(5),
        attach_attempt_token: attach_token(0x80),
        accept_marker_delivery_seq: None,
    };
    let v48 = CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(6),
        attach_secret: secret(6),
        attach_attempt_token: attach_token(0x81),
        accept_marker_delivery_seq: Some(H),
    };
    assert_client_round_trip(ClientRequest::CredentialAttach(u48));
    assert_client_round_trip(ClientRequest::CredentialAttach(v48));
    let leave_e6 = LeaveRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(6),
        attach_secret: secret(6),
        leave_attempt_token: leave_token(0x80),
    };
    let leave_e7 = LeaveRequest {
        capability_generation: generation(7),
        attach_secret: secret(7),
        leave_attempt_token: leave_token(0x81),
        ..leave_e6
    };
    assert_client_round_trip(ClientRequest::Leave(leave_e6));
    assert_client_round_trip(ClientRequest::Leave(leave_e7));

    assert_eq!(
        sequence_budget(H + 2, 1, 1, 0, 1, 1, 1, 1, 0).remaining,
        u64::MAX - 102
    );
    assert_eq!(
        [H + 3, H + 4, H + 5, H + 6, H + 7, H + 8],
        [103, 104, 105, 106, 107, 108]
    );
    assert_eq!([H - 4, H - 3, H - 2, H - 1], [96, 97, 98, 99]);
    let baseline = assert_uniform_baseline(5, 1, 5);
    let capacity = mandatory_capacity(baseline, q, k, uniform(7));
    assert_eq!(capacity.debt, wide_uniform(2));
    assert!(capacity.is_legal());

    let delivery = marker_delivery(P0, e6, H).expect("validated case-48 marker record restores");
    let marker_progress = extract_marker_progress(
        delivery
            .delivered(debt_q, Event::marker_delivered(P0, e6, H))
            .expect("U48 retargeted then delivered h to e6"),
    );
    assert_eq!(marker_progress.marker_delivery_seq(), Some(H));

    for observer_progress in [H - 1, H] {
        let expected_floor = if observer_progress == H - 1 { H } else { H + 1 };
        assert_floor(
            u128::from(H - 2),
            Some(H),
            H + 2,
            observer_progress,
            u128::from(expected_floor),
            u128::from(expected_floor),
            u128::from(expected_floor),
        );
        assert_eq!(
            marker_progress
                .complete_ack(
                    debt_q,
                    Event::marker_acknowledged(P0, e6, H, expected_floor),
                    DebtCompletion::clear(),
                )
                .expect("exact marker ack clears either observer-order snapshot"),
            ClosureState::Clear
        );
        assert!(no_edge_legal(
            WideResourceVector::default(),
            wide_uniform(3),
            q,
            k,
            uniform(7),
        ));
    }

    // Fate first derives DCR from the real marker-backed PCP. Each observer
    // ordering below drives its own fenced transition with its measured floor:
    // h in the behind arm and h+1 in the ahead arm.
    let dcr = extract_credential_recovery(
        marker_progress
            .binding_fate(debt_q, Event::binding_fate_observed(P0, e6, H - 1))
            .expect("e6 fate matches marker PCP"),
    );
    assert_eq!(dcr.marker_delivery_seq(), H);
    let transfer = recovery_transfer(wide_uniform(4), k, uniform(1))
        .expect("V48 Attached transfers one K charge");
    assert_eq!(transfer.baseline, wide_uniform(5));
    assert_eq!(transfer.remaining_recovery_claim, uniform(1));
    let post_recovery = mandatory_capacity(
        transfer.baseline,
        q,
        transfer.remaining_recovery_claim,
        uniform(7),
    );
    assert_eq!(post_recovery.debt, wide_uniform(1));
    let debt_one = closure_debt(1);
    for (observer_progress, recovery_floor) in [(H - 1, H), (H, H + 1)] {
        let recovered_op = ObserverProjection::new(H + 4);
        let fenced = dcr
            .fenced_attach(
                debt_q,
                Event::fenced_recovery_committed(P0, H, e6, e7, recovery_floor),
                DebtCompletion::observer_projection(debt_one, recovered_op),
            )
            .expect("V48 exact marker proof commits e7");
        assert_eq!(fenced.prior_binding_epoch(), e6);
        assert_eq!(fenced.new_binding_epoch(), e7);
        assert_floor(
            u128::from(H - 1),
            Some(H),
            H + 4,
            observer_progress,
            u128::from(recovery_floor),
            u128::from(recovery_floor),
            u128::from(recovery_floor),
        );
        assert_floor(
            u128::from(recovery_floor),
            Some(H + 2),
            H + 4,
            H + 2,
            u128::from(H + 3),
            u128::from(H + 3),
            u128::from(H + 3),
        );
        assert!(no_edge_legal(
            WideResourceVector::default(),
            wide_uniform(3),
            q,
            k,
            uniform(7),
        ));

        // Recovered e7 fate is tied to this arm's fenced proof and preserves
        // OP. Completion installs DCursor rather than recreating DCR.
        let recovered_fate = fenced
            .recovered_binding_fate(Event::binding_fate_observed(P0, e7, recovery_floor))
            .expect("e7 fate matches the fenced result epoch and measured floor");
        let pending = match recovered_op
            .apply_recovered_binding_fate(debt_one, debt_one, recovered_fate)
            .expect("fate preserves the incomplete recovery OP")
        {
            RecoveredBindingFateTransition::PendingStorage(pending) => pending,
            RecoveredBindingFateTransition::DetachedCursorRelease(_) => {
                panic!("incomplete OP is not covered")
            }
        };
        let dcursor_state = recovered_op
            .complete_after_recovered_binding_fate(
                Event::projection_completed(H + 4),
                Some(debt_one),
                pending,
            )
            .expect("completion installs the strict DCursor suffix");
        let ClosureState::Owed {
            edge: StoredEdge::DetachedCursorRelease(dcursor),
            ..
        } = dcursor_state
        else {
            panic!("recovered marker was accepted, so e7 fate is cursor-only")
        };
        assert_eq!(dcursor.last_dead_binding_epoch(), e7);
        let claim = dcursor
            .validate_leave_claim(P0, uniform(1), uniform(1), 1)
            .expect("e7 detached Leave consumes the remaining K record");
        assert_eq!(
            dcursor
                .leave(
                    debt_one,
                    Event::detached_leave_committed(P0, recovery_floor),
                    claim,
                    DebtCompletion::observer_projection(debt_one, ObserverProjection::new(H + 6),),
                )
                .expect("e7 detached Leave preserves its arm's measured floor"),
            ClosureState::Owed {
                debt: debt_one,
                edge: StoredEdge::ObserverProjection(ObserverProjection::new(H + 6)),
            }
        );
        assert_floor(
            u128::from(recovery_floor),
            None,
            H + 6,
            H + 6,
            u128::from(H + 7),
            u128::from(H + 7),
            u128::from(H + 7),
        );

        // Live e7 Leave is the complementary ordering before recovery OP
        // completion. Its own h+5 append atomically replaces OP h+4 with the
        // exact later projection; it does not reorder projection ahead of Leave.
        let live_leave = Event::live_leave_committed(P0, e7, recovery_floor);
        let live_leave_projection = ObserverProjection::new(H + 5);
        let leave_successor = recovered_op
            .later_projection_after_leave(&live_leave, debt_q, live_leave_projection)
            .expect("live e7 Leave binds exact OP h+4 to later OP h+5");
        assert_eq!(
            recovered_op
                .leave_with_later_projection(debt_one, live_leave, leave_successor)
                .expect("live e7 Leave atomically installs its exact later OP"),
            ClosureState::Owed {
                debt: debt_q,
                edge: StoredEdge::ObserverProjection(live_leave_projection),
            }
        );
        assert_floor(
            u128::from(recovery_floor),
            None,
            H + 5,
            H + 5,
            u128::from(H + 6),
            u128::from(H + 6),
            u128::from(H + 6),
        );

        // The e6 detached-Leave and live-Leave alternatives likewise use the
        // exact behind/ahead floor rather than confusing OP through_seq with F'.
        let e6_claim = dcr
            .validate_leave_claim(P0, uniform(1), k, 1)
            .expect("durable e6 terminal leaves one exact K charge");
        assert_eq!(
            dcr.detached_leave(
                debt_q,
                Event::detached_leave_committed(P0, recovery_floor),
                e6_claim,
                DebtCompletion::observer_projection(debt_one, ObserverProjection::new(H + 4)),
            )
            .expect("e6 detached Leave selects OP"),
            ClosureState::Owed {
                debt: debt_one,
                edge: StoredEdge::ObserverProjection(ObserverProjection::new(H + 4)),
            }
        );
        assert_eq!(
            marker_progress
                .leave(
                    debt_q,
                    Event::live_leave_committed(P0, e6, recovery_floor),
                    DebtCompletion::observer_projection(debt_one, ObserverProjection::new(H + 3),),
                )
                .expect("live e6 Leave is the mandatory-Q edge transition"),
            ClosureState::Owed {
                debt: debt_one,
                edge: StoredEdge::ObserverProjection(ObserverProjection::new(H + 3)),
            }
        );
        assert_floor(
            u128::from(recovery_floor),
            None,
            H + 4,
            H + 4,
            u128::from(H + 5),
            u128::from(H + 5),
            u128::from(H + 5),
        );
        assert_floor(
            u128::from(recovery_floor),
            None,
            H + 3,
            H + 3,
            u128::from(H + 4),
            u128::from(H + 4),
            u128::from(H + 4),
        );
    }
    assert!(no_edge_legal(
        WideResourceVector::default(),
        wide_uniform(1),
        q,
        k,
        uniform(7),
    ));
}

#[test]
fn acceptance_case_49_undelivered_marker_is_leave_only_and_never_fenced_recovery() {
    // Frozen PARTICIPANT-CONTRACT.md lines 4439-4491.
    const CONVERSATION: u64 = 49;
    const H: u64 = 100;
    let q = uniform(2);
    let k = uniform(2);
    let e9 = epoch(49, 9, 9);
    let debt_q = closure_debt(2);

    assert_floor(
        u128::from(H - 3),
        Some(H - 4),
        H,
        H - 3,
        u128::from(H - 2),
        u128::from(H - 3),
        u128::from(H - 2),
    );
    let baseline = assert_uniform_baseline(3, 1, 3);
    let capacity = mandatory_capacity(baseline, q, k, uniform(5));
    assert_eq!(capacity.debt, wide_uniform(2));
    assert!(capacity.is_legal());
    let budget = sequence_budget(H, 1, 1, 0, 0, 0, 1, 0, 0);
    assert_eq!(budget.remaining, u64::MAX - H);
    assert_eq!([H + 1, H + 2, H + 3], [101, 102, 103]);
    assert_eq!([H - 9, H - 8], [91, 92]);

    let active_e9 = ActiveBinding {
        participant_id: P0,
        conversation_id: CONVERSATION,
        binding_epoch: e9,
    };
    let pending = match active_e9.connection_lost(BindingTerminalDisposition::Pending(
        PendingBindingTerminalPosition::new(H - 9),
    )) {
        liminal_protocol::lifecycle::DiedBindingTransition::Pending(pending) => pending,
        liminal_protocol::lifecycle::DiedBindingTransition::Committed(_) => {
            panic!("case 49 fate is intentionally pending")
        }
    };
    assert_eq!(pending.admission_order().transaction_order(), H - 9);
    assert_eq!(pending.binding_epoch(), e9);
    assert_eq!(
        pending.cause(),
        liminal_protocol::wire::DiedCause::ConnectionLost
    );

    // The emitter never delivered h. Exact fate on MarkerDelivery therefore
    // derives DMR, whose sealed interface permits only K-backed detached Leave.
    let marker = marker_delivery(P0, e9, H).expect("validated case-49 marker record restores");
    let marker_fate = Event::binding_fate_observed(P0, e9, H - 2);
    let dmr_state = marker
        .binding_fate(debt_q, marker_fate)
        .expect("exact e9 fate consumes undelivered marker delivery");
    assert_eq!(
        dmr_state,
        marker
            .binding_fate(debt_q, marker_fate)
            .expect("crash replay reproduces the same DMR")
    );
    let dmr = extract_marker_release(dmr_state);
    assert_eq!(dmr.participant_id(), P0);
    assert_eq!(dmr.marker_delivery_seq(), H);
    assert_eq!(dmr.last_dead_binding_epoch(), e9);
    assert_eq!(
        dmr.ordinary_attach_refusal(),
        DetachedAttachRefusal::RecoveryFence
    );
    assert_eq!(
        dmr.marker_attach_refusal(H),
        DetachedAttachRefusal::MarkerNotDelivered
    );

    let u49_n = CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(9),
        attach_secret: secret(9),
        attach_attempt_token: attach_token(0x90),
        accept_marker_delivery_seq: None,
    };
    let u49_m = CredentialAttachRequest {
        attach_attempt_token: attach_token(0x91),
        accept_marker_delivery_seq: Some(H),
        ..u49_n
    };
    assert_client_round_trip(ClientRequest::CredentialAttach(u49_n.clone()));
    assert_client_round_trip(ClientRequest::CredentialAttach(u49_m.clone()));
    let snapshot = ClosureSnapshot {
        marker_capacity_credits: 1,
        marker_anchors: 1,
        entry_debt: 2,
        byte_debt: 2 * BM,
        repayment_edge: RepaymentEdge::DetachedMarkerRelease {
            participant_id: P0,
            marker_delivery_seq: H,
            last_dead_binding_epoch: e9,
        },
        edge_sequence_claims: 3,
        edge_order_position_claims: 1,
        edge_k_remaining: k,
        k_headroom: wide_uniform(2),
        episode_churn_used: 1,
        delta_cycles: 0,
        episode_churn_limit: 2,
    };
    let generic_refusal = MarkerClosureCapacityExceeded {
        request: ClosureCheckedEnvelope::CredentialAttach(attach_envelope(&u49_n)),
        snapshot,
        reason: ClosureRefusalReason::RecoveryFence,
    };
    assert_server_round_trip(ServerValue::MarkerClosureCapacityExceeded(Box::new(
        generic_refusal,
    )));
    let not_delivered = MarkerNotDelivered {
        request: MarkerProofRequest::CredentialAttach(liminal_protocol::wire::AttachMarkerProof {
            conversation_id: CONVERSATION,
            token: u49_m.attach_attempt_token,
            participant_id: P0,
            capability_generation: generation(9),
            requested_marker_delivery_seq: H,
        }),
        reason: MarkerNotDeliveredReason::NotDeliveredToProofEpoch,
        expected_marker_delivery_seq: H,
    };
    assert_server_round_trip(ServerValue::MarkerNotDelivered(not_delivered));
    assert_eq!(
        snapshot.repayment_edge,
        RepaymentEdge::DetachedMarkerRelease {
            participant_id: P0,
            marker_delivery_seq: H,
            last_dead_binding_epoch: e9,
        }
    );

    let leave_request = LeaveRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(9),
        attach_secret: secret(9),
        leave_attempt_token: leave_token(0x99),
    };
    assert_client_round_trip(ClientRequest::Leave(leave_request));
    let claim = dmr
        .validate_leave_claim(P0, uniform(2), k, 2)
        .expect("pending terminal and Left consume the two-record K claim");
    let transfer =
        recovery_transfer(baseline, k, uniform(2)).expect("two records transfer all remaining K");
    assert_eq!(transfer.baseline, wide_uniform(5));
    assert_eq!(transfer.remaining_recovery_claim, uniform(0));
    let leave_capacity = mandatory_capacity(
        transfer.baseline,
        q,
        transfer.remaining_recovery_claim,
        uniform(5),
    );
    assert_eq!(leave_capacity.debt, wide_uniform(2));
    assert!(leave_capacity.is_legal());
    let op = ObserverProjection::new(H + 2);
    assert_eq!(
        dmr.leave(
            debt_q,
            Event::detached_leave_committed(P0, H - 2),
            claim,
            DebtCompletion::observer_projection(debt_q, op),
        )
        .expect("DMR Leave installs OP through h+2"),
        ClosureState::Owed {
            debt: debt_q,
            edge: StoredEdge::ObserverProjection(op),
        }
    );
    assert_floor(
        u128::from(H - 2),
        None,
        H + 2,
        H - 3,
        u128::from(H - 2),
        u128::from(H - 2),
        u128::from(H - 2),
    );
    let clear = op
        .clear_after_completion(&Event::projection_completed(H + 2))
        .expect("exact projection completion can clear");
    assert_eq!(
        op.complete(debt_q, Event::projection_completed(H + 2), clear)
            .expect("OP completion consumes its exact event"),
        ClosureState::Clear
    );
    assert_floor(
        u128::from(H - 2),
        None,
        H + 2,
        H + 2,
        u128::from(H + 3),
        u128::from(H + 3),
        u128::from(H + 3),
    );
    let post_baseline = assert_uniform_baseline(0, 0, 1);
    assert!(no_edge_legal(
        WideResourceVector::default(),
        post_baseline,
        q,
        k,
        uniform(5),
    ));
    assert_eq!(
        sequence_budget(H + 2, 0, 0, 0, 0, 0, 0, 0, 0),
        SequenceBudget {
            high_watermark: H + 2,
            remaining: u64::MAX - (H + 2),
            e: 0,
            t: 0,
            m: 0,
            rs: 0,
            rt: 0,
            l_times_t: 0,
            l_times_rt: 0,
            l_other_times_e: 0,
        }
    );
}

#[test]
fn acceptance_case_50_reconnect_permits_are_single_use_and_wait_inventory_is_total() {
    // Frozen PARTICIPANT-CONTRACT.md lines 4492-4507. Reconnect scheduling,
    // dedup notification, pressure buffering, and the pinned multi-root audit are
    // SDK/product mechanics outside liminal-protocol; this compact harness emits
    // the crate's real ReconnectDelayResult values and records every mutation.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum ReconnectEntryPoint {
        RustLifecycle,
        RustRemoteHandle,
        Gleam,
        CallerTimer,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct ReconnectHarness {
        permit_delay_ms: Option<u64>,
        state: ReconnectState,
        attempts: u64,
        last_delay_ms: Option<u64>,
        timer_arms: u64,
        network_opens: u64,
    }

    impl ReconnectHarness {
        fn new() -> Self {
            Self {
                permit_delay_ms: None,
                state: ReconnectState::Reconnecting,
                attempts: 0,
                last_delay_ms: None,
                timer_arms: 0,
                network_opens: 0,
            }
        }

        fn transport_fate(&mut self, delay_ms: u64) {
            self.permit_delay_ms = Some(delay_ms);
        }

        fn reconnect(&mut self, entry: ReconnectEntryPoint) -> ReconnectDelayResult {
            let Some(delay_ms) = self.permit_delay_ms.take() else {
                return ReconnectDelayResult::ReconnectNotArmed {
                    state: self.state,
                    required_event: ReconnectRequiredEvent::TransportFate,
                };
            };
            self.attempts += 1;
            self.last_delay_ms = Some(delay_ms);
            if entry == ReconnectEntryPoint::CallerTimer {
                self.timer_arms += 1;
            }
            ReconnectDelayResult::ReconnectArmed { delay_ms }
        }

        fn manual_connect(&mut self) {
            self.network_opens += 1;
        }
    }

    for entry in [
        ReconnectEntryPoint::RustLifecycle,
        ReconnectEntryPoint::RustRemoteHandle,
        ReconnectEntryPoint::Gleam,
        ReconnectEntryPoint::CallerTimer,
    ] {
        let mut harness = ReconnectHarness::new();
        harness.transport_fate(125);
        assert_eq!(
            harness.reconnect(entry),
            ReconnectDelayResult::ReconnectArmed { delay_ms: 125 }
        );
        let after_first = harness;
        assert_eq!(
            harness.reconnect(entry),
            ReconnectDelayResult::ReconnectNotArmed {
                state: ReconnectState::Reconnecting,
                required_event: ReconnectRequiredEvent::TransportFate,
            }
        );
        assert_eq!(harness, after_first);
        harness.transport_fate(250);
        assert_eq!(
            harness.reconnect(entry),
            ReconnectDelayResult::ReconnectArmed { delay_ms: 250 }
        );
        assert_eq!(harness.attempts, 2);
        assert_eq!(harness.last_delay_ms, Some(250));
        assert_eq!(harness.network_opens, 0);
        harness.manual_connect();
        assert_eq!(harness.network_opens, 1);
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct PressureHarness {
        buffered: Vec<u8>,
        published: Vec<u8>,
        producer_retries: u64,
        delivery_estimate_ms: Option<u64>,
    }
    let mut pressure = PressureHarness {
        buffered: Vec::new(),
        published: Vec::new(),
        producer_retries: 0,
        delivery_estimate_ms: None,
    };
    pressure.buffered.push(7);
    pressure.delivery_estimate_ms = Some(40);
    assert_eq!(pressure.buffered, [7]);
    assert_eq!(pressure.producer_retries, 0);
    let item = pressure
        .buffered
        .pop()
        .expect("accepted item remains buffered");
    pressure.published.push(item);
    assert_eq!(pressure.published, [7]);
    assert_eq!(pressure.producer_retries, 0);

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct DedupExpiryHarness {
        present: bool,
        expiry_notifications: u64,
        sweeps: u64,
    }
    let mut dedup = DedupExpiryHarness {
        present: true,
        expiry_notifications: 0,
        sweeps: 0,
    };
    dedup.expiry_notifications += 1;
    dedup.present = false;
    assert_eq!(
        dedup,
        DedupExpiryHarness {
            present: false,
            expiry_notifications: 1,
            sweeps: 0,
        }
    );

    // Re-run the frozen source audit rather than asserting a copied count
    // vector. `git grep -l` proves the pathspecs match the exact five product
    // roots and 199 nonempty files before zero-match expressions are accepted.
    let mut file_arguments = vec!["grep", "-I", "-l", "-E", ".", LAW1_PIN, "--"];
    file_arguments.extend_from_slice(&LAW1_PATHS);
    let files = git_output(&file_arguments, false);
    assert_eq!(files.lines().count(), 199);
    let roots = files
        .lines()
        .map(|line| {
            let (_, path) = line
                .split_once(':')
                .expect("revision grep output carries a path");
            let mut components = path.split('/');
            match components.next() {
                Some("crates") => format!(
                    "crates/{}/src",
                    components.next().expect("crate path has a crate name")
                ),
                Some("sdks") => format!(
                    "sdks/{}",
                    components.next().expect("SDK path has an SDK name")
                ),
                _ => panic!("unexpected product path {path}"),
            }
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        roots,
        [
            "crates/liminal-sdk/src",
            "crates/liminal-server/src",
            "crates/liminal/src",
            "sdks/liminal-gleam",
            "sdks/liminal-ts",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>()
    );

    let mut observed_counts = Vec::new();
    for (expression, expected) in [
        ("recv_timeout", 28),
        ("try_recv", 2),
        ("poll", 151),
        ("yield_now", 12),
        ("sleep", 58),
        ("Instant::now", 96),
        ("read_timeout|set_read_timeout|read_one_frame", 29),
        ("wait_timeout", 0),
        ("park_timeout", 0),
        ("sleep_until", 0),
        ("SystemTime::now", 5),
        (r"\.elapsed[[:space:]]*\(", 10),
        ("setTimeout", 2),
        ("setInterval", 0),
    ] {
        let actual = law1_count(expression, false, &LAW1_PATHS);
        assert_eq!(actual, expected, "frozen LAW-1 expression {expression}");
        observed_counts.push(actual);
    }
    let typescript_paths = [":(glob)sdks/**/*.ts", ":(exclude,glob)sdks/*/tests/**"];
    let typescript_sleep = law1_count("sleep", false, &typescript_paths);
    assert_eq!(typescript_sleep, 5);
    observed_counts.push(typescript_sleep);
    let sweep_scan = law1_count(
        "([[:alnum:]_]*(sweep|scan)[[:alnum:]_]*)",
        true,
        &LAW1_PATHS,
    );
    assert_eq!(sweep_scan, 119);
    observed_counts.push(sweep_scan);
    let explicit_wait = law1_count(r"Condvar|\.wait[[:space:]]*\(", false, &LAW1_PATHS);
    assert_eq!(explicit_wait, 15);
    observed_counts.push(explicit_wait);
    for (word, expected) in [
        ("reconnect", 172),
        ("retry", 43),
        ("backoff", 27),
        ("delay", 109),
    ] {
        let expression = format!("([[:alnum:]_]*{word}[[:alnum:]_]*)");
        let actual = law1_count(&expression, true, &LAW1_PATHS);
        assert_eq!(actual, expected, "frozen LAW-1 vocabulary {word}");
        observed_counts.push(actual);
    }
    assert_eq!(observed_counts.len(), 21);

    // Each of the thirteen classified families must have concrete source
    // evidence at the pin. Together with the exact total grep counts above,
    // adding or dropping an unmatched lexical/structural wait fails this test.
    for (family, expression, paths) in [
        (
            "listener accept",
            "accept|sleep",
            &["crates/liminal-server/src/server/listener.rs"][..],
        ),
        (
            "membership poll",
            "poll|recv_timeout",
            &["crates/liminal-server/src/cluster/membership.rs"][..],
        ),
        (
            "health accept",
            "accept|read_timeout",
            &["crates/liminal-server/src/health/endpoint.rs"][..],
        ),
        (
            "shutdown drain",
            "recv_timeout|wait",
            &["crates/liminal-server/src/server/shutdown.rs"][..],
        ),
        (
            "channel reply",
            "try_recv|recv_timeout",
            &["crates/liminal/src/channel/actor/wait.rs"][..],
        ),
        (
            "SDK push reader",
            "read_timeout|recv_timeout",
            &["crates/liminal-sdk/src/remote/tcp/push_client.rs"][..],
        ),
        (
            "SDK subscription reader",
            "read_timeout|read_one_frame",
            &["crates/liminal-sdk/src/remote/tcp/subscription.rs"][..],
        ),
        (
            "durability bridge",
            "block_on|poll",
            &["crates/liminal/src/durability/bridge.rs"][..],
        ),
        (
            "push reply re-arm",
            "receive|recv_timeout",
            &["crates/liminal-sdk/src/remote/tcp/push_client.rs"][..],
        ),
        (
            "subscription setup",
            "read_one_frame|SETUP_TIMEOUT",
            &["crates/liminal-sdk/src/remote/tcp/subscription.rs"][..],
        ),
        (
            "TypeScript reconnect timer",
            "reconnect|setTimeout",
            &["sdks/liminal-ts/src/connection.ts"][..],
        ),
        (
            "Rust/Gleam reconnect delay contract",
            "reconnect|backoff|delay",
            &[
                "crates/liminal-sdk/src/connection/lifecycle.rs",
                "crates/liminal-sdk/src/remote/handles.rs",
                "sdks/liminal-gleam/src/liminal/connection.gleam",
            ][..],
        ),
        (
            "dedup expiry sweep",
            "sweep|scan",
            &["crates/liminal/src/durability/dedup/sweep.rs"][..],
        ),
    ] {
        assert!(
            !law1_grep(expression, true, paths).is_empty(),
            "LAW-1 family {family} lost its classified source evidence"
        );
    }
}

#[test]
fn acceptance_case_51_detached_attach_exhaustion_and_cursor_only_recovery_fence() {
    // Frozen PARTICIPANT-CONTRACT.md lines 4508-4616.
    const CONVERSATION: u64 = 51;
    const MAX: u64 = u64::MAX;
    let h = MAX - 7;
    let q = uniform(2);
    let k = uniform(2);
    let e5 = epoch(51, 5, 5);
    let e6 = epoch(51, 6, 6);

    let active_e5 = ActiveBinding {
        participant_id: P0,
        conversation_id: CONVERSATION,
        binding_epoch: e5,
    };
    let died_e5 = match active_e5.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(h - 5, h),
    )) {
        liminal_protocol::lifecycle::DiedBindingTransition::Committed(terminal) => terminal,
        liminal_protocol::lifecycle::DiedBindingTransition::Pending(_) => {
            panic!("pre-attach terminal is durable")
        }
    };
    let member = restored_member(CONVERSATION, 5, h - 2, Some(died_e5.into()));
    assert_floor(
        u128::from(h - 1),
        Some(h - 2),
        h,
        h - 2,
        u128::from(h - 1),
        u128::from(h - 1),
        u128::from(h - 1),
    );
    assert_eq!(sequence_budget(h, 1, 0, 0, 0, 0, 0, 0, 0).remaining, 7);
    let pre_baseline = assert_uniform_baseline(2, 0, 3);
    assert!(zero_debt_admission(pre_baseline, q, k, uniform(7)));

    let u51 = CredentialAttachRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(5),
        attach_secret: secret(5),
        attach_attempt_token: attach_token(0x51),
        accept_marker_delivery_seq: None,
    };
    assert_client_round_trip(ClientRequest::CredentialAttach(u51.clone()));
    let verified = member
        .verify_detached_attach(
            BindingState::Detached,
            ClosureState::Clear
                .ordinary_detached_attach_admission()
                .expect("clear state admits an ordinary detached attach"),
            u51.clone(),
            AttachSecretProof::Verified,
            AttachCommitParameters {
                binding: ActiveBinding {
                    participant_id: P0,
                    conversation_id: CONVERSATION,
                    binding_epoch: e6,
                },
                attach_secret: secret(6),
                attached_position: AttachedRecordPosition::new(h - 4, h + 1),
                receipt_expires_at: 1_000,
                provenance_expires_at: 2_000,
            },
        )
        .expect("U51 is exact detached credential authority");
    let attached = commit_attach(verified, DetachCell::<[u8; 32]>::default())
        .expect("production detached attach commits");
    assert_eq!(
        attached.binding_state,
        BindingState::Bound(ActiveBinding {
            participant_id: P0,
            conversation_id: CONVERSATION,
            binding_epoch: e6,
        })
    );
    assert_eq!(attached.member.generation(), generation(6));
    assert_eq!(attached.member.attach_secret(), secret(6));
    assert_eq!(attached.attached.delivery_seq(), h + 1);
    assert_eq!(attached.outcome.origin_binding_epoch(), e6);
    assert_eq!(attached.outcome.persisted_cursor(), h - 2);
    assert_eq!(attached.outcome.accepted_marker_delivery_seq(), None);

    let post_budget = sequence_budget(h + 1, 1, 1, 0, 1, 1, 1, 1, 0);
    assert_eq!(post_budget.remaining, 6);
    assert_eq!(
        [h + 2, h + 3, h + 4, h + 5, h + 6, h + 7],
        [MAX - 5, MAX - 4, MAX - 3, MAX - 2, MAX - 1, MAX]
    );
    assert_eq!(
        [h - 3, h - 2, h - 1, h],
        [MAX - 10, MAX - 9, MAX - 8, MAX - 7]
    );
    let post_baseline = assert_uniform_baseline(3, 0, 4);
    let post_capacity = mandatory_capacity(post_baseline, q, k, uniform(7));
    assert_eq!(post_capacity.debt, wide_uniform(1));
    assert!(post_capacity.is_legal());
    let op = ObserverProjection::new(h + 1);
    // Equality exhaustion at h=MAX-6 returns the exact ten-field budget and
    // common U51 envelope before any attach mutation.
    let exhaustion_h = MAX - 6;
    let exhausted_budget = sequence_budget(exhaustion_h + 1, 1, 1, 0, 1, 1, 1, 1, 0);
    assert_eq!(exhausted_budget.high_watermark, MAX - 5);
    assert_eq!(exhausted_budget.remaining, 5);
    let exhausted = ConversationSequenceExhausted {
        request: SequenceAllocatingEnvelope::CredentialAttach(attach_envelope(&u51)),
        sequence_budget: exhausted_budget,
    };
    assert_server_round_trip(ServerValue::ConversationSequenceExhausted(Box::new(
        exhausted,
    )));

    let active_e6 = ActiveBinding {
        participant_id: P0,
        conversation_id: CONVERSATION,
        binding_epoch: e6,
    };
    let died_e6 = match active_e6.connection_lost(BindingTerminalDisposition::Committed(
        CommittedBindingTerminalPosition::new(h - 3, h + 2),
    )) {
        liminal_protocol::lifecycle::DiedBindingTransition::Committed(terminal) => terminal,
        liminal_protocol::lifecycle::DiedBindingTransition::Pending(_) => {
            panic!("case 51 no-marker fate appends its claimed terminal")
        }
    };
    assert_eq!(died_e6.delivery_seq(), h + 2);
    assert_eq!(
        died_e6.cause(),
        liminal_protocol::wire::DiedCause::ConnectionLost
    );
    let fate_capacity = mandatory_capacity(wide_uniform(5), q, k, uniform(7));
    assert_eq!(fate_capacity.debt, wide_uniform(2));
    assert!(fate_capacity.is_legal());
    assert_floor(
        u128::from(h - 1),
        Some(h - 2),
        h + 2,
        h - 2,
        u128::from(h - 1),
        u128::from(h - 1),
        u128::from(h - 1),
    );
    let fate_budget = sequence_budget(h + 2, 1, 0, 0, 0, 0, 0, 0, 0);
    assert_eq!(fate_budget.remaining, 5);

    // Fate before OP consumes this case's exact preceding attach commit. OP
    // remains current with a latent DCursor suffix until its exact completion.
    let ordinary_fate = attached
        .ordinary_binding_fate(died_e6, h - 1)
        .expect("exact e6 terminal consumes the integrated U51 attach history");
    assert_eq!(ordinary_fate.through_seq(), h - 2);
    assert_eq!(ordinary_fate.resulting_floor(), h - 1);
    let pending_cursor_release = op.apply_ordinary_binding_fate(closure_debt(2), ordinary_fate);
    assert_eq!(
        pending_cursor_release.current_state(),
        ClosureState::Owed {
            debt: closure_debt(2),
            edge: StoredEdge::ObserverProjection(op),
        }
    );
    let dcursor_state = op
        .complete_after_ordinary_binding_fate(
            Event::projection_completed(h + 1),
            Some(closure_debt(2)),
            pending_cursor_release,
        )
        .expect("exact U51 OP completion installs the ordinary cursor suffix");
    let ClosureState::Owed {
        edge: StoredEdge::DetachedCursorRelease(dcursor),
        ..
    } = dcursor_state
    else {
        panic!("ordinary e6 fate must select DCursor after OP")
    };
    assert_eq!(dcursor.last_dead_binding_epoch(), e6);
    assert_eq!(
        dcursor.ordinary_attach_refusal(),
        DetachedAttachRefusal::RecoveryFence
    );
    assert_eq!(
        dcursor.marker_attach_refusal(),
        DetachedAttachRefusal::MarkerMismatch
    );
    assert_eq!(
        dcursor.binding_required_refusal(),
        DetachedAttachRefusal::NoBinding
    );

    let u51_n = CredentialAttachRequest {
        capability_generation: generation(6),
        attach_secret: secret(6),
        attach_attempt_token: attach_token(0x52),
        accept_marker_delivery_seq: None,
        ..u51
    };
    let u51_m = CredentialAttachRequest {
        attach_attempt_token: attach_token(0x53),
        accept_marker_delivery_seq: Some(h + 1),
        ..u51_n
    };
    assert_client_round_trip(ClientRequest::CredentialAttach(u51_n.clone()));
    let u51_some_marker_bytes =
        assert_client_round_trip(ClientRequest::CredentialAttach(u51_m.clone()));
    assert_eq!(u51_some_marker_bytes.len(), 97);
    let dcursor_snapshot = ClosureSnapshot {
        marker_capacity_credits: 0,
        marker_anchors: 0,
        entry_debt: 2,
        byte_debt: 2 * BM,
        repayment_edge: RepaymentEdge::DetachedCursorRelease {
            participant_id: P0,
            last_dead_binding_epoch: e6,
        },
        edge_sequence_claims: 1,
        edge_order_position_claims: 1,
        edge_k_remaining: k,
        k_headroom: wide_uniform(2),
        episode_churn_used: 0,
        delta_cycles: 0,
        episode_churn_limit: 2,
    };
    let recovery_fence = MarkerClosureCapacityExceeded {
        request: ClosureCheckedEnvelope::CredentialAttach(attach_envelope(&u51_n)),
        snapshot: dcursor_snapshot,
        reason: ClosureRefusalReason::RecoveryFence,
    };
    assert_server_round_trip(ServerValue::MarkerClosureCapacityExceeded(Box::new(
        recovery_fence,
    )));
    let marker_mismatch = MarkerMismatch {
        request: MarkerProofRequest::CredentialAttach(liminal_protocol::wire::AttachMarkerProof {
            conversation_id: CONVERSATION,
            token: u51_m.attach_attempt_token,
            participant_id: P0,
            capability_generation: generation(6),
            requested_marker_delivery_seq: h + 1,
        }),
        mismatch: MarkerMismatchBody::NoMarkerExpected,
    };
    assert_server_round_trip(ServerValue::MarkerMismatch(marker_mismatch));

    for no_binding in [
        NoBinding {
            request: BindingRequiredEnvelope::ParticipantAck(ParticipantAckEnvelope {
                conversation_id: CONVERSATION,
                participant_id: P0,
                capability_generation: generation(6),
                through_seq: h + 1,
            }),
        },
        NoBinding {
            request: BindingRequiredEnvelope::MarkerAck(MarkerAckEnvelope {
                conversation_id: CONVERSATION,
                participant_id: P0,
                capability_generation: generation(6),
                marker_delivery_seq: h + 1,
            }),
        },
        NoBinding {
            request: BindingRequiredEnvelope::RecordAdmission(RecordAdmissionEnvelope {
                conversation_id: CONVERSATION,
                participant_id: P0,
                capability_generation: generation(6),
            }),
        },
    ] {
        assert_server_round_trip(ServerValue::NoBinding(no_binding));
    }
    assert_client_round_trip(ClientRequest::MarkerAck(MarkerAck {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(6),
        marker_delivery_seq: h + 1,
    }));

    let l51 = LeaveRequest {
        conversation_id: CONVERSATION,
        participant_id: P0,
        capability_generation: generation(6),
        attach_secret: secret(6),
        leave_attempt_token: leave_token(0x51),
    };
    assert_client_round_trip(ClientRequest::Leave(l51));
    let leave_claim = dcursor
        .validate_leave_claim(P0, uniform(1), k, 1)
        .expect("durable terminal leaves one K-backed Left charge");
    let transfer = recovery_transfer(wide_uniform(2), k, uniform(1))
        .expect("detached Left transfers one K record after removals");
    assert_eq!(transfer.baseline, wide_uniform(3));
    assert_eq!(transfer.remaining_recovery_claim, uniform(1));
    assert_eq!(
        dcursor
            .leave(
                closure_debt(2),
                Event::detached_leave_committed(P0, h + 2),
                leave_claim,
                DebtCompletion::clear(),
            )
            .expect("DCursor's sole participant successor is detached Leave"),
        ClosureState::Clear
    );
    assert_floor(
        u128::from(h - 1),
        None,
        h + 3,
        h + 1,
        u128::from(h + 2),
        u128::from(h + 2),
        u128::from(h + 2),
    );
    assert!(no_edge_legal(
        WideResourceVector::default(),
        transfer.baseline,
        q,
        k,
        uniform(7),
    ));

    // Exact non-protocol attach gates and valid parking arithmetic from the
    // case are persisted by the consuming server/SDK, not this crate.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct AttachGates {
        connection_capacity: u64,
        connection_occupied: u64,
        receipt_limits: [u64; 2],
        receipt_occupied: [u64; 2],
        provenance_limits: [u64; 3],
        provenance_occupied: [u64; 3],
        receipt_ttl_ms: u64,
        provenance_ttl_ms: u64,
    }
    let gates = AttachGates {
        connection_capacity: 2,
        connection_occupied: 0,
        receipt_limits: [2, 2],
        receipt_occupied: [0, 0],
        provenance_limits: [2, 2, 2],
        provenance_occupied: [0, 0, 0],
        receipt_ttl_ms: 1_000,
        provenance_ttl_ms: 2_000,
    };
    assert_eq!(gates.connection_capacity - gates.connection_occupied, 2);
    assert!(
        gates
            .receipt_occupied
            .iter()
            .zip(gates.receipt_limits)
            .all(|(occupied, limit)| *occupied < limit)
    );
    assert!(
        gates
            .provenance_occupied
            .iter()
            .zip(gates.provenance_limits)
            .all(|(occupied, limit)| *occupied < limit)
    );
    assert!(gates.receipt_ttl_ms < gates.provenance_ttl_ms);
    let n = 4_u64;
    let g = 4_u64;
    let p = 1_u64;
    let pr = u64::try_from(u51_some_marker_bytes.len()).expect("PR fits u64");
    assert_eq!(pr, 97);
    let request_limit = pr.max(40);
    assert_eq!(request_limit, 97);
    let recovery_entry = 16_u64;
    let response_entry = 26_u64;
    let error_entry = 27_u64;
    let recovery_framing = 16_u64;
    let recovery_constant = 8_u64;
    assert_eq!((n, g, p), (4, 4, 1));
    // MR and PF are codec-generated symbols in the frozen case. Exercise the
    // exact B=R+MR and C=D=4B formula over both mandated MR bounds instead of
    // assigning RC(P)=8 to MR or silently assuming PF=128.
    for row_metadata in [1_u64, 1_048_576] {
        let row_bound = request_limit
            .checked_add(row_metadata)
            .expect("R+MR fits the case-51 fixture");
        let conversation_bytes = n
            .checked_mul(row_bound)
            .expect("N times B fits the case-51 fixture");
        let sdk_bytes = g
            .checked_mul(row_bound)
            .expect("G times B fits the case-51 fixture");
        assert_eq!(row_bound, 97 + row_metadata);
        assert_eq!(conversation_bytes, 4 * row_bound);
        assert_eq!(sdk_bytes, 4 * row_bound);
    }
    assert_eq!((recovery_entry, response_entry, error_entry), (16, 26, 27));
    let recovery_request_max = recovery_framing + recovery_constant + recovery_entry * p;
    let recovery_status_max =
        recovery_framing + recovery_constant + (response_entry * p).max(error_entry);
    assert_eq!(recovery_request_max, 40);
    assert_eq!(recovery_status_max, 51);
    for fixed_frame_max in [pr, 128, 1_048_576] {
        let wire_frame = fixed_frame_max.max(request_limit).max(128);
        assert_eq!(wire_frame, fixed_frame_max.max(97).max(128));
        assert!(wire_frame >= fixed_frame_max);
        assert!(wire_frame >= request_limit);
        assert!(wire_frame >= 128);
    }
    assert_eq!(dcursor_snapshot.marker_capacity_credits, 0);
    assert_eq!(dcursor_snapshot.marker_anchors, 0);
}
