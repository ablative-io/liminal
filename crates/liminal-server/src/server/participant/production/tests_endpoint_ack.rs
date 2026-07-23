//! Acceptance item 28: cumulative ack endpoints must be durable obligations.

use std::error::Error;

use liminal_protocol::wire::{
    AttachAttemptToken, AttachSecret, ClientRequest, CredentialAttachRequest, DetachAttemptToken,
    DetachRequest, EnrollmentRequest, EnrollmentToken, Generation, ParticipantAck, RecordAdmission,
    RecordAdmissionAttemptToken, ServerValue,
};

use super::{SocketFixture, socket_fixture};

const CONVERSATION: u64 = 15_780_113;

struct EndpointObligationScenario {
    home: tempfile::TempDir,
    server: SocketFixture,
    peer: socket_fixture::SocketPeer,
    participant_zero: u64,
    participant_one: u64,
    participant_zero_secret: AttachSecret,
    participant_one_secret: AttachSecret,
}

impl EndpointObligationScenario {
    fn start() -> Result<Self, Box<dyn Error>> {
        let home = tempfile::tempdir()?;
        let data_dir = home.path().join("durability");
        let mut server = SocketFixture::start_replay_gated(&data_dir)?;
        let mut peer = server.spawn_peer()?;

        let enrolled_zero = server.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x28; 16]),
        }))?;
        let ServerValue::EnrollBound(zero) = enrolled_zero else {
            return Err(
                format!("participant zero enrollment did not bind: {enrolled_zero:?}").into(),
            );
        };
        let enrolled_one = peer.request(ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: CONVERSATION,
            enrollment_token: EnrollmentToken::new([0x29; 16]),
        }))?;
        let ServerValue::EnrollBound(one) = enrolled_one else {
            return Err(
                format!("participant one enrollment did not bind: {enrolled_one:?}").into(),
            );
        };
        assert_eq!(zero.participant_id(), 0);
        assert_eq!(one.participant_id(), 1);

        let acknowledged = server.request(ClientRequest::ParticipantAck(ParticipantAck {
            conversation_id: CONVERSATION,
            participant_id: zero.participant_id(),
            capability_generation: Generation::ONE,
            through_seq: 2,
        }))?;
        if !matches!(acknowledged, ServerValue::AckCommitted(_)) {
            return Err(
                format!("participant zero could not establish cursor 2: {acknowledged:?}").into(),
            );
        }
        let sender_excluded = server.request(ClientRequest::RecordAdmission(RecordAdmission {
            conversation_id: CONVERSATION,
            participant_id: zero.participant_id(),
            capability_generation: Generation::ONE,
            record_admission_attempt_token: RecordAdmissionAttemptToken::new([0x2A; 16]),
            payload: vec![0x2A],
        }))?;
        let ServerValue::RecordCommitted(sender_excluded) = sender_excluded else {
            return Err(
                format!("sender-excluded record did not commit: {sender_excluded:?}").into(),
            );
        };
        assert_eq!(sender_excluded.delivery_seq(), 3);
        assert_eq!(
            sender_excluded.sender_participant_id(),
            zero.participant_id()
        );

        Ok(Self {
            home,
            server,
            peer,
            participant_zero: zero.participant_id(),
            participant_one: one.participant_id(),
            participant_zero_secret: zero.attach_secret(),
            participant_one_secret: one.attach_secret(),
        })
    }

    fn assert_gap_unchanged(
        &mut self,
        capability_generation: Generation,
        through_seq: u64,
        current_cursor: u64,
    ) -> Result<(), Box<dyn Error>> {
        let before = self
            .server
            .participant_owner_facts(CONVERSATION, self.participant_zero)?;
        assert_eq!(before.frontier_cursor, current_cursor);
        let outcome = self
            .server
            .request(ClientRequest::ParticipantAck(ParticipantAck {
                conversation_id: CONVERSATION,
                participant_id: self.participant_zero,
                capability_generation,
                through_seq,
            }))?;
        let ServerValue::AckGap(gap) = outcome else {
            return Err(format!(
                "endpoint {through_seq} without a committed obligation was accepted: {outcome:?}"
            )
            .into());
        };
        assert_eq!(gap.request().conversation_id, CONVERSATION);
        assert_eq!(gap.request().participant_id, self.participant_zero);
        assert_eq!(gap.request().capability_generation, capability_generation);
        assert_eq!(gap.request().through_seq, through_seq);
        assert_eq!(gap.current_cursor(), current_cursor);
        let after = self
            .server
            .participant_owner_facts(CONVERSATION, self.participant_zero)?;
        assert_eq!(after, before, "AckGap must not mutate either owner");
        Ok(())
    }

    fn commit_real_obligation_at_four(&mut self) -> Result<(), Box<dyn Error>> {
        let detached = self.peer.request(ClientRequest::Detach(DetachRequest {
            conversation_id: CONVERSATION,
            participant_id: self.participant_one,
            capability_generation: Generation::ONE,
            detach_attempt_token: DetachAttemptToken::new([0x2B; 16]),
        }))?;
        let ServerValue::DetachCommitted(detached) = detached else {
            return Err(format!("participant one detach did not commit: {detached:?}").into());
        };
        assert_eq!(detached.detached_delivery_seq(), 4);
        let before = self
            .server
            .participant_owner_facts(CONVERSATION, self.participant_zero)?;
        assert_eq!(before.frontier_cursor, 2);
        assert_eq!(before.outbox_ack_through, 2);
        assert_eq!(before.next_live_obligation, Some(4));

        let outcome = self
            .server
            .request(ClientRequest::ParticipantAck(ParticipantAck {
                conversation_id: CONVERSATION,
                participant_id: self.participant_zero,
                capability_generation: Generation::ONE,
                through_seq: 4,
            }))?;
        if !matches!(outcome, ServerValue::AckCommitted(_)) {
            return Err(
                format!("real obligation 4 did not commit across gap 3: {outcome:?}").into(),
            );
        }
        let after = self
            .server
            .participant_owner_facts(CONVERSATION, self.participant_zero)?;
        assert_eq!(after.frontier_cursor, 4);
        assert_eq!(after.outbox_ack_through, 4);
        assert_eq!(after.next_live_obligation, None);
        Ok(())
    }

    fn reattach_after_sequence_six_commits_detached(
        &mut self,
    ) -> Result<Generation, Box<dyn Error>> {
        let detached = self.server.request(ClientRequest::Detach(DetachRequest {
            conversation_id: CONVERSATION,
            participant_id: self.participant_zero,
            capability_generation: Generation::ONE,
            detach_attempt_token: DetachAttemptToken::new([0x2C; 16]),
        }))?;
        let ServerValue::DetachCommitted(detached) = detached else {
            return Err(format!("participant zero detach did not commit: {detached:?}").into());
        };
        assert_eq!(detached.detached_delivery_seq(), 5);

        let attached_one =
            self.peer
                .request(ClientRequest::CredentialAttach(CredentialAttachRequest {
                    conversation_id: CONVERSATION,
                    participant_id: self.participant_one,
                    capability_generation: Generation::ONE,
                    attach_secret: self.participant_one_secret,
                    attach_attempt_token: AttachAttemptToken::new([0x2D; 16]),
                    accept_marker_delivery_seq: None,
                }))?;
        if !matches!(attached_one, ServerValue::AttachBound(_)) {
            return Err(
                format!("participant one did not commit sequence 6: {attached_one:?}").into(),
            );
        }
        let attached_zero =
            self.server
                .request(ClientRequest::CredentialAttach(CredentialAttachRequest {
                    conversation_id: CONVERSATION,
                    participant_id: self.participant_zero,
                    capability_generation: Generation::ONE,
                    attach_secret: self.participant_zero_secret,
                    attach_attempt_token: AttachAttemptToken::new([0x2E; 16]),
                    accept_marker_delivery_seq: None,
                }))?;
        let ServerValue::AttachBound(attached_zero) = attached_zero else {
            return Err(format!("participant zero did not reattach: {attached_zero:?}").into());
        };
        Ok(attached_zero.capability_generation())
    }

    fn stop(self) {
        let Self {
            home, server, peer, ..
        } = self;
        drop(peer);
        server.stop();
        drop(home);
    }
}

/// The guard is unchanged contract: an ack endpoint that is not a durable
/// obligation is refused with `AckGap`, leaving both owners untouched.
///
/// Only the SETUP is trued for the B1 ruled contract. Sequence 6 (participant
/// one's reattach) is produced while participant zero is resumable-Detached, so
/// zero now DOES hold a committed obligation at 6 — it is no longer a hole. The
/// genuine no-obligation endpoint beyond every obligation zero holds is sequence
/// 7, zero's own reattach record: a sender is excluded from its own recipient
/// snapshot, so zero holds no obligation at 7. Acking to 7 (with the cursor at
/// the real frontier 4) still returns `AckGap`.
#[test]
fn endpoint_with_no_committed_obligation_refuses() -> Result<(), Box<dyn Error>> {
    let mut scenario = EndpointObligationScenario::start()?;
    scenario.assert_gap_unchanged(Generation::ONE, 3, 2)?;
    scenario.commit_real_obligation_at_four()?;
    let reattached_generation = scenario.reattach_after_sequence_six_commits_detached()?;
    scenario.assert_gap_unchanged(reattached_generation, 7, 4)?;
    scenario.stop();
    Ok(())
}
