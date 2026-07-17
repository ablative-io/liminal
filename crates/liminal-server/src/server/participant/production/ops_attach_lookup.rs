//! Credential-attach token-phase resolution and refusal mapping (split from
//! [`super::ops_attach`] under the 500-code-line lens).
//!
//! The token phase resolves the request against the CURRENT receipt's own
//! deadline pair, the retained provenance fingerprints, and the R-C0
//! completeness rule; the refusal mapper carries every classified fact FROM
//! the crate's lookup value into the request-bound response authority.

use liminal_protocol::lifecycle::{
    AttachSecretProof, CredentialAttachLookupResult, CredentialAttachProvenance,
    CredentialAttachTokenPhase, MarkerProofDecision, MarkerProofInput, MarkerProofState,
    ResolvedIdentity, select_marker_proof,
};
use liminal_protocol::wire::{
    AttachEnvelope, AttemptTokenBodyConflict as WireAttemptTokenBodyConflict, BindingEpoch,
    CredentialAttachRequest, CredentialAttachResponse, MarkerMismatch, MarkerNotDelivered,
    MarkerProofRequest, ReceiptExpired as WireReceiptExpired, ReceiptExpiryReason, ServerValue,
};

use super::barrier::OperationFacts;
use super::facts::{self, Digest};
use super::state::{Slot, StateError};

impl Slot {
    /// Resolves the credential-attach token phase and its phase-scoped
    /// constant-time secret proof.
    ///
    /// The token phase resolves against the CURRENT receipt's own deadline
    /// pair first, then against the retained provenance fingerprints of ended
    /// receipts. Each receipt's windows are fixed at its own commit; a later
    /// attach never re-opens them. The verifier is phase-scoped (contract
    /// R-C0): a live receipt replay verifies against the receipt's own
    /// committed presented secret (contract row 4 recovers a lost rotation
    /// with the invalidated OLD secret); every other phase verifies against
    /// the slot's current secret. Provenance phases ignore the proof by
    /// construction of their result path.
    pub(super) fn attach_token_phase(
        &self,
        request: &CredentialAttachRequest,
        now: u128,
    ) -> (
        CredentialAttachTokenPhase<'_, Digest, Digest, Digest>,
        AttachSecretProof,
    ) {
        let identity = ResolvedIdentity::<Digest, Digest, Digest>::Live(&self.member);
        let mut verifier_bytes = self.attach_secret.into_bytes();
        let token_phase = match self.attach.as_ref() {
            Some(attach) if attach.token == request.attach_attempt_token => {
                if now < attach.receipt_expires_at {
                    verifier_bytes = attach.verifier;
                    CredentialAttachTokenPhase::LiveReceipt {
                        identity,
                        receipt: &attach.receipt,
                    }
                } else if now < attach.provenance_expires_at {
                    CredentialAttachTokenPhase::Provenance {
                        identity,
                        provenance: CredentialAttachProvenance::new(
                            attach.result_generation,
                            ReceiptExpiryReason::Deadline,
                        ),
                    }
                } else {
                    CredentialAttachTokenPhase::AfterProvenance
                }
            }
            _ => match self
                .attach_provenance
                .get(&request.attach_attempt_token.into_bytes())
            {
                Some(record) if now < record.provenance_expires_at => {
                    CredentialAttachTokenPhase::Provenance {
                        identity,
                        provenance: CredentialAttachProvenance::new(
                            record.result_generation,
                            record.reason,
                        ),
                    }
                }
                Some(_) => CredentialAttachTokenPhase::AfterProvenance,
                None => self.unmatched_token_phase(request, now),
            },
        };
        let secret_proof =
            if facts::constant_time_eq(&verifier_bytes, &request.attach_secret.into_bytes()) {
                AttachSecretProof::Verified
            } else {
                AttachSecretProof::Mismatch
            };
        (token_phase, secret_proof)
    }

    /// Classifies a token with no receipt or fingerprint match (contract
    /// R-C0 completeness rule).
    ///
    /// `NoMatch` lets the lookup prove no-commit (`StaleAuthority`) for an
    /// old presented generation — legal exactly while the rotation FROM that
    /// generation is still inside its provenance window, because the
    /// in-window fingerprint set for the generation is then complete and
    /// this token is provably absent from it. Once that fingerprint's
    /// deadline has passed (whether the record is retained-expired or
    /// pruned), exact-old and unknown-old are intentionally indistinguishable
    /// and classify `AfterProvenance` (`StaleOrUnknownReceipt`, which claims
    /// no commit proof). Current-or-newer presented generations always take
    /// `NoMatch`: the ordinary generation/secret authority checks own them.
    fn unmatched_token_phase(
        &self,
        request: &CredentialAttachRequest,
        now: u128,
    ) -> CredentialAttachTokenPhase<'_, Digest, Digest, Digest> {
        let presented = request.capability_generation.get();
        if presented >= self.member.generation().get() {
            return CredentialAttachTokenPhase::NoMatch;
        }
        // Every generation advance is exactly one committed rotation, so the
        // rotation from the presented old generation minted presented + 1.
        let Some(successor) = presented.checked_add(1) else {
            // Unreachable: presented is strictly below a valid generation.
            return CredentialAttachTokenPhase::NoMatch;
        };
        let current_receipt_witnesses = self.attach.as_ref().is_some_and(|attach| {
            attach.result_generation.get() == successor && now < attach.provenance_expires_at
        });
        let retained_record_witnesses = self.attach_provenance.values().any(|record| {
            record.result_generation.get() == successor && now < record.provenance_expires_at
        });
        if current_receipt_witnesses || retained_record_witnesses {
            CredentialAttachTokenPhase::NoMatch
        } else {
            CredentialAttachTokenPhase::AfterProvenance
        }
    }
}

/// Classifies a marker-bearing (fenced-recovery) attach through the crate's
/// total marker-proof selector against the factual delivery state.
///
/// This binding delivers no markers yet (no delivery pump exists for
/// participant records), so the durable marker facts are empty: no expected
/// marker, no delivery witness. The crate selects the exact typed refusal; a
/// permitted fenced attach is unreachable until delivery exists, and
/// observing one is a loud invariant failure — never a silently hand-built
/// outcome.
pub(super) fn marker_bearing_attach_refusal(
    request: &CredentialAttachRequest,
    slot: &Slot,
    operation_facts: &OperationFacts,
) -> Result<ServerValue, StateError> {
    let Some(input) = MarkerProofInput::credential_attach(request) else {
        return Err(StateError::invariant(
            "marker-bearing attach classification without a presented marker",
        ));
    };
    let proof_epoch = BindingEpoch::new(
        operation_facts.receiving_incarnation,
        request.capability_generation,
    );
    let marker_state = MarkerProofState::new(slot.member.cursor(), false, None, proof_epoch, None);
    let response = match select_marker_proof(&marker_state, input) {
        MarkerProofDecision::MarkerMismatch(MarkerMismatch {
            request: MarkerProofRequest::CredentialAttach(proof),
            mismatch,
        }) => CredentialAttachResponse::marker_mismatch(proof, mismatch),
        MarkerProofDecision::MarkerNotDelivered(MarkerNotDelivered {
            request: MarkerProofRequest::CredentialAttach(proof),
            reason,
            expected_marker_delivery_seq,
        }) => CredentialAttachResponse::marker_not_delivered(
            proof,
            reason,
            expected_marker_delivery_seq,
        ),
        MarkerProofDecision::MarkerMismatch(_) | MarkerProofDecision::MarkerNotDelivered(_) => {
            return Err(StateError::invariant(
                "attach marker proof classified under a foreign operation envelope",
            ));
        }
        MarkerProofDecision::AckNoOp(_) => {
            return Err(StateError::invariant(
                "attach marker proof classified as a marker-ack no-op",
            ));
        }
        MarkerProofDecision::Permit(_) => {
            return Err(StateError::invariant(
                "marker proof permitted although no marker was ever delivered",
            ));
        }
    };
    Ok(response.into_server_value())
}

/// Maps a non-authorized credential-attach lookup onto its bound response.
///
/// Every classified fact travels FROM the crate's lookup value into the
/// response authority — the conflict kind, the provenance row's generations
/// and terminal reason — with no server-side re-derivation of any lifecycle
/// rule.
pub(super) fn credential_attach_refusal(
    lookup: &CredentialAttachLookupResult<'_, Digest>,
    envelope: AttachEnvelope,
    slot: &Slot,
) -> Result<ServerValue, StateError> {
    let response = match lookup {
        CredentialAttachLookupResult::ParticipantUnknown(_) => {
            CredentialAttachResponse::participant_unknown(envelope)
        }
        CredentialAttachLookupResult::StaleAuthority(_) => {
            CredentialAttachResponse::stale_authority(envelope, slot.member.generation())
        }
        CredentialAttachLookupResult::AttemptTokenBodyConflict(value) => {
            let WireAttemptTokenBodyConflict::CredentialAttach { conflict, .. } = value else {
                return Err(StateError::invariant(
                    "leave conflict row observed in the credential-attach lookup",
                ));
            };
            CredentialAttachResponse::attempt_token_body_conflict(&envelope, *conflict)
        }
        CredentialAttachLookupResult::Bound(_) => {
            let outcome = slot
                .attach
                .as_ref()
                .map(|attach| attach.outcome.clone())
                .ok_or_else(|| {
                    StateError::invariant("attach receipt replay without a stored receipt")
                })?;
            CredentialAttachResponse::bound(outcome)
        }
        CredentialAttachLookupResult::UnboundReceipt(_) => {
            let outcome = slot
                .attach
                .as_ref()
                .map(|attach| attach.outcome.clone())
                .ok_or_else(|| {
                    StateError::invariant("attach receipt replay without a stored receipt")
                })?;
            CredentialAttachResponse::unbound_receipt(outcome)
        }
        CredentialAttachLookupResult::ReceiptExpired(value) => {
            let WireReceiptExpired::CredentialAttach {
                result_generation,
                current_generation,
                reason,
                ..
            } = value
            else {
                return Err(StateError::invariant(
                    "enrollment provenance row observed in the credential-attach lookup",
                ));
            };
            CredentialAttachResponse::receipt_expired(
                &envelope,
                *result_generation,
                *current_generation,
                *reason,
            )
        }
        CredentialAttachLookupResult::StaleOrUnknownReceipt(value) => {
            CredentialAttachResponse::stale_or_unknown_receipt(value.clone())
        }
        CredentialAttachLookupResult::Retired(_) => {
            return Err(StateError::invariant(
                "retired identity observed in a binding that mints no tombstones",
            ));
        }
        CredentialAttachLookupResult::AuthorizedFresh { .. } => {
            return Err(StateError::invariant(
                "authorized attach routed through the refusal mapper",
            ));
        }
    };
    Ok(response.into_server_value())
}
