//! Board #37: provenance is retained only for receipts whose delivery was
//! OBSERVED, under the 2026-08-12 ruling's definition — the client
//! demonstrably possessed the secret that receipt minted.
//!
//! Only credential attach is secret-bearing against the slot's current
//! secret, and every committed attach supersedes the receipt that minted it
//! (`ops_attach_lookup::attach_token_phase` verifies a fresh attempt token
//! against `slot.attach_secret`, and only `AuthorizedFresh` reaches the
//! commit). So possession of a receipt's minted secret is proven by exactly
//! one event: the next committed attach. That makes the predicate structural
//! rather than a new fact to plumb —
//!
//! * enrollment receipt: proven ⟺ `slot.enrollment_receipt_ended.is_some()`
//! * attach receipt: proven ⟺ it has been retired into `slot.attach_provenance`
//! * the CURRENT receipt and an unended enrollment receipt: UNPROVEN
//!
//! and every input is rebuilt by cold replay from durable bytes, so retention
//! stays a pure function of durable bytes.
//!
//! Two families of pin live here. The OCCUPANCY pins prove unproven receipts
//! stop consuming stage-8 provenance slots and that proof promotes exactly
//! one. The CLASSIFICATION-NEUTRALITY pin proves R-C0's answers did not move:
//! occupancy and classification read different state, and this lane changes
//! only the former.

use std::error::Error;

use liminal_protocol::wire::{
    AttachSecret, ConnectionIncarnation, ReceiptCapacityExceeded, ReceiptCapacityScope,
    ServerValue, StaleAuthority,
};

use super::ProductionParticipantHandler;
use super::tests::{dispatch, open_disk_store_for_tests, test_participant_config};
use super::tests_capacity::capacity_config;
use super::tests_receipts::{GEN_ONE, attach, attach_request, detach, enroll, generation};

/// Asserts one exact credential-attach `ReceiptCapacityExceeded` row.
fn assert_attach_receipt_refusal(
    value: &ServerValue,
    scope: ReceiptCapacityScope,
    limit: u64,
    occupied: u64,
) -> Result<(), Box<dyn Error>> {
    let ServerValue::ReceiptCapacityExceeded(ReceiptCapacityExceeded::CredentialAttach {
        scope: got_scope,
        limit: got_limit,
        occupied: got_occupied,
        ..
    }) = value
    else {
        return Err(format!(
            "expected the credential-attach ReceiptCapacityExceeded row ({scope:?}), got: {value:?}"
        )
        .into());
    };
    assert_eq!(*got_scope, scope);
    assert_eq!(*got_limit, limit);
    assert_eq!(*got_occupied, occupied);
    Ok(())
}

/// THE LEAK, and its exact repair, in one fixture.
///
/// Two participants enroll in two conversations and neither ever attaches, so
/// neither client ever proved it possesses the secret its enrollment receipt
/// minted. Before this lane those two fingerprints occupied the server
/// provenance scope for the full `receipt_provenance_ttl_ms` — provenance
/// retained for a delivery that never happened — and with the cap at 1 the
/// scope was over-limit at 2, so the FIRST honest rotation was refused.
///
/// After it they occupy nothing, that rotation is admitted, and the very act
/// of admitting it proves possession of the enrollment secret and promotes
/// EXACTLY ONE fingerprint — which the second rotation then meets as an
/// exactly-full scope carrying `occupied: 1`.
///
/// The second half is load-bearing: without it this test would pass just as
/// well against a build that stopped counting provenance altogether.
#[test]
fn unproven_receipts_occupy_no_provenance_slot_and_proof_promotes_exactly_one()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(137, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    let config = capacity_config(|c| c.max_receipt_provenance_server = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 801;

    let receipt = enroll(&handler, incarnation, conversation_id, [0x71; 16])?;
    let participant_id = receipt.participant_id();
    // A second enrolled-and-never-attached participant, in its own
    // conversation, so the server scope sees two unproven fingerprints.
    enroll(&handler, incarnation, 802, [0x72; 16])?;

    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [0x73; 16],
    )?;
    // Unproven possession occupies nothing, so this honest rotation is
    // admitted against a server provenance cap of 1.
    let first = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [0x74; 16],
        ),
    )?;
    assert_eq!(first.capability_generation(), generation(2)?);

    // That attach proved possession of the enrollment secret: exactly one
    // fingerprint is now retained, and it fills the cap of 1.
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        generation(2)?,
        [0x75; 16],
    )?;
    let refused = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            first.attach_secret(),
            [0x76; 16],
        ),
    )?;
    assert_attach_receipt_refusal(&refused, ReceiptCapacityScope::ProvenanceServer, 1, 1)
}

/// The just-minted receipt is unproven BY CONSTRUCTION, so a participant that
/// attaches once holds exactly one retained fingerprint (its enrollment's),
/// never two.
///
/// This is the per-participant twin of the promotion arm above and it pins
/// the site the ruling names directly: the provenance window minted at
/// `ops_attach.rs`'s `install_attach_receipt` must not occupy while nothing
/// has yet verified against the secret it minted.
#[test]
fn the_just_minted_attach_receipt_holds_no_provenance_slot() -> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(138, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Cap 1 per participant: the enrollment fingerprint alone may occupy it.
    let config = capacity_config(|c| c.max_receipt_provenance_per_participant = 1);
    let handler = ProductionParticipantHandler::new(store, config)?;
    let conversation_id = 803;

    let receipt = enroll(&handler, incarnation, conversation_id, [0x77; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [0x78; 16],
    )?;
    let first = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [0x79; 16],
        ),
    )?;

    // One proven fingerprint (the enrollment's) fills the per-participant cap.
    // If the just-minted attach receipt ALSO occupied, the scope would be
    // over-limit at 2 and the refusal below would carry `occupied: 2`.
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        generation(2)?,
        [0x7A; 16],
    )?;
    let refused = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            first.attach_secret(),
            [0x7B; 16],
        ),
    )?;
    assert_attach_receipt_refusal(&refused, ReceiptCapacityScope::ProvenanceParticipant, 1, 1)
}

/// CLASSIFICATION NEUTRALITY (the ruling's binding constraint 1), measured
/// rather than intended.
///
/// Occupancy and classification read different state. This lane changes what
/// OCCUPIES a stage-8 provenance scope; it touches no input of R-C0's
/// token-phase resolution — not `slot.attach_provenance`, not
/// `attach.provenance_expires_at`, not `enrollment_provenance_expires_at`.
/// This pin holds that separation to the wire, over the two arms the ruling
/// enumerates, with every cap generous so occupancy can never be the reason
/// an answer changes.
///
/// Arm (i): an exact old attempt token, retired into provenance by a later
/// rotation, still resolves through the Provenance phase.
/// Arm (ii): an UNKNOWN token at a superseded generation is provably absent
/// from the complete in-window fingerprint set for that generation, so it
/// keeps the `StaleAuthority` no-commit proof rather than degrading to
/// `StaleOrUnknownReceipt`.
///
/// ⛔ Arm (ii) is the one a careless occupancy change breaks: its answer is
/// derived from `unmatched_token_phase`'s COMPLETENESS premise over retained
/// records. A build that dropped retained records to free slots would flip it
/// to `StaleOrUnknownReceipt` — which is exactly the red this pin was proved
/// against.
#[test]
fn r_c0_token_phase_classification_is_unchanged_by_observed_provenance_retention()
-> Result<(), Box<dyn Error>> {
    let home = tempfile::tempdir()?;
    let data_dir = home.path().join("durability");
    let incarnation = ConnectionIncarnation::new(139, 1);
    let store = open_disk_store_for_tests(&data_dir)?;
    // Deployment defaults: long windows, generous caps.
    let handler = ProductionParticipantHandler::new(store, test_participant_config())?;
    let conversation_id = 804;

    let receipt = enroll(&handler, incarnation, conversation_id, [0x7C; 16])?;
    let participant_id = receipt.participant_id();
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        GEN_ONE,
        [0x7D; 16],
    )?;
    let first = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            receipt.attach_secret(),
            [0x7E; 16],
        ),
    )?;
    // A second rotation retires the first attach receipt into its provenance
    // record, so its exact token resolves through the Provenance phase.
    detach(
        &handler,
        incarnation,
        conversation_id,
        participant_id,
        generation(2)?,
        [0x7F; 16],
    )?;
    let second = attach(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            generation(2)?,
            first.attach_secret(),
            [0x80; 16],
        ),
    )?;
    assert_eq!(second.capability_generation(), generation(3)?);

    // ⛔ Both arms present GEN_ONE deliberately. Its successor generation (2)
    // is witnessed ONLY by the retired record for token [0x7E; 16]. Presenting
    // generation 2 instead would make the CURRENT receipt the witness, and the
    // current receipt survives every retention change — which is exactly how
    // the first draft of this pin passed against a build that retained nothing
    // at all. The generation under test has to be one whose only witness is the
    // retained record.

    // Arm (i): the exact retired attempt token still resolves through the
    // Provenance phase and answers the R-C0 `ReceiptExpired` row.
    let exact_old = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            second.attach_secret(),
            [0x7E; 16],
        ),
    )?;
    assert!(
        matches!(exact_old, ServerValue::ReceiptExpired(_)),
        "an exact retired attempt token inside its provenance window must answer the \
         ReceiptExpired row from its retained fingerprint; retention stopped feeding \
         classification: {exact_old:?}"
    );

    // Arm (ii): an unknown token at the superseded generation keeps the
    // no-commit proof, which requires the in-window fingerprint set for that
    // generation to still be COMPLETE.
    let unknown_in_window = dispatch(
        &handler,
        incarnation,
        attach_request(
            conversation_id,
            participant_id,
            GEN_ONE,
            AttachSecret::new([0xAB; 32]),
            [0x81; 16],
        ),
    )?;
    let ServerValue::StaleAuthority(StaleAuthority::Live {
        current_generation, ..
    }) = unknown_in_window
    else {
        return Err(format!(
            "an unknown token at the superseded generation must keep the StaleAuthority \
             no-commit proof; the in-window fingerprint set is no longer complete: \
             {unknown_in_window:?}"
        )
        .into());
    };
    assert_eq!(current_generation, generation(3)?);
    Ok(())
}
