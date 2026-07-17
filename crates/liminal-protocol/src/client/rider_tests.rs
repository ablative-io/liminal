use super::*;
use crate::wire::{ClientDiscriminant, ClientRequest, EnrollmentRequest, EnrollmentToken};

type TestResult<T = ()> = Result<T, &'static str>;

fn token_bearing_abandonment_record() -> TestResult<ClientResumeRecord> {
    let mut aggregate = ClientParticipantAggregate::new();
    aggregate.restored_abandonment = Some(RestoredExpectedOperationAbandonment {
        request: ClientRequest::Enrollment(EnrollmentRequest {
            conversation_id: 41,
            enrollment_token: EnrollmentToken::new([42; 16]),
        }),
        reason: RestoredExpectedOperationAbandonmentReason::TokenlessAfterCrash,
        was_issued: true,
    });
    aggregate
        .resume_record()
        .map_err(|_| "token-bearing abandonment fixture must encode canonically")
}

#[test]
fn canonical_decode_rejects_token_bearing_abandonment() -> TestResult {
    let record = token_bearing_abandonment_record()?;
    assert_eq!(
        ClientResumeRecord::decode_canonical(&record.encode_canonical()),
        Err(ClientResumeRecordDecodeError::InvalidAbandonmentRequest {
            request: ClientDiscriminant::EnrollmentRequest,
        })
    );
    Ok(())
}

#[test]
fn canonical_restore_rejects_token_bearing_abandonment() -> TestResult {
    let record = token_bearing_abandonment_record()?;
    assert_eq!(
        record.restore(),
        Err(ClientResumeRestoreError::CorruptRecord(
            ClientResumeRecordDecodeError::InvalidAbandonmentRequest {
                request: ClientDiscriminant::EnrollmentRequest,
            },
        ))
    );
    Ok(())
}
