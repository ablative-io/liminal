use liminal_protocol::lifecycle::{
    AttachCommitParameters, AttachSecretProof, BindingState, FencedAttachCommit, LiveMember,
};
use liminal_protocol::wire::CredentialAttachRequest;

fn verify_then_reuse(
    member: LiveMember<Vec<u8>>,
    binding: BindingState,
    request: CredentialAttachRequest,
    proof: FencedAttachCommit,
    parameters: AttachCommitParameters,
) {
    let verified = member.verify_fenced_attach(
        binding,
        request,
        AttachSecretProof::Verified,
        proof,
        None,
        parameters,
    );
    drop(verified);
    let participant_id = proof.participant_id();
    let _ = participant_id;
}

fn main() {
    let _ = verify_then_reuse;
}
