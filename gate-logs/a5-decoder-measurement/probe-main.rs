use liminal_protocol::wire::{decode, ReceiverDirection, PARTICIPANT_FRAME_TYPE};

fn frame(discriminant: u16, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u16.to_be_bytes());
    payload.extend_from_slice(&0u16.to_be_bytes());
    payload.extend_from_slice(&discriminant.to_be_bytes());
    payload.extend_from_slice(body);
    let mut out = Vec::new();
    out.push(PARTICIPANT_FRAME_TYPE);
    out.push(0);
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

fn main() {
    // POSITIVE CONTROL: well-formed ObserverProgressed body (3x u64, layout from the
    // published crate's own acceptance test codec_server_push_acceptance_tests.rs:103-106)
    let mut observer_body = Vec::new();
    observer_body.extend_from_slice(&22u64.to_be_bytes());
    observer_body.extend_from_slice(&23u64.to_be_bytes());
    observer_body.extend_from_slice(&24u64.to_be_bytes());
    let control = decode(&frame(0x0200, &observer_body), ReceiverDirection::Client);
    println!("CONTROL  0x0200 well-formed (client): {}", if control.is_ok() { "Ok(ObserverProgressed)".into() } else { format!("{control:?}") });

    // PROBE (a): 0x0202 MarkerSettled — unknown push tag at 0.5.1
    let a = decode(&frame(0x0202, &observer_body), ReceiverDirection::Client);
    println!("PROBE(a) 0x0202 unknown push  (client): {a:?}");

    // PROBE (b): unassigned server-value discriminant (new refusal wrappers would be
    // new ServerDiscriminant values) — 0x0130 is unassigned at 0.5.1 (registry ends 0x0124)
    let b = decode(&frame(0x0130, &observer_body), ReceiverDirection::Client);
    println!("PROBE(b) 0x0130 unknown value (client): {b:?}");
}
