# A5 decoder measurement — obligation 2 classification (2026-08-13 ~03:15Z)

**Question** (PARTICIPANT-CONTRACT.md §0.16 obligation 2, :924-927@d9585b5): at
PUBLISHED-CLIENT bytes, does the shipped decoder IGNORE or STRICT-REFUSE the
0x0202 MarkerSettled push and the two new refusal wrapper values?

**VERDICT: STRICT-REFUSE ⇒ all three new values ride the next planned protocol
breaking window alongside A4's build and `StateUnavailable { source }`.**

## Published bytes measured
- liminal-protocol 0.5.1 — sha256 `4fa498e4dab5f9a2bae27923d8d510a6b89b3f45db7a511fb89c1c82aa8e8c90`
- liminal-sdk 0.6.1 — sha256 `0b5fcc84c25984eb2ba7cc49128426f448e2e171d4f0d7102309360c35cb9f56`
- liminal-rs 0.5.5 — sha256 `60c1327aacd68cb9b190347cb2ceacb74d9553e443b56fc3d81f7a31c85011bf`
All checksums verified against the crates.io API (User-Agent set; serde as
positive control for the registry probe). liminal-rs has no liminal-protocol
dependency (its own src/protocol/) — the wire client is protocol + sdk.

## Executed probe (main.rs here, compiled against the SHA-verified unpacked 0.5.1 sources)
```
CONTROL  0x0200 well-formed (client): Ok(ObserverProgressed)
PROBE(a) 0x0202 unknown push  (client): Err(Decode { class: UnknownDiscriminant })
PROBE(b) 0x0130 unknown value (client): Err(Decode { class: UnknownDiscriminant })
```
Control proves the predicate can pass. Bonus discriminator run: 0x0110
(assigned = ConversationSequenceExhausted) fails LATER with InvalidField —
the tag match demonstrably runs before body decode.

## Mechanism at the published bytes (all cites @ liminal-protocol 0.5.1)
- `pub fn decode` at wire/codec.rs:340; client direction :389-397 tries
  PushDiscriminant then ServerDiscriminant then returns
  `Err(Decode{UnknownDiscriminant})` — NO skip/ignore arm exists.
- `u16_registry!` macro tags.rs:18-52: `TryFrom<u16>` errs on any unassigned
  value (:41-50).
- PushDiscriminant ends at 0x0201 (tags.rs:160-162); ServerDiscriminant spans
  0x0100-0x0124 (tags.rs:80-152), nothing past 0x0124 ⇒ 0x0202 and any new
  refusal-wrapper tag are unassigned at published bytes.
- Published crate's OWN tests pin the refusal: codec_tests.rs
  `wrong_direction_and_unassigned_values_are_unknown` (unassigned 0xFFFF →
  UnknownDiscriminant) and codec_server_push_acceptance_tests.rs:172-181.

## Propagation in the shipped SDK (read, cites @ liminal-sdk 0.6.1)
- tcp/participant.rs:60 and websocket/participant.rs:68: every inbound frame
  through `decode(..., ReceiverDirection::Client)`, error → SdkError via
  `codec_error` (fn `response_frame`).
- Both pump doors propagate: `receive_participant` framing.rs:219-223,
  `receive_participant_within` framing.rs:246-253. The frame is consumed at a
  clean boundary but the API surfaces an error — nothing is ignored.

## Field consequence
0x0202 is a PUSH: it arrives unprompted at every connected published client.
Shipped without the breaking window, the first settlement wake errors every
published pump. The refusal wrappers are request-correlated (only the refused
requester sees them) — same classification, smaller blast radius.
