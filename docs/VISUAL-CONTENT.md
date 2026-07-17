# Visual content plan

Placement suggestions for screenshots, terminal recordings, and
diagrams that would strengthen Liminal's documentation.

## README

| Location | Content | Format |
|---|---|---|
| After "What it is" | Architecture diagram: channel processes, conversation actors, durable mailboxes, supervision tree — showing the actor-per-conversation model | SVG (Excalidraw sketch aesthetic) |
| After "Features" | Terminal recording: start liminal-server, connect a client, publish to a channel with schema validation, subscribe and receive | asciinema or GIF, ~30s |
| After "Architecture" | Crate dependency graph: liminal-protocol (leaf) -> liminal / liminal-sdk -> liminal-server, with beamr and haematite as external deps | SVG |

## Wire protocol

| Location | Content | Format |
|---|---|---|
| Protocol docs | Sequence diagram: Subscribe -> Publish -> Deliver -> Accept/Defer/Reject showing the backpressure handshake | SVG or Mermaid |
| Protocol docs | Frame layout diagram: header fields, length prefix, payload region, causal metadata | SVG |

## Conversation lifecycle

| Location | Content | Format |
|---|---|---|
| Conversation design docs | State machine diagram: Created -> Active -> Completing -> Closed / Failed, with crash-recovery transitions | SVG |
| Conversation design docs | Sequence diagram: participant join, message exchange, crash detection via process link, conversation recovery | SVG |

## Backpressure

| Location | Content | Format |
|---|---|---|
| Pressure design docs | Diagram: slow consumer scenario — Accept/Defer/Reject flow between producer, channel, and consumer | SVG |
| Pressure design docs | Terminal recording: backpressure demo with a slow subscriber showing Defer/Reject in action | asciinema or GIF, ~20s |

## Durability

| Location | Content | Format |
|---|---|---|
| Durability design docs | Diagram: message lifecycle through haematite's content-addressed storage — publish, persist, crash, replay | SVG |
| Durability design docs | Terminal recording: publish messages, kill -9 the server, restart, show messages survived | asciinema or GIF, ~30s (the money shot) |

## SDK usage

| Location | Content | Format |
|---|---|---|
| TypeScript SDK | Code screenshot: connecting, subscribing, publishing with schema validation — syntax-highlighted in an editor | PNG, dark theme |
| Gleam SDK | Code screenshot: native BEAM client connecting to liminal — syntax-highlighted | PNG, dark theme |

## Video walkthrough ideas

| Topic | Duration | Audience |
|---|---|---|
| "Conversations, not queues" — what makes liminal different, with a live demo | 5 min | Developers evaluating messaging systems |
| "Durable mailboxes" — publish, crash, recover, nothing lost | 3 min | Infrastructure engineers |
| "Backpressure that works" — Accept/Defer/Reject in action under load | 5 min | Developers building AI agent coordination |
| "Liminal + Aion" — workflow steps coordinating through conversations | 8 min | Teams building on the Ablative stack |

## Tools

- **Terminal recordings**: [asciinema](https://asciinema.org) (renders as text, accessible) or [VHS](https://github.com/charmbracelet/vhs) (GIF/MP4 from a script)
- **Architecture diagrams**: hand-drawn SVG or [Excalidraw](https://excalidraw.com) for the sketch aesthetic
- **Sequence diagrams**: [Mermaid](https://mermaid.js.org) for version-controlled diagrams, or hand-drawn SVG
- **Screenshots**: macOS with a clean terminal (ghostty or iTerm2, dark theme matching the Ablative brand)
