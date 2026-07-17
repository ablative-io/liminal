//! R1.1 upgrade-policy pins: the F6 origin case enumeration, the exact-path
//! rule, the no-subprotocol rule, and the fixed refusal statuses — pure,
//! socket-free (the socket-level handshake surface is exercised end-to-end in
//! `tests/ws_transport_e2e.rs` with raw HTTP bytes and a real client).

use tungstenite::handshake::server::Request;
use tungstenite::http::StatusCode;

use super::{AcceptorSettings, UpgradeRefusal, validate_upgrade_request};

fn settings(origins: &[&str]) -> AcceptorSettings {
    AcceptorSettings {
        path: "/liminal".to_owned(),
        allowed_origins: origins.iter().map(|origin| (*origin).to_owned()).collect(),
        ping_interval: None,
        message_bound: 1024,
    }
}

fn upgrade_request(path: &str, headers: &[(&str, &str)]) -> Result<Request, String> {
    let mut builder = tungstenite::http::Request::builder()
        .method("GET")
        .uri(path)
        .header("Host", "server.example.com")
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder.body(()).map_err(|error| error.to_string())
}

fn refusal_of(
    request: &Request,
    acceptor: &AcceptorSettings,
    case: &str,
) -> Result<UpgradeRefusal, String> {
    match validate_upgrade_request(request, acceptor) {
        Err(refusal) => Ok(refusal),
        Ok(()) => Err(format!("{case}: the upgrade must be refused")),
    }
}

// ---- F6: the four origin cases ----

#[test]
fn f6_listed_origin_passes() -> Result<(), String> {
    let request = upgrade_request("/liminal", &[("Origin", "https://app.example.com")])?;
    assert_eq!(
        validate_upgrade_request(&request, &settings(&["https://app.example.com"])),
        Ok(())
    );
    Ok(())
}

#[test]
fn f6_unlisted_origin_receives_typed_refusal() -> Result<(), String> {
    let request = upgrade_request("/liminal", &[("Origin", "https://evil.example.com")])?;
    let refusal = refusal_of(
        &request,
        &settings(&["https://app.example.com"]),
        "unlisted origin",
    )?;
    assert_eq!(
        refusal,
        UpgradeRefusal::OriginNotAllowed {
            origin: "https://evil.example.com".to_owned()
        }
    );
    assert_eq!(refusal.status(), StatusCode::FORBIDDEN);
    Ok(())
}

#[test]
fn f6_empty_allow_list_refuses_origin_bearing_upgrades_only() -> Result<(), String> {
    // Origin-bearing with NO configured list: fail closed.
    let browser = upgrade_request("/liminal", &[("Origin", "https://app.example.com")])?;
    let refusal = refusal_of(&browser, &settings(&[]), "fail-closed origin")?;
    assert_eq!(refusal.status(), StatusCode::FORBIDDEN);

    // Native client with no Origin header: passes regardless of configuration.
    let native = upgrade_request("/liminal", &[])?;
    assert_eq!(validate_upgrade_request(&native, &settings(&[])), Ok(()));
    Ok(())
}

#[test]
fn f6_null_opaque_origin_is_refused_unless_listed() -> Result<(), String> {
    let request = upgrade_request("/liminal", &[("Origin", "null")])?;
    let refusal = refusal_of(
        &request,
        &settings(&["https://app.example.com"]),
        "opaque origin",
    )?;
    assert_eq!(refusal.status(), StatusCode::FORBIDDEN);
    assert_eq!(
        validate_upgrade_request(&request, &settings(&["null"])),
        Ok(()),
        "a deployment may explicitly list the opaque origin"
    );
    Ok(())
}

#[test]
fn duplicate_origin_headers_are_refused() -> Result<(), String> {
    let request = upgrade_request(
        "/liminal",
        &[
            ("Origin", "https://app.example.com"),
            ("Origin", "https://app.example.com"),
        ],
    )?;
    let refusal = refusal_of(
        &request,
        &settings(&["https://app.example.com"]),
        "duplicate origins",
    )?;
    assert_eq!(refusal, UpgradeRefusal::DuplicateOriginHeader);
    assert_eq!(refusal.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

// ---- exact path and subprotocol rules ----

#[test]
fn wrong_path_is_refused_with_not_found() -> Result<(), String> {
    let request = upgrade_request("/other", &[])?;
    let refusal = refusal_of(&request, &settings(&[]), "wrong path")?;
    assert!(matches!(refusal, UpgradeRefusal::WrongPath { .. }));
    assert_eq!(refusal.status(), StatusCode::NOT_FOUND);
    Ok(())
}

#[test]
fn query_on_the_exact_path_is_refused() -> Result<(), String> {
    let request = upgrade_request("/liminal?token=1", &[])?;
    let refusal = refusal_of(&request, &settings(&[]), "query path")?;
    assert!(matches!(refusal, UpgradeRefusal::WrongPath { .. }));
    Ok(())
}

#[test]
fn offered_subprotocol_is_refused() -> Result<(), String> {
    let request = upgrade_request("/liminal", &[("Sec-WebSocket-Protocol", "chat")])?;
    let refusal = refusal_of(&request, &settings(&[]), "subprotocol")?;
    assert_eq!(refusal, UpgradeRefusal::SubprotocolOffered);
    assert_eq!(refusal.status(), StatusCode::BAD_REQUEST);
    Ok(())
}

// ---- refusal status table ----

#[test]
fn refusal_statuses_are_fixed_and_non_success() {
    let cases: Vec<(UpgradeRefusal, StatusCode)> = vec![
        (
            UpgradeRefusal::OversizedRequestHead { received: 9000 },
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE,
        ),
        (
            UpgradeRefusal::MalformedRequest {
                detail: "x".to_owned(),
            },
            StatusCode::BAD_REQUEST,
        ),
        (UpgradeRefusal::JunkAfterRequest, StatusCode::BAD_REQUEST),
        (
            UpgradeRefusal::NotAWebSocketUpgrade {
                detail: "x".to_owned(),
            },
            StatusCode::BAD_REQUEST,
        ),
        (
            UpgradeRefusal::WrongPath {
                requested: "/x".to_owned(),
            },
            StatusCode::NOT_FOUND,
        ),
        (
            UpgradeRefusal::DuplicateOriginHeader,
            StatusCode::BAD_REQUEST,
        ),
        (
            UpgradeRefusal::MalformedOriginHeader,
            StatusCode::BAD_REQUEST,
        ),
        (
            UpgradeRefusal::OriginNotAllowed {
                origin: "https://x".to_owned(),
            },
            StatusCode::FORBIDDEN,
        ),
        (UpgradeRefusal::SubprotocolOffered, StatusCode::BAD_REQUEST),
    ];
    for (refusal, expected) in cases {
        assert_eq!(refusal.status(), expected, "refusal: {refusal}");
        assert!(!refusal.status().is_success());
    }
}
