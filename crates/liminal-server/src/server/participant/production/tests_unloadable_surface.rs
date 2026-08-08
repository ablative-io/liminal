//! THE OPERATOR CAN SEE WHICH CONVERSATION THE NODE REFUSED TO LOAD.
//!
//! Containment already refuses one unloadable conversation on its own, names it
//! in the refusal, and keeps serving every other conversation
//! (`tests_boot_containment.rs`). The record it writes had no reader: the
//! accessor carried its own `#[allow(dead_code)]` saying so. A node that
//! contains a broken conversation and then cannot be ASKED which one is a node
//! whose containment an operator has to infer from log archaeology.
//!
//! This file pins the answer end to end, through the production construction
//! path rather than around it:
//!
//! ```text
//! store with one unloadable conversation
//!   -> LiminalConnectionServices::from_config_with_store   (production build)
//!   -> services.unloadable_conversation_record()           (the shared record)
//!   -> HealthServerHandle::install_unloadable_record       (the startup wiring)
//!   -> GET /unloadable-conversations                       (the operator)
//! ```
//!
//! The only production step not exercised here is `server/runtime.rs`'s call of
//! the installer, which cannot run without binding a whole server deployment.
//!
//! Each arm carries its own control: the surface is read BEFORE installation as
//! well as after, so the id it reports afterwards is provably the installed
//! record's answer and not a constant the route could have produced anyway.

use std::error::Error;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use liminal::durability::DurableStore;
use serde_json::Value;

use crate::config::types::{LimitsConfig, ServerConfig, ServicesConfig};
use crate::health::{SharedReadinessState, start_health_server};
use crate::server::connection::services::LiminalConnectionServices;

use super::tests::test_participant_config;
use super::tests_boot_containment::{
    FaultMode, HEALTHY_CONVERSATION, OneConversationFault, POISONED_CONVERSATION,
    seed_two_conversations,
};

/// A full-profile deployment whose ONLY configured capability is the
/// participant — the smallest config that makes `from_config_with_store` build
/// the production handler over a caller-supplied store.
fn participant_deployment() -> Result<ServerConfig, Box<dyn Error>> {
    Ok(ServerConfig {
        listen_address: "127.0.0.1:0".parse()?,
        health_listen_address: "127.0.0.1:0".parse()?,
        drain_timeout_ms: 30_000,
        channels: Vec::new(),
        routing_rules: Vec::new(),
        persistence_path: None,
        cluster: None,
        auth: None,
        services: ServicesConfig {
            profile: "full".to_owned(),
        },
        limits: LimitsConfig::default(),
        participant: Some(test_participant_config()),
        websocket: None,
    })
}

fn get(address: SocketAddr, path: &str) -> Result<Value, Box<dyn Error>> {
    let mut stream = TcpStream::connect(address)?;
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    stream.write_all(format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;

    if !response.starts_with("HTTP/1.1 200 ") {
        return Err(format!("{path} did not answer 200: {response}").into());
    }
    let Some((_headers, body)) = response.split_once("\r\n\r\n") else {
        return Err(format!("{path} response had no header/body separator: {response}").into());
    };
    Ok(serde_json::from_str(body)?)
}

/// A store with exactly one conversation the node cannot load, built through
/// the same fault the containment gate uses.
fn store_with_one_unloadable_conversation() -> Result<Arc<dyn DurableStore>, Box<dyn Error>> {
    let inner = seed_two_conversations()?;
    Ok(Arc::new(OneConversationFault::new(
        inner,
        POISONED_CONVERSATION,
        FaultMode::CorruptRecordNow(0),
    )))
}

/// THE PIN: a server built over a store holding one unloadable conversation
/// reports that conversation, by id, on the operator surface.
#[test]
fn the_health_surface_names_the_conversation_the_node_refused_to_load() -> Result<(), Box<dyn Error>>
{
    let services = LiminalConnectionServices::from_config_with_store(
        &participant_deployment()?,
        store_with_one_unloadable_conversation()?,
    )?;
    let record = services
        .unloadable_conversation_record()
        .ok_or("a configured participant must publish its refused-load record")?;

    let server = start_health_server("127.0.0.1:0".parse()?, SharedReadinessState::default())?;
    // THE CONTROL, read before installation: the same route, the same server,
    // and no record attached. Without it, the answer below would also be
    // satisfied by a route that reported this conversation unconditionally.
    let before = get(server.local_addr(), "/unloadable-conversations")?;
    server.install_unloadable_record(record);
    let after = get(server.local_addr(), "/unloadable-conversations")?;
    server.shutdown()?;

    assert_eq!(
        before["participant_installed"], false,
        "a server with no record attached must not claim to be reporting one: {before}"
    );
    assert_eq!(before["count"], 0, "control: {before}");

    assert_eq!(
        after["participant_installed"], true,
        "the installed record must be reported as installed: {after}"
    );
    assert_eq!(
        after["count"], 1,
        "exactly one conversation was unloadable; the surface must name that one and no \
         others: {after}"
    );
    assert_eq!(
        after["conversations"][0]["conversation_id"], POISONED_CONVERSATION,
        "the surface named the wrong conversation: {after}"
    );
    assert_eq!(
        after["conversations"][0]["class"], "internal",
        "the refusal class must ride as its own field, not only inside the message: {after}"
    );
    let Some(reason) = after["conversations"][0]["reason"].as_str() else {
        return Err(format!("the refusal carried no reason text: {after}").into());
    };
    assert!(
        !reason.is_empty(),
        "the refusal's own text must reach the operator: {after}"
    );

    // The healthy neighbour on the same store is NOT reported: containment
    // attributed the failure to one conversation, and so does the surface.
    let reported = after["conversations"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry["conversation_id"].as_u64())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    assert!(
        !reported.contains(&HEALTHY_CONVERSATION),
        "the healthy conversation {HEALTHY_CONVERSATION} was reported as unloadable: {reported:?}"
    );

    Ok(())
}

/// The other half of the same question: a deployment whose store is clean
/// reports an INSTALLED record with nothing in it — which is a different answer
/// from the control above, and the reason `participant_installed` exists.
#[test]
fn a_clean_store_reports_an_installed_but_empty_surface() -> Result<(), Box<dyn Error>> {
    let services = LiminalConnectionServices::from_config_with_store(
        &participant_deployment()?,
        seed_two_conversations()?,
    )?;
    let record = services
        .unloadable_conversation_record()
        .ok_or("a configured participant must publish its refused-load record")?;

    let server = start_health_server("127.0.0.1:0".parse()?, SharedReadinessState::default())?;
    server.install_unloadable_record(record);
    let status = get(server.local_addr(), "/unloadable-conversations")?;
    server.shutdown()?;

    assert_eq!(
        status["participant_installed"], true,
        "the record IS installed here: {status}"
    );
    assert_eq!(
        status["count"], 0,
        "a clean store refuses nothing: {status}"
    );

    Ok(())
}
