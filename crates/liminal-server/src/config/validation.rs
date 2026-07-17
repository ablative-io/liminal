use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::ServerError;

use super::types::{LoadedSchema, ServerConfig, ServiceProfile};

/// Validates a fully loaded server configuration before startup.
///
/// Validation is intentionally limited to deterministic semantic checks and
/// filesystem inspection. It does not bind sockets, connect to peers, or perform
/// any other network I/O. Beyond the semantic checks it also resolves and loads
/// each channel's `schema_ref` from disk (relative to `base_dir`, or verbatim for
/// absolute paths), parses the JSON Schema document, and stores it on the channel
/// so the channel can later be built with a real schema. A missing, unreadable, or
/// non-JSON schema file is an accumulated validation error like any other.
///
/// `base_dir` is the directory the config file was loaded from; relative
/// `schema_ref` paths resolve against it. When it is `None` (e.g. a config
/// assembled in memory), relative paths resolve against the process working
/// directory, so callers that construct a config directly should use absolute
/// `schema_ref` paths.
///
/// # Errors
///
/// Returns [`ServerError::ConfigValidation`] containing all discovered validation
/// errors when the configuration is not safe to use for startup.
pub fn validate(config: &mut ServerConfig, base_dir: Option<&Path>) -> Result<(), ServerError> {
    let mut errors = Vec::new();

    validate_listen_address(config, &mut errors);
    validate_health_listen_address(config, &mut errors);
    validate_drain_timeout(config, &mut errors);
    validate_channels(config, &mut errors);
    validate_routing_rules(config, &mut errors);
    validate_persistence_path(config, &mut errors);
    validate_cluster(config, &mut errors);
    validate_auth(config, &mut errors);
    validate_services(config, &mut errors);
    config.limits.collect_errors(&mut errors);
    validate_participant(config, &mut errors);
    load_channel_schemas(config, base_dir, &mut errors);

    if errors.is_empty() {
        Ok(())
    } else {
        Err(ServerError::ConfigValidation {
            message: errors.join("; "),
        })
    }
}

/// Resolves, reads, and parses each channel's `schema_ref`, storing the loaded
/// document on the channel. Follows the same deterministic-local-FS discipline as
/// [`validate_persistence_path`]: every failure is accumulated rather than
/// short-circuiting, so an operator sees all schema problems at once.
fn load_channel_schemas(
    config: &mut ServerConfig,
    base_dir: Option<&Path>,
    errors: &mut Vec<String>,
) {
    for channel in &mut config.channels {
        let Some(schema_ref) = channel.schema_ref.as_ref() else {
            continue;
        };
        let path = resolve_schema_path(schema_ref, base_dir);
        match load_schema_document(&path) {
            Ok(loaded) => channel.loaded_schema = Some(loaded),
            Err(reason) => errors.push(format!(
                "channels.schema_ref '{}': {reason}",
                schema_ref.display()
            )),
        }
    }
}

/// Resolves a `schema_ref` to a concrete path: absolute refs are used verbatim,
/// relative refs are joined onto `base_dir` (or the working directory when there
/// is no base directory).
fn resolve_schema_path(schema_ref: &Path, base_dir: Option<&Path>) -> PathBuf {
    // `Path::join` returns `schema_ref` unchanged when it is absolute, so the
    // base-dir arm covers both the absolute and relative cases.
    base_dir.map_or_else(|| schema_ref.to_path_buf(), |dir| dir.join(schema_ref))
}

/// Reads, JSON-parses, and schema-compiles a schema file, returning the loaded
/// document or a human-readable reason on failure (missing/unreadable file,
/// invalid JSON, or valid JSON that is not a compilable JSON Schema). The
/// compile check runs here so every schema problem surfaces in the accumulated
/// validation pass instead of deferring to a different error class at channel
/// construction.
fn load_schema_document(path: &Path) -> Result<LoadedSchema, String> {
    let bytes = std::fs::read(path)
        .map_err(|error| format!("schema file '{}' is unreadable: {error}", path.display()))?;
    let document: serde_json::Value = serde_json::from_slice(&bytes).map_err(|error| {
        format!(
            "schema file '{}' is not valid JSON: {error}",
            path.display()
        )
    })?;
    liminal::channel::Schema::new(document.clone()).map_err(|error| {
        format!(
            "schema file '{}' is not a valid JSON Schema: {error}",
            path.display()
        )
    })?;
    Ok(LoadedSchema { bytes, document })
}

fn validate_listen_address(config: &ServerConfig, errors: &mut Vec<String>) {
    if config.listen_address.port() == 0 {
        errors.push("listen_address: port must be non-zero".to_owned());
    }
}

fn validate_health_listen_address(config: &ServerConfig, errors: &mut Vec<String>) {
    if config.health_listen_address.port() == 0 {
        errors.push("health_listen_address: port must be non-zero".to_owned());
    }

    if config.health_listen_address == config.listen_address {
        errors.push(
            "health_listen_address: must differ from listen_address for probe isolation".to_owned(),
        );
    } else if config.health_listen_address.port() == config.listen_address.port() {
        errors.push(
            "health_listen_address: port must differ from listen_address port for probe isolation"
                .to_owned(),
        );
    }
}

fn validate_drain_timeout(config: &ServerConfig, errors: &mut Vec<String>) {
    if config.drain_timeout_ms == 0 {
        errors.push("drain_timeout_ms: must be greater than zero".to_owned());
    }
}

fn validate_channels(config: &ServerConfig, errors: &mut Vec<String>) {
    let mut seen = BTreeSet::new();
    let mut duplicates = BTreeSet::new();

    for channel in &config.channels {
        let name = channel.name.trim();
        if name.is_empty() {
            errors.push("channels.name: channel name must not be empty".to_owned());
            continue;
        }

        if !seen.insert(name.to_owned()) {
            duplicates.insert(name.to_owned());
        }
    }

    if !duplicates.is_empty() {
        let names = duplicates.into_iter().collect::<Vec<_>>().join(", ");
        errors.push(format!("channels.name: duplicate channel names: {names}"));
    }
}

fn validate_routing_rules(config: &ServerConfig, errors: &mut Vec<String>) {
    let channel_names = config
        .channels
        .iter()
        .map(|channel| channel.name.as_str())
        .collect::<BTreeSet<_>>();

    for (index, rule) in config.routing_rules.iter().enumerate() {
        let source = rule.source_channel.trim();
        if source.is_empty() {
            errors.push(format!(
                "routing_rules[{index}].source_channel: source channel must not be empty"
            ));
        } else if !channel_names.contains(source) {
            errors.push(format!(
                "routing_rules[{index}].source_channel: unknown channel '{source}'"
            ));
        }

        let target = rule.target_channel.trim();
        if target.is_empty() {
            errors.push(format!(
                "routing_rules[{index}].target_channel: target channel must not be empty"
            ));
        } else if !channel_names.contains(target) {
            errors.push(format!(
                "routing_rules[{index}].target_channel: unknown channel '{target}'"
            ));
        }
    }
}

fn validate_persistence_path(config: &ServerConfig, errors: &mut Vec<String>) {
    let Some(path) = config.persistence_path.as_deref() else {
        return;
    };

    match std::fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                errors.push(format!(
                    "persistence_path '{}': path must be an existing directory",
                    path.display()
                ));
            } else if metadata.permissions().readonly() {
                errors.push(format!(
                    "persistence_path '{}': path is not writable",
                    path.display()
                ));
            }
        }
        Err(error) => {
            errors.push(format!(
                "persistence_path '{}': path is unreachable: {error}",
                path.display()
            ));
        }
    }
}

fn validate_cluster(config: &ServerConfig, errors: &mut Vec<String>) {
    let Some(cluster) = config.cluster.as_ref() else {
        return;
    };

    if cluster.node_name.trim().is_empty() {
        errors.push("cluster.node_name: node name must not be empty".to_owned());
    }

    if cluster.cookie.is_empty() {
        errors.push("cluster.cookie: distribution cookie must not be empty".to_owned());
    }

    if cluster.listen_address.port() == 0 {
        errors.push("cluster.listen_address: distribution port must be non-zero".to_owned());
    }

    if cluster.listen_address == config.listen_address {
        errors.push(
            "cluster.listen_address: distribution port must differ from the client listen_address"
                .to_owned(),
        );
    }

    let mut seed_node_counts = BTreeMap::new();
    for (index, seed_node) in cluster.seed_nodes.iter().enumerate() {
        if seed_node.port() == 0 {
            errors.push(format!(
                "cluster.seed_nodes[{index}]: seed node port must be non-zero"
            ));
        }
        seed_node_counts
            .entry(seed_node.to_string())
            .and_modify(|count| *count += 1)
            .or_insert(1_usize);
    }

    let duplicates = seed_node_counts
        .into_iter()
        .filter_map(|(seed_node, count)| (count > 1).then_some(seed_node))
        .collect::<Vec<_>>();

    if !duplicates.is_empty() {
        errors.push(format!(
            "cluster.seed_nodes: duplicate seed nodes: {}",
            duplicates.join(", ")
        ));
    }
}

/// Validates the optional `[auth]` section. When present its token must be
/// non-empty: an empty token would gate nothing (every client's empty `auth_token`
/// would match), so it is rejected rather than silently leaving the server open.
/// The token is not trimmed — a shared secret may legitimately contain leading or
/// trailing whitespace.
fn validate_auth(config: &ServerConfig, errors: &mut Vec<String>) {
    let Some(auth) = config.auth.as_ref() else {
        return;
    };

    if auth.token.is_empty() {
        errors.push("auth.token: authentication token must not be empty".to_owned());
    }
}

/// Validates the `[services]` profile selection.
///
/// An unrecognised `profile` value is a typed config validation error. When the
/// profile is `worker-front-door`, config that asks for machinery the profile does
/// not build is rejected via [`worker_front_door_field_errors`].
fn validate_services(config: &ServerConfig, errors: &mut Vec<String>) {
    let profile = match config.services.profile() {
        Ok(profile) => profile,
        Err(error) => {
            // `profile()` only ever yields a `ConfigValidation` carrying the bare
            // field message; surface it directly so it reads like every other
            // accumulated error rather than the wrapped `Display` prefix.
            match error {
                crate::ServerError::ConfigValidation { message } => errors.push(message),
                other => errors.push(other.to_string()),
            }
            return;
        }
    };

    if profile == ServiceProfile::WorkerFrontDoor {
        errors.extend(worker_front_door_field_errors(config));
    }
}

/// Semantic checks for the optional `[participant]` section: the shared
/// nonzero/ordering rules plus the protocol codec's own minimum-frame check on
/// `wire_frame_limit`, so an impossible limit fails at validation rather than
/// at service construction.
fn validate_participant(config: &ServerConfig, errors: &mut Vec<String>) {
    let Some(participant) = config.participant.as_ref() else {
        return;
    };
    participant.collect_errors(errors);
    if participant.wire_frame_limit != 0
        && let Err(error) = crate::server::participant::normalize_configured_frame_limit(
            participant.wire_frame_limit,
        )
    {
        errors.push(format!(
            "participant.wire_frame_limit: {} is below the protocol's minimum complete \
             participant frame ({error:?})",
            participant.wire_frame_limit
        ));
    }
}

/// Cross-field checks for the worker-front-door profile: config that asks for
/// machinery the profile does not build — channels, routing rules, a persistence
/// path, or a cluster — is rejected rather than silently ignored. The front door
/// constructs no channel, conversation, haematite, or distribution services, so
/// honouring any of those keys is impossible and accepting them quietly would be a
/// silent tradeoff.
///
/// Called from BOTH the file-loading validation pass ([`validate_services`]) and
/// the runtime construction path
/// ([`build_connection_services`](crate::server::connection::build_connection_services)),
/// so a directly-constructed `ServerConfig` that skips file validation still cannot
/// combine the worker profile with full-only machinery.
pub(crate) fn worker_front_door_field_errors(config: &ServerConfig) -> Vec<String> {
    let mut errors = Vec::new();
    if !config.channels.is_empty() {
        errors.push(
            "services.profile: \"worker-front-door\" builds no channels; remove the \
             [[channels]] entries or use profile = \"full\""
                .to_owned(),
        );
    }
    if !config.routing_rules.is_empty() {
        errors.push(
            "services.profile: \"worker-front-door\" builds no channels to route between; \
             remove the [[routing_rules]] entries or use profile = \"full\""
                .to_owned(),
        );
    }
    if config.persistence_path.is_some() {
        errors.push(
            "services.profile: \"worker-front-door\" builds no durable store; remove \
             persistence_path or use profile = \"full\""
                .to_owned(),
        );
    }
    if config.cluster.is_some() {
        errors.push(
            "services.profile: \"worker-front-door\" builds no channel cluster; remove the \
             [cluster] section or use profile = \"full\""
                .to_owned(),
        );
    }
    if config.participant.is_some() {
        errors.push(
            "services.profile: \"worker-front-door\" installs no participant service; remove \
             the [participant] section or use profile = \"full\""
                .to_owned(),
        );
    }
    errors
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::ServerError;

    use super::validate;
    use crate::config::types::{
        AuthConfig, ChannelDef, ClusterConfig, LimitsConfig, ParticipantConfig, RoutingRuleDef,
        ServerConfig, ServicesConfig,
    };

    static NEXT_TEMP_DIR_ID: AtomicU64 = AtomicU64::new(0);

    fn socket(address: &str) -> Result<SocketAddr, Box<dyn std::error::Error>> {
        Ok(address.parse()?)
    }

    fn sample_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
        Ok(ServerConfig {
            listen_address: socket("127.0.0.1:8080")?,
            health_listen_address: socket("127.0.0.1:8081")?,
            drain_timeout_ms: 30_000,
            channels: vec![ChannelDef {
                name: "orders".to_owned(),
                schema_ref: None,
                durable: true,
                loaded_schema: None,
            }],
            routing_rules: vec![RoutingRuleDef {
                source_channel: "orders".to_owned(),
                target_channel: "orders".to_owned(),
                predicate: None,
            }],
            persistence_path: None,
            cluster: Some(ClusterConfig {
                node_name: "node-a".to_owned(),
                listen_address: socket("127.0.0.1:9000")?,
                seed_nodes: vec![socket("127.0.0.1:9001")?],
                cookie: "test-cookie".to_owned(),
            }),
            auth: None,
            services: ServicesConfig::default(),
            limits: LimitsConfig::default(),
            participant: None,
        })
    }

    /// A worker-front-door config: no channels, routing, persistence, or cluster —
    /// the shape the front-door profile requires.
    fn worker_front_door_config() -> Result<ServerConfig, Box<dyn std::error::Error>> {
        Ok(ServerConfig {
            channels: Vec::new(),
            routing_rules: Vec::new(),
            persistence_path: None,
            cluster: None,
            services: ServicesConfig {
                profile: "worker-front-door".to_owned(),
            },
            ..sample_config()?
        })
    }

    fn unique_temp_dir(label: &str) -> PathBuf {
        let id = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "liminal-server-validation-{label}-{}-{id}",
            std::process::id()
        ))
    }

    fn config_validation_message(result: Result<(), ServerError>) -> String {
        let Err(ServerError::ConfigValidation { message }) = result else {
            return String::new();
        };
        message
    }

    #[test]
    fn valid_config_passes_validation() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;

        validate(&mut config, None)?;

        Ok(())
    }

    #[test]
    fn invalid_listen_address_reports_field_name() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.listen_address = socket("127.0.0.1:0")?;

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("listen_address"));
        assert!(message.contains("port"));

        Ok(())
    }

    #[test]
    fn invalid_health_listen_address_reports_field_name() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut config = sample_config()?;
        config.health_listen_address = socket("127.0.0.1:0")?;

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("health_listen_address"));
        assert!(message.contains("port"));

        Ok(())
    }

    #[test]
    fn matching_health_and_main_listen_addresses_are_rejected()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.health_listen_address = config.listen_address;

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("health_listen_address"));
        assert!(message.contains("listen_address"));

        Ok(())
    }

    #[test]
    fn matching_health_and_main_listen_ports_are_rejected() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut config = sample_config()?;
        config.health_listen_address = socket("0.0.0.0:8080")?;

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("health_listen_address"));
        assert!(message.contains("port"));

        Ok(())
    }

    #[test]
    fn zero_drain_timeout_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.drain_timeout_ms = 0;

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("drain_timeout_ms"));
        assert!(message.contains("greater than zero"));

        Ok(())
    }

    #[test]
    fn duplicate_channel_names_are_listed() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.channels.push(ChannelDef {
            name: "orders".to_owned(),
            schema_ref: None,
            durable: false,
            loaded_schema: None,
        });

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("duplicate"));
        assert!(message.contains("orders"));

        Ok(())
    }

    #[test]
    fn unreachable_persistence_path_reports_path() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        let path = unique_temp_dir("missing");
        config.persistence_path = Some(path.clone());

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("persistence_path"));
        assert!(message.contains(&path.display().to_string()));

        Ok(())
    }

    #[test]
    fn file_persistence_path_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        let path = unique_temp_dir("file");
        fs::write(&path, "not a directory")?;
        config.persistence_path = Some(path.clone());

        let message = config_validation_message(validate(&mut config, None));
        fs::remove_file(&path)?;

        assert!(message.contains("persistence_path"));
        assert!(message.contains("directory"));

        Ok(())
    }

    #[test]
    fn multiple_validation_errors_are_reported_together() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut config = sample_config()?;
        let missing_path = unique_temp_dir("multi-missing");
        config.listen_address = socket("127.0.0.1:0")?;
        config.channels.push(ChannelDef {
            name: "orders".to_owned(),
            schema_ref: None,
            durable: false,
            loaded_schema: None,
        });
        config.persistence_path = Some(missing_path.clone());

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("listen_address"));
        assert!(message.contains("duplicate channel names: orders"));
        assert!(message.contains(&missing_path.display().to_string()));

        Ok(())
    }

    #[test]
    fn routing_rules_reference_configured_channels() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.routing_rules[0].target_channel = "unknown".to_owned();

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("routing_rules[0].target_channel"));
        assert!(message.contains("unknown"));

        Ok(())
    }

    /// Writes `contents` to a fresh uniquely-named temp file and returns its path.
    fn write_temp_schema(
        label: &str,
        contents: &str,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path = unique_temp_dir(label).with_extension("json");
        fs::write(&path, contents)?;
        Ok(path)
    }

    #[test]
    fn absolute_schema_ref_is_loaded_and_parsed() -> Result<(), Box<dyn std::error::Error>> {
        let schema = r#"{"type":"object","properties":{"id":{"type":"integer"}}}"#;
        let schema_path = write_temp_schema("load-ok", schema)?;
        let mut config = sample_config()?;
        config.channels[0].schema_ref = Some(schema_path.clone());

        let result = validate(&mut config, None);
        fs::remove_file(&schema_path)?;
        result?;

        let loaded = config.channels[0]
            .loaded_schema
            .as_ref()
            .ok_or("schema should have been loaded onto the channel")?;
        assert_eq!(loaded.bytes, schema.as_bytes());
        assert_eq!(
            loaded.document.get("type").and_then(|t| t.as_str()),
            Some("object")
        );

        Ok(())
    }

    #[test]
    fn relative_schema_ref_resolves_against_base_dir() -> Result<(), Box<dyn std::error::Error>> {
        let dir = unique_temp_dir("relative-base");
        fs::create_dir_all(&dir)?;
        let schema = r#"{"type":"object"}"#;
        fs::write(dir.join("orders.json"), schema)?;

        let mut config = sample_config()?;
        config.channels[0].schema_ref = Some(PathBuf::from("orders.json"));

        let result = validate(&mut config, Some(&dir));
        fs::remove_dir_all(&dir)?;
        result?;

        assert!(config.channels[0].loaded_schema.is_some());

        Ok(())
    }

    #[test]
    fn missing_schema_ref_file_reports_validation_error() -> Result<(), Box<dyn std::error::Error>>
    {
        let missing = unique_temp_dir("missing-schema").with_extension("json");
        let mut config = sample_config()?;
        config.channels[0].schema_ref = Some(missing.clone());

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("schema_ref"));
        assert!(message.contains(&missing.display().to_string()));
        assert!(message.contains("unreadable"));

        Ok(())
    }

    #[test]
    fn invalid_json_schema_ref_reports_validation_error() -> Result<(), Box<dyn std::error::Error>>
    {
        let schema_path = write_temp_schema("bad-json", "{ this is not json")?;
        let mut config = sample_config()?;
        config.channels[0].schema_ref = Some(schema_path.clone());

        let message = config_validation_message(validate(&mut config, None));
        fs::remove_file(&schema_path)?;

        assert!(message.contains("schema_ref"));
        assert!(message.contains("not valid JSON"));

        Ok(())
    }

    #[test]
    fn valid_json_invalid_schema_ref_reports_validation_error()
    -> Result<(), Box<dyn std::error::Error>> {
        // Valid JSON that is not a compilable JSON Schema: a schema document
        // must be an object, so a bare array parses but fails compilation.
        let schema_path = write_temp_schema("bad-schema", "[]")?;
        let mut config = sample_config()?;
        config.channels[0].schema_ref = Some(schema_path.clone());

        let message = config_validation_message(validate(&mut config, None));
        fs::remove_file(&schema_path)?;

        assert!(message.contains("schema_ref"));
        assert!(message.contains("not a valid JSON Schema"));

        Ok(())
    }

    #[test]
    fn present_non_empty_auth_token_passes_validation() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.auth = Some(AuthConfig {
            token: "s3cr3t".to_owned(),
        });

        validate(&mut config, None)?;

        Ok(())
    }

    #[test]
    fn empty_auth_token_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.auth = Some(AuthConfig {
            token: String::new(),
        });

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("auth.token"));
        assert!(message.contains("must not be empty"));

        Ok(())
    }

    #[test]
    fn absent_auth_section_passes_validation() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.auth = None;

        validate(&mut config, None)?;

        Ok(())
    }

    #[test]
    fn default_profile_is_full_and_passes_validation() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;

        // The default services config resolves to the full profile.
        assert_eq!(
            config.services.profile()?,
            crate::config::types::ServiceProfile::Full
        );
        validate(&mut config, None)?;

        Ok(())
    }

    #[test]
    fn unknown_profile_is_a_validation_error() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.services = ServicesConfig {
            profile: "banana".to_owned(),
        };

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("services.profile"));
        assert!(message.contains("banana"));
        assert!(message.contains("worker-front-door"));

        Ok(())
    }

    #[test]
    fn worker_front_door_profile_with_empty_topology_passes()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut config = worker_front_door_config()?;

        assert_eq!(
            config.services.profile()?,
            crate::config::types::ServiceProfile::WorkerFrontDoor
        );
        validate(&mut config, None)?;

        Ok(())
    }

    #[test]
    fn worker_front_door_profile_rejects_channels_persistence_and_cluster()
    -> Result<(), Box<dyn std::error::Error>> {
        // Start from the full sample (channels + cluster present) but flip only the
        // profile: every full-mode-only knob must be rejected, not silently ignored.
        let mut config = sample_config()?;
        config.services = ServicesConfig {
            profile: "worker-front-door".to_owned(),
        };
        config.persistence_path = Some(PathBuf::from("/tmp"));

        let message = config_validation_message(validate(&mut config, None));

        assert!(message.contains("builds no channels"));
        assert!(message.contains("builds no durable store"));
        assert!(message.contains("builds no channel cluster"));

        Ok(())
    }

    #[test]
    fn default_limits_pass_validation_and_carry_signed_numbers()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        // The signed §5 defaults resolve from an absent `[limits]` section.
        assert_eq!(config.limits.max_connections, 256);
        assert_eq!(config.limits.max_subscriptions_per_connection, 32);
        assert_eq!(config.limits.max_conversations_per_connection, 32);
        assert_eq!(config.limits.max_pending_pushes_per_connection, 32);
        assert_eq!(
            config
                .limits
                .max_pending_conversation_replies_per_connection,
            32
        );
        assert_eq!(config.limits.max_pending_replies_per_conversation, 8);
        assert_eq!(config.limits.max_connection_inbox_bytes, 4 * 1024 * 1024);
        assert_eq!(config.limits.max_subscription_inbox_depth, 256);
        validate(&mut config, None)?;
        Ok(())
    }

    /// §5 cap-refusal (config half): every zero cap is a typed config validation
    /// error — the unlimited-by-silence state §5 outlaws — reported by field name.
    #[test]
    fn zero_limits_are_typed_config_errors() -> Result<(), Box<dyn std::error::Error>> {
        type LimitMutator = (&'static str, fn(&mut ServerConfig));
        let mutators: [LimitMutator; 8] = [
            ("max_connections", |c| c.limits.max_connections = 0),
            ("max_subscriptions_per_connection", |c| {
                c.limits.max_subscriptions_per_connection = 0;
            }),
            ("max_conversations_per_connection", |c| {
                c.limits.max_conversations_per_connection = 0;
            }),
            ("max_pending_pushes_per_connection", |c| {
                c.limits.max_pending_pushes_per_connection = 0;
            }),
            ("max_pending_conversation_replies_per_connection", |c| {
                c.limits.max_pending_conversation_replies_per_connection = 0;
            }),
            ("max_pending_replies_per_conversation", |c| {
                c.limits.max_pending_replies_per_conversation = 0;
            }),
            ("max_connection_inbox_bytes", |c| {
                c.limits.max_connection_inbox_bytes = 0;
            }),
            ("max_subscription_inbox_depth", |c| {
                c.limits.max_subscription_inbox_depth = 0;
            }),
        ];
        for (field, mutate) in mutators {
            let mut config = sample_config()?;
            mutate(&mut config);
            let message = config_validation_message(validate(&mut config, None));
            assert!(
                message.contains(&format!("limits.{field}")),
                "zero {field} must report a typed limits.{field} error, got: {message}"
            );
            assert!(
                message.contains("greater than zero"),
                "the {field} refusal must say why: {message}"
            );
        }
        Ok(())
    }

    /// A complete participant section with deployment-plausible nonzero values.
    const fn sample_participant() -> ParticipantConfig {
        ParticipantConfig {
            wire_frame_limit: 65_536,
            attach_receipt_ttl_ms: 60_000,
            receipt_provenance_ttl_ms: 600_000,
            max_live_attach_receipts_server: 1_024,
            max_live_attach_receipts_per_participant: 8,
            max_receipt_provenance_server: 4_096,
            max_receipt_provenance_per_conversation: 256,
            max_receipt_provenance_per_participant: 64,
            max_retired_identity_slots_server: 1_024,
            identity_slots: 4,
            observer_recovery_max_entries: 64,
            max_semantic_conversations_per_connection: 32,
        }
    }

    #[test]
    fn valid_participant_section_passes_validation() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        config.participant = Some(sample_participant());
        let temp_dir = std::env::temp_dir().join(format!(
            "liminal-server-participant-validation-{}-{}",
            std::process::id(),
            NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&temp_dir)?;
        config.persistence_path = Some(temp_dir.clone());
        let result = validate(&mut config, None);
        fs::remove_dir_all(&temp_dir)?;
        assert!(result.is_ok(), "expected valid config, got {result:?}");
        Ok(())
    }

    #[test]
    fn zero_participant_values_are_typed_config_errors() -> Result<(), Box<dyn std::error::Error>> {
        type ParticipantMutator = (&'static str, fn(&mut ParticipantConfig));
        let mutators: [ParticipantMutator; 12] = [
            ("wire_frame_limit", |p| p.wire_frame_limit = 0),
            ("attach_receipt_ttl_ms", |p| p.attach_receipt_ttl_ms = 0),
            ("receipt_provenance_ttl_ms", |p| {
                p.receipt_provenance_ttl_ms = 0;
            }),
            ("max_live_attach_receipts_server", |p| {
                p.max_live_attach_receipts_server = 0;
            }),
            ("max_live_attach_receipts_per_participant", |p| {
                p.max_live_attach_receipts_per_participant = 0;
            }),
            ("max_receipt_provenance_server", |p| {
                p.max_receipt_provenance_server = 0;
            }),
            ("max_receipt_provenance_per_conversation", |p| {
                p.max_receipt_provenance_per_conversation = 0;
            }),
            ("max_receipt_provenance_per_participant", |p| {
                p.max_receipt_provenance_per_participant = 0;
            }),
            ("max_retired_identity_slots_server", |p| {
                p.max_retired_identity_slots_server = 0;
            }),
            ("identity_slots", |p| p.identity_slots = 0),
            ("observer_recovery_max_entries", |p| {
                p.observer_recovery_max_entries = 0;
            }),
            ("max_semantic_conversations_per_connection", |p| {
                p.max_semantic_conversations_per_connection = 0;
            }),
        ];
        for (field, mutate) in mutators {
            let mut config = sample_config()?;
            let mut participant = sample_participant();
            mutate(&mut participant);
            config.participant = Some(participant);
            let message = config_validation_message(validate(&mut config, None));
            assert!(
                message.contains(&format!("participant.{field}")),
                "expected typed error for participant.{field}, got: {message}"
            );
        }
        Ok(())
    }

    #[test]
    fn provenance_ttl_shorter_than_receipt_is_a_typed_config_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        let mut participant = sample_participant();
        participant.receipt_provenance_ttl_ms = participant.attach_receipt_ttl_ms - 1;
        config.participant = Some(participant);
        let message = config_validation_message(validate(&mut config, None));
        assert!(
            message.contains("participant.receipt_provenance_ttl_ms"),
            "expected TTL-ordering error, got: {message}"
        );
        Ok(())
    }

    #[test]
    fn undersized_wire_frame_limit_is_a_typed_config_error()
    -> Result<(), Box<dyn std::error::Error>> {
        let mut config = sample_config()?;
        let mut participant = sample_participant();
        participant.wire_frame_limit = 1;
        config.participant = Some(participant);
        let message = config_validation_message(validate(&mut config, None));
        assert!(
            message.contains("participant.wire_frame_limit"),
            "expected minimum-frame error, got: {message}"
        );
        Ok(())
    }

    #[test]
    fn worker_front_door_rejects_participant_section() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = worker_front_door_config()?;
        config.participant = Some(sample_participant());
        let message = config_validation_message(validate(&mut config, None));
        assert!(
            message.contains("installs no participant service"),
            "expected worker-front-door participant rejection, got: {message}"
        );
        Ok(())
    }
}
