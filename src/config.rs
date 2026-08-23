use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::MachineKeys;

/// Fully validated runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Address the gateway HTTP listener binds to.
    pub listen: SocketAddr,
    /// SHA-256 digest (lowercase hex) of each meter key -> stable machine id.
    pub machine_keys: MachineKeys,
}

/// Load and validate the startup configuration from a TOML file. Any failure
/// returns a useful error so the process exits before serving partial state
/// (startup fail-closed).
pub fn load(path: impl AsRef<Path>) -> Result<Config, ConfigError> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path).map_err(|source| ConfigError {
        kind: ErrorKind::Io {
            path: path.to_owned(),
            source,
        },
    })?;
    let raw: RawConfig = toml::from_str(&contents).map_err(|source| ConfigError {
        kind: ErrorKind::Parse {
            path: path.to_owned(),
            source,
        },
    })?;
    Config::try_from(raw, path)
}

/// A 64-character lowercase hex string is the exact shape the gateway derives
/// for a meter key at runtime; anything else can never match a presented key
/// and is rejected at startup instead of failing silently at runtime.
fn is_digest_hex(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    listen: String,
    machine_keys: BTreeMap<String, String>,
}

impl Config {
    fn try_from(raw: RawConfig, path: &Path) -> Result<Config, ConfigError> {
        let validation = |message: String| ConfigError {
            kind: ErrorKind::Validation {
                path: path.to_owned(),
                message,
            },
        };

        let listen = raw.listen.parse::<SocketAddr>().map_err(|_| {
            validation("listen must be a valid IP:port address, e.g. \"127.0.0.1:8787\"".into())
        })?;

        if raw.machine_keys.is_empty() {
            return Err(validation(
                "machine_keys must map at least one meter key digest to a machine id".into(),
            ));
        }
        let mut machine_keys = BTreeMap::new();
        for (digest, machine_id) in raw.machine_keys {
            if !is_digest_hex(&digest) {
                return Err(validation(format!(
                    "machine_keys entry {digest:?} must be a 64-character lowercase hex SHA-256 digest"
                )));
            }
            if machine_id.trim().is_empty() {
                return Err(validation(format!(
                    "machine id for digest {digest} must not be blank"
                )));
            }
            machine_keys.insert(digest, machine_id);
        }

        Ok(Config {
            listen,
            machine_keys,
        })
    }
}

/// Startup configuration errors. Messages never echo credentials; a machine id
/// or digest is at most an identity hint, not a secret.
#[derive(Debug)]
pub struct ConfigError {
    kind: ErrorKind,
}

#[derive(Debug)]
enum ErrorKind {
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: PathBuf,
        source: toml::de::Error,
    },
    Validation {
        path: PathBuf,
        message: String,
    },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::Io { path, source } => {
                write!(f, "cannot read config {}: {}", path.display(), source)
            }
            ErrorKind::Parse { path, source } => {
                write!(f, "invalid TOML in {}: {}", path.display(), source)
            }
            ErrorKind::Validation { path, message } => {
                write!(f, "invalid config in {}: {message}", path.display())
            }
        }
    }
}

impl std::error::Error for ConfigError {}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use super::*;
    use crate::MachineKeys;

    const TEST_METER_KEY_DIGEST: &str =
        "82805ec33616c4aa802f141d3703fb17213fd8ced358f3a62348d8cf6e1ce051";

    fn write_config(dir: &tempfile::TempDir, body: &str) -> std::path::PathBuf {
        let path = dir.path().join("config.toml");
        std::fs::write(&path, body).expect("write synthetic config");
        path
    }

    #[test]
    fn valid_config_loads_listen_and_machine_keys() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_config(
            &dir,
            &format!(
                "listen = \"127.0.0.1:8787\"\n\n[machine_keys]\n\"{TEST_METER_KEY_DIGEST}\" = \"machine-a\"\n"
            ),
        );
        let cfg = load(&path).expect("synthetic config is valid");
        assert_eq!(
            cfg.listen,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8787)
        );
        assert_eq!(
            cfg.machine_keys,
            MachineKeys::from([(TEST_METER_KEY_DIGEST.to_string(), "machine-a".to_string())])
        );
    }

    #[test]
    fn missing_config_file_is_a_useful_error() {
        let err = load("/nonexistent/debitmetre/config.toml").expect_err("must fail closed");
        let text = err.to_string();
        assert!(
            text.contains("cannot read config"),
            "useful unreadable-config error, got: {text}"
        );
        assert!(text.contains("/nonexistent/debitmetre/config.toml"));
    }

    #[test]
    fn malformed_toml_is_a_useful_error() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_config(&dir, "listen = 127.0.0.1:8787\n[machine_keys\n");
        let err = load(&path).expect_err("must fail closed");
        assert!(
            err.to_string().contains("invalid TOML"),
            "useful parse error, got: {err}"
        );
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_config(
            &dir,
            "listen = \"127.0.0.1:8787\"\nupstream = \"https://chatgpt.com/\"\n",
        );
        let err = load(&path).expect_err("unknown field must be rejected");
        assert!(err.to_string().contains("invalid TOML"), "got: {err}");
    }

    #[test]
    fn invalid_listen_address_is_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_config(
            &dir,
            "listen = \"not-an-address\"\n\n[machine_keys]\n\"x\" = \"machine-a\"\n",
        );
        let err = load(&path).expect_err("invalid listen must fail");
        assert!(
            err.to_string().contains("listen"),
            "error names the listen field, got: {err}"
        );
    }

    #[test]
    fn empty_machine_keys_are_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_config(&dir, "listen = \"127.0.0.1:8787\"\n\n[machine_keys]\n");
        let err = load(&path).expect_err("no machine mapping must fail");
        assert!(
            err.to_string().contains("machine_keys"),
            "error names machine_keys, got: {err}"
        );
    }

    #[test]
    fn malformed_digest_keys_are_rejected() {
        for (label, digest) in [
            ("wrong length", "short"),
            (
                "uppercase hex",
                "82805EC33616C4AA802F141D3703FB17213FD8CED358F3A62348D8CF6E1CE051",
            ),
            (
                "non hex",
                "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz",
            ),
        ] {
            let dir = tempfile::TempDir::new().unwrap();
            let path = write_config(
                &dir,
                &format!(
                    "listen = \"127.0.0.1:8787\"\n\n[machine_keys]\n\"{digest}\" = \"machine-a\"\n"
                ),
            );
            let err = load(&path).expect_err("malformed digest must fail");
            assert!(
                err.to_string()
                    .contains("64-character lowercase hex SHA-256 digest"),
                "{label}: useful digest error, got: {err}"
            );
        }
    }

    #[test]
    fn blank_machine_ids_are_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = write_config(
            &dir,
            &format!(
                "listen = \"127.0.0.1:8787\"\n\n[machine_keys]\n\"{TEST_METER_KEY_DIGEST}\" = \"   \"\n"
            ),
        );
        let err = load(&path).expect_err("blank machine id must fail");
        assert!(
            err.to_string().contains("machine id"),
            "error names the machine id, got: {err}"
        );
    }
}
