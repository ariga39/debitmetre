//! debitmetre — central transparent proxy gateway (single binary entry point).
//!
//! Startup order: CLI parse -> config load -> bind -> serve with graceful
//! shutdown. Any startup failure prints a useful error to stderr and exits
//! non-zero (startup fail-closed). Runtime logs are concise and never print
//! credentials or request/response bodies.

use std::path::PathBuf;
use std::process::ExitCode;

use debitmetre::config::{self, Config};
use debitmetre::summary;
use debitmetre::Gateway;

/// Default config path when `--config` is not given.
const DEFAULT_CONFIG_PATH: &str = "/etc/debitmetre/config.toml";

/// What the operator asked the binary to do.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    /// Run the gateway (current behavior).
    Run { config: PathBuf },
    /// Print accumulated token usage grouped by machine and model (issue #3).
    Summary { config: PathBuf },
}

/// CLI argument errors. Static messages only; argument values are never echoed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliError {
    Help,
    UnknownOption,
    MissingValue,
}

impl std::fmt::Display for CliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CliError::Help => Ok(()),
            CliError::UnknownOption => write!(f, "unknown option (see --help)"),
            CliError::MissingValue => write!(f, "--config requires a path argument"),
        }
    }
}

fn usage() -> &'static str {
    "debitmetre — central transparent proxy gateway for Codex clients\n\
     \n\
     USAGE:\n\
     \x20  debitmetre [--config <PATH>]          run the gateway\n\
     \x20  debitmetre summary [--config <PATH>]  print accumulated token usage by machine and model\n\
     \n\
     OPTIONS:\n\
     \x20  --config <PATH>   path to the TOML config (default: /etc/debitmetre/config.toml)\n\
     \x20  -h, --help        print this help"
}

fn parse_args(args: &[String]) -> Result<Command, CliError> {
    let mut config: Option<PathBuf> = None;
    let mut summary_command = false;
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "-h" || arg == "--help" {
            return Err(CliError::Help);
        }
        if arg == "summary" {
            summary_command = true;
            continue;
        }
        if arg == "--config" {
            let value = iter.next().ok_or(CliError::MissingValue)?;
            config = Some(PathBuf::from(value));
            continue;
        }
        if let Some(value) = arg.strip_prefix("--config=") {
            if value.is_empty() {
                return Err(CliError::MissingValue);
            }
            config = Some(PathBuf::from(value));
            continue;
        }
        return Err(CliError::UnknownOption);
    }
    let config = config.unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_PATH));
    if summary_command {
        Ok(Command::Summary { config })
    } else {
        Ok(Command::Run { config })
    }
}

fn init_logging() {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .with_target(false)
        .init();
}

/// Test-only seam (feature `test-upstream-override`): point the fixed upstream
/// at a fake upstream via `DEBITMETRE_TEST_UPSTREAM`. Production builds never
/// enable the feature, so the upstream stays fixed in code (SSRF prevention).
/// Opening the configured usage file is part of gateway construction: a bad
/// path fails startup (fail-closed, DESIGN.md §6).
fn build_gateway(cfg: &Config) -> Result<Gateway, String> {
    let open_usage_file = |gateway: Result<Gateway, std::io::Error>| {
        gateway.map_err(|err| format!("cannot open usage file {}: {err}", cfg.usage_file.display()))
    };
    #[cfg(feature = "test-upstream-override")]
    if let Ok(base) = std::env::var("DEBITMETRE_TEST_UPSTREAM") {
        let url = reqwest::Url::parse(&base).expect("DEBITMETRE_TEST_UPSTREAM must be a valid URL");
        return open_usage_file(Ok(Gateway::for_tests(
            url,
            cfg.machine_keys.clone(),
            &cfg.usage_file,
        )));
    }
    open_usage_file(Gateway::production(
        cfg.machine_keys.clone(),
        &cfg.usage_file,
    ))
}

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = match parse_args(&args) {
        Ok(command) => command,
        Err(CliError::Help) => {
            println!("{}", usage());
            return ExitCode::SUCCESS;
        }
        Err(err) => {
            eprintln!("debitmetre: {err}");
            eprintln!("{}", usage());
            return ExitCode::from(2);
        }
    };
    match command {
        Command::Run { config } => run_gateway(config).await,
        Command::Summary { config } => run_summary(config),
    }
}

/// Local summary command (issue #3): load the same gateway configuration and
/// print the accumulated token usage grouped by machine and model. Errors go to
/// stderr and exit non-zero; warnings are printed by the summary reader itself.
fn run_summary(config_path: PathBuf) -> ExitCode {
    let cfg = match config::load(&config_path) {
        Ok(cfg) => cfg,
        Err(err) => {
            eprintln!("debitmetre: {err}");
            return ExitCode::FAILURE;
        }
    };
    let mut stdout = std::io::stdout().lock();
    match summary::print_summary(&cfg.usage_file, &mut stdout) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("debitmetre: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run_gateway(config_path: PathBuf) -> ExitCode {
    init_logging();

    let cfg = match config::load(&config_path) {
        Ok(cfg) => cfg,
        Err(err) => {
            tracing::error!(
                event = "configuration_error",
                reason = %err,
                "configuration error"
            );
            return ExitCode::FAILURE;
        }
    };

    let app = match build_gateway(&cfg) {
        Ok(gateway) => gateway.router(),
        Err(err) => {
            tracing::error!(
                event = "configuration_error",
                reason = %err,
                "configuration error"
            );
            return ExitCode::FAILURE;
        }
    };

    let listener = match tokio::net::TcpListener::bind(cfg.listen).await {
        Ok(listener) => listener,
        Err(err) => {
            tracing::error!("cannot bind {}: {err}", cfg.listen);
            return ExitCode::FAILURE;
        }
    };
    let addr = match listener.local_addr() {
        Ok(addr) => addr,
        Err(err) => {
            tracing::error!("cannot resolve listener address: {err}");
            return ExitCode::FAILURE;
        }
    };

    tracing::info!(
        listen = %addr,
        machines = cfg.machine_keys.len(),
        "gateway listening"
    );

    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = stop_rx.await;
            })
            .await
    });

    tokio::select! {
        _ = shutdown_signal() => {
            tracing::info!("shutdown signal received; draining connections");
        }
        result = &mut server => {
            return match result {
                Ok(Ok(())) => {
                    tracing::info!("gateway stopped");
                    ExitCode::SUCCESS
                }
                Ok(Err(err)) => {
                    tracing::error!("server error: {err}");
                    ExitCode::FAILURE
                }
                Err(err) => {
                    tracing::error!("server task failed: {err}");
                    ExitCode::FAILURE
                }
            };
        }
    }

    let _ = stop_tx.send(());
    match server.await {
        Ok(Ok(())) => {
            tracing::info!("gateway stopped");
            ExitCode::SUCCESS
        }
        Ok(Err(err)) => {
            tracing::error!("server error: {err}");
            ExitCode::FAILURE
        }
        Err(err) => {
            tracing::error!("server task failed: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_flag_and_default_path() {
        assert_eq!(
            parse_args(&[]).unwrap(),
            Command::Run {
                config: PathBuf::from(DEFAULT_CONFIG_PATH)
            }
        );
        assert_eq!(
            parse_args(&["--config".to_string(), "/tmp/x.toml".to_string()]).unwrap(),
            Command::Run {
                config: PathBuf::from("/tmp/x.toml")
            }
        );
        assert_eq!(
            parse_args(&["--config=/tmp/y.toml".to_string()]).unwrap(),
            Command::Run {
                config: PathBuf::from("/tmp/y.toml")
            }
        );
    }

    #[test]
    fn summary_subcommand_shares_the_config_flag() {
        assert_eq!(
            parse_args(&["summary".to_string()]).unwrap(),
            Command::Summary {
                config: PathBuf::from(DEFAULT_CONFIG_PATH)
            }
        );
        assert_eq!(
            parse_args(&[
                "summary".to_string(),
                "--config".to_string(),
                "/tmp/s.toml".to_string()
            ])
            .unwrap(),
            Command::Summary {
                config: PathBuf::from("/tmp/s.toml")
            }
        );
        assert_eq!(
            parse_args(&[
                "--config".to_string(),
                "/tmp/s.toml".to_string(),
                "summary".to_string()
            ])
            .unwrap(),
            Command::Summary {
                config: PathBuf::from("/tmp/s.toml")
            }
        );
    }

    #[test]
    fn unknown_or_malformed_args_are_rejected() {
        assert!(matches!(
            parse_args(&["--bogus".to_string()]),
            Err(CliError::UnknownOption)
        ));
        assert!(matches!(
            parse_args(&["bogus".to_string()]),
            Err(CliError::UnknownOption)
        ));
        assert!(matches!(
            parse_args(&["--config".to_string()]),
            Err(CliError::MissingValue)
        ));
        assert!(matches!(
            parse_args(&["--config=".to_string()]),
            Err(CliError::MissingValue)
        ));
    }

    #[test]
    fn help_requests_usage() {
        assert!(matches!(
            parse_args(&["-h".to_string()]),
            Err(CliError::Help)
        ));
        assert!(matches!(
            parse_args(&["--help".to_string()]),
            Err(CliError::Help)
        ));
    }
}
