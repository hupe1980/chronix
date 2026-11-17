//! `chronixd` — the Chronix time-series database server.
//!
//! ```text
//! chronixd --data-dir /var/lib/chronix --bind 0.0.0.0:8086
//! chronixd --demo
//! chronixd inspect /var/lib/chronix
//! chronixd mcp --data-dir ./gateway-copy
//! ```

use clap::Parser;

use chronixd::cli::ServerArgs;

/// Chronix time-series database server.
#[derive(Parser, Debug)]
#[command(
    name = "chronixd",
    version,
    about = "Chronix time-series database server",
    args_conflicts_with_subcommands = true
)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,

    /// Serve a generated week of gateway data from a temporary directory,
    /// on loopback, without authentication. Deleted on exit.
    #[arg(long, env = "CHRONIXD_DEMO")]
    demo: bool,

    #[command(flatten)]
    server: ServerArgs,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Report what a data directory holds, without opening it.
    ///
    /// Safe on a directory a running server holds: nothing is written,
    /// locked or replayed.
    Inspect(chronixd::inspect_cmd::InspectArgs),

    /// Answer an AI agent over MCP on stdio, from a data directory opened
    /// read-only.
    ///
    /// For a directory copied off a device. A directory a running server
    /// holds is refused: connect to that server's `/mcp` instead.
    #[cfg(feature = "mcp")]
    Mcp(chronixd::mcp::McpArgs),
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let cli = Cli::parse();
    match &cli.command {
        Some(Command::Inspect(args)) => return args.run(),
        #[cfg(feature = "mcp")]
        Some(Command::Mcp(args)) => return args.run().await,
        None => {}
    }
    if cli.demo {
        // The demo's config is the default one, so the console is on whenever
        // the feature is compiled in.
        #[cfg(feature = "console")]
        let console = Some(chronixd::console::PATH);
        #[cfg(not(feature = "console"))]
        let console = None;
        return match chronixd::demo::run(&cli.server, console).await {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                let real_dir = matches!(
                    e.downcast_ref::<chronixd::demo::DemoError>(),
                    Some(chronixd::demo::DemoError::RealDirectory)
                );
                std::process::ExitCode::from(if real_dir { 2 } else { 1 })
            }
        };
    }
    let result = match cli.server.load() {
        Ok(config) => chronixd::cli::serve(&cli.server, config, Vec::new()).await,
        Err(e) => Err(e),
    };
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(e) => {
            // `Display`, not `Debug`: the `Debug` form escapes a config parse
            // error's newlines inside a tuple struct.
            eprintln!("chronixd: {e}");
            std::process::ExitCode::FAILURE
        }
    }
}
