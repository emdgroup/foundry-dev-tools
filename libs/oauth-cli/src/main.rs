mod cache;
mod cli;
mod config;
mod error;
mod log;
mod oauth;
mod server;

use clap::{Parser, Subcommand};
use config::CliFlags;
use std::process;

#[derive(Parser)]
#[command(
    name = "foundry-dev-tools-oauth",
    about = "OAuth2 CLI for Palantir Foundry — obtain and manage Bearer tokens",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,

    /// Foundry hostname (e.g., foundry.example.com)
    #[arg(long, global = true)]
    hostname: Option<String>,

    /// OAuth2 client ID
    #[arg(long, global = true)]
    client_id: Option<String>,

    /// OAuth2 client secret (for confidential clients)
    #[arg(long, global = true)]
    client_secret: Option<String>,

    /// OAuth2 scopes (space-separated, default: "offline_access api:read-data")
    #[arg(long, global = true)]
    scopes: Option<String>,

    /// Local server port for OAuth callback (default: 8888)
    #[arg(long, global = true)]
    port: Option<u16>,

    /// Enable debug logging to {config_dir}/oauth-debug.log
    #[arg(long, global = true)]
    debug: bool,

    /// Use console mode instead of browser (for headless/SSH environments)
    #[arg(long, global = true)]
    no_browser: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Interactive login: open browser, complete OAuth2 flow, store refresh token
    Login,

    /// Output a fresh access token to stdout (refresh if needed)
    Token,

    /// Show authentication status and cached credentials
    Status,

    /// Clear stored credentials
    Logout,
}

fn main() {
    let cli = Cli::parse();

    let flags = CliFlags {
        hostname: cli.hostname,
        client_id: cli.client_id,
        client_secret: cli.client_secret,
        scopes: cli.scopes,
        port: cli.port,
        no_browser: cli.no_browser,
        debug: cli.debug,
    };

    let config = match config::Config::resolve(flags) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Configuration error: {}", e);
            process::exit(1);
        }
    };

    let result = match cli.command {
        Command::Login => cli::login(&config),
        Command::Token => cli::token(&config),
        Command::Status => cli::status(&config),
        Command::Logout => cli::logout(&config),
    };

    if let Err(e) = result {
        log::debug_log(
            config.debug,
            &config.config_dir,
            "EXIT",
            &format!("1 — {}", e),
        );
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
