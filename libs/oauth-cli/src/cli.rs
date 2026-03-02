use crate::cache;
use crate::config::Config;
use crate::error::{Error, Result};
use crate::log;
use crate::oauth;
use crate::server;
use std::io::{self, BufRead, Write};

/// Run the interactive login flow: open browser (or console mode), complete OAuth2, store tokens.
pub fn login(config: &Config) -> Result<()> {
    log::debug_log(
        config.debug,
        &config.config_dir,
        "STARTED",
        &format!(
            "login — hostname={}, scopes={}",
            config.hostname,
            config.scopes_str()
        ),
    );

    let pkce = oauth::generate_pkce();
    let state = oauth::generate_state();

    let (redirect_uri, code) = if config.no_browser {
        // Console mode: user manually copies auth code
        let redirect_uri = config.callback_url();
        let auth_url = oauth::build_authorization_url(config, &pkce, &state, &redirect_uri);

        eprintln!("Open this URL in your browser to authorize:");
        eprintln!();
        eprintln!("  {}", auth_url);
        eprintln!();
        eprint!("Paste the authorization code here: ");
        io::stderr().flush().ok();

        let mut code = String::new();
        io::stdin()
            .lock()
            .read_line(&mut code)
            .map_err(|_| Error::OAuthAuthorization("failed to read authorization code".into()))?;
        let code = code.trim().to_string();
        if code.is_empty() {
            return Err(Error::OAuthAuthorization("empty authorization code".into()));
        }

        (redirect_uri, code)
    } else {
        // Browser mode: start local server, open browser
        log::debug_log(
            config.debug,
            &config.config_dir,
            "LOGIN_TRIGGERED",
            "starting browser-based login",
        );

        let (port, callback) = {
            // Start the server first so we know the actual port
            let (port, _redirect_uri) = start_server_and_open_browser(config, &pkce, &state)?;
            // wait_for_callback blocks until the browser redirects back
            let (actual_port, callback) = server::wait_for_callback(port, 300)?;
            // The actual_port should match, but use what we got
            let _ = actual_port;
            (port, callback)
        };

        // Validate state
        if callback.state != state {
            return Err(Error::StateMismatch {
                expected: state,
                got: callback.state,
            });
        }

        let redirect_uri = config.local_redirect_uri(port);
        (redirect_uri, callback.code)
    };

    // Exchange the authorization code for tokens
    log::debug_log(
        config.debug,
        &config.config_dir,
        "LOGIN_PENDING",
        "exchanging authorization code for tokens",
    );
    let token_resp = oauth::exchange_code(config, &code, &pkce.code_verifier, &redirect_uri)?;

    // Save refresh token
    if let Some(ref refresh_token) = token_resp.refresh_token {
        cache::with_lock(&config.config_dir, config.debug, || {
            cache::save(
                &config.config_dir,
                &config.hostname,
                &config.client_id,
                &config.scopes,
                refresh_token,
                config.debug,
            )
        })?;
    }

    log::debug_log(
        config.debug,
        &config.config_dir,
        "LOGIN_OK",
        "login completed, refresh token saved",
    );
    eprintln!("Login successful! Tokens cached for {}.", config.hostname);

    Ok(())
}

/// Start the local callback server and open the browser to the authorization URL.
/// Returns the port the server is bound to.
fn start_server_and_open_browser(
    config: &Config,
    pkce: &oauth::Pkce,
    state: &str,
) -> Result<(u16, String)> {
    // We need to know the port before building the URL, but wait_for_callback
    // does the binding. We'll use a two-step approach: bind, build URL, open browser.
    // Actually, server::wait_for_callback already binds and waits. But we need
    // to open the browser *after* binding but *before* the callback arrives.
    //
    // Restructure: bind the server, open the browser, then wait for the callback.
    // This requires splitting wait_for_callback. For simplicity, we'll use a
    // thread: spawn the server wait in a thread, open the browser, then join.

    let port = config.port;
    let redirect_uri = config.local_redirect_uri(port);
    let auth_url = oauth::build_authorization_url(config, pkce, state, &redirect_uri);

    // Open browser (best effort)
    eprintln!("Opening browser for authentication...");
    if let Err(e) = open::that(&auth_url) {
        eprintln!("Failed to open browser: {}", e);
        eprintln!("Please open this URL manually:");
        eprintln!("  {}", auth_url);
    }

    Ok((port, redirect_uri))
}

/// Get a fresh access token, refreshing via cached refresh_token. Outputs token to stdout.
pub fn token(config: &Config) -> Result<()> {
    log::debug_log(
        config.debug,
        &config.config_dir,
        "STARTED",
        &format!(
            "token — hostname={}, scopes={}",
            config.hostname,
            config.scopes_str()
        ),
    );

    // Try refresh under the lock (fast path)
    let result = cache::with_lock(&config.config_dir, config.debug, || {
        refresh_cached_token(config)
    });

    let access_token = match result {
        Ok(token) => token,
        Err(Error::LoginRequired) | Err(Error::TokenRefresh { .. }) => {
            // No cached token or refresh failed — try auto-login OUTSIDE the lock
            // so the interactive browser flow doesn't hold the lock for 30+ seconds
            if let Err(ref e) = result {
                log::debug_log(
                    config.debug,
                    &config.config_dir,
                    "LOGIN_TRIGGERED",
                    &format!("attempting auto-login: {}", e),
                );
            }
            try_auto_login(config)?
        }
        Err(e) => return Err(e),
    };

    // Print access token to stdout (the ONLY thing that goes to stdout)
    println!("{}", access_token);
    log::debug_log(
        config.debug,
        &config.config_dir,
        "TOKEN_OUTPUT",
        "access token printed to stdout",
    );
    log::debug_log(config.debug, &config.config_dir, "EXIT", "0");

    Ok(())
}

/// Try to refresh using a cached token (called while holding the lock).
/// Returns LoginRequired if no cached token exists.
fn refresh_cached_token(config: &Config) -> Result<String> {
    let cached = cache::load(
        &config.config_dir,
        &config.hostname,
        &config.client_id,
        &config.scopes,
        config.debug,
    )?;

    match cached {
        Some(refresh_tok) => {
            log::debug_log(
                config.debug,
                &config.config_dir,
                "REFRESH_START",
                "sending refresh token request",
            );

            match oauth::refresh_token(config, &refresh_tok) {
                Ok(resp) => {
                    // Save the rotated refresh token
                    if let Some(ref new_refresh) = resp.refresh_token {
                        cache::save(
                            &config.config_dir,
                            &config.hostname,
                            &config.client_id,
                            &config.scopes,
                            new_refresh,
                            config.debug,
                        )?;
                    }
                    log::debug_log(
                        config.debug,
                        &config.config_dir,
                        "REFRESH_OK",
                        "new access token received",
                    );
                    Ok(resp.access_token)
                }
                Err(e) => {
                    log::debug_log(
                        config.debug,
                        &config.config_dir,
                        "REFRESH_FAIL",
                        &format!("{}", e),
                    );
                    eprintln!("Token refresh failed: {}", e);
                    Err(e)
                }
            }
        }
        None => Err(Error::LoginRequired),
    }
}

/// Attempt auto-login (interactive). If not possible, return LoginRequired error.
/// Called OUTSIDE the lock so the browser flow doesn't block other processes.
fn try_auto_login(config: &Config) -> Result<String> {
    // Check if we're in an interactive terminal
    if !atty_is_terminal() {
        eprintln!("No cached credentials and not running interactively.");
        eprintln!("Run `foundry-dev-tools-oauth login` in a terminal first.");
        return Err(Error::LoginRequired);
    }

    log::debug_log(
        config.debug,
        &config.config_dir,
        "LOGIN_TRIGGERED",
        "no valid token, starting interactive login",
    );
    eprintln!("No cached token found. Starting login flow...");

    // Run login flow (this may open a browser and wait — NOT under the lock)
    login(config)?;

    // After login, refresh under the lock to get an access token
    cache::with_lock(&config.config_dir, config.debug, || {
        let refresh_tok = cache::load(
            &config.config_dir,
            &config.hostname,
            &config.client_id,
            &config.scopes,
            config.debug,
        )?
        .ok_or(Error::LoginRequired)?;

        let resp = oauth::refresh_token(config, &refresh_tok)?;
        if let Some(ref new_refresh) = resp.refresh_token {
            cache::save(
                &config.config_dir,
                &config.hostname,
                &config.client_id,
                &config.scopes,
                new_refresh,
                config.debug,
            )?;
        }

        Ok(resp.access_token)
    })
}

/// Check if stderr is a terminal (heuristic for interactivity).
fn atty_is_terminal() -> bool {
    std::io::IsTerminal::is_terminal(&std::io::stderr())
}

/// Show authentication status.
pub fn status(config: &Config) -> Result<()> {
    let has_token = cache::load(
        &config.config_dir,
        &config.hostname,
        &config.client_id,
        &config.scopes,
        config.debug,
    )?
    .is_some();

    eprintln!("  Hostname:  {}", config.hostname);
    eprintln!("  Scopes:    {}", config.scopes_str());
    eprintln!("  Has token: {}", if has_token { "yes" } else { "no" });

    if !has_token {
        eprintln!();
        eprintln!("Run `foundry-dev-tools-oauth login` to authenticate.");
    }

    Ok(())
}

/// Clear stored credentials.
pub fn logout(config: &Config) -> Result<()> {
    log::debug_log(
        config.debug,
        &config.config_dir,
        "STARTED",
        &format!("logout — hostname={}", config.hostname),
    );

    let removed = cache::with_lock(&config.config_dir, config.debug, || {
        cache::delete(
            &config.config_dir,
            &config.hostname,
            &config.client_id,
            &config.scopes,
        )
    })?;

    if removed {
        eprintln!("Credentials for {} removed.", config.hostname);
    } else {
        eprintln!("No credentials found for {}.", config.hostname);
    }

    Ok(())
}
