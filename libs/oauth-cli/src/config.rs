use crate::error::{Error, Result};
use std::path::PathBuf;

/// Default OAuth2 scopes requested when none are explicitly provided.
pub const DEFAULT_SCOPES: &[&str] = &["offline_access", "api:read-data"];

/// Resolved configuration for the CLI.
#[derive(Debug, Clone)]
pub struct Config {
    pub hostname: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub scopes: Vec<String>,
    pub config_dir: PathBuf,
    pub port: u16,
    pub no_browser: bool,
    pub debug: bool,
    /// CLI flags that were explicitly passed (not from env vars),
    /// formatted as command-line arguments for display in error messages.
    pub explicit_cli_args: String,
}

/// Raw values from CLI flags (all optional — flags override env vars).
#[derive(Debug, Default)]
pub struct CliFlags {
    pub hostname: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub scopes: Option<String>,
    pub port: Option<u16>,
    pub no_browser: bool,
    pub debug: bool,
}

/// Resolve the OAuth config directory by checking for an existing `config.toml`
/// in the same candidate directories the Python side uses (config.py:63-66).
/// Returns `{config_parent}/oauth/` where `config_parent` is the first directory
/// containing a `config.toml`, or `~/.foundry-dev-tools/` as the default.
fn resolve_config_dir() -> PathBuf {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));

    // Candidates in the same order as the Python side (config.py:63-66)
    let candidates = [
        home.join(".foundry-dev-tools"),
        home.join(".config").join("foundry-dev-tools"),
        dirs::config_dir()
            .unwrap_or_else(|| home.join(".config"))
            .join("foundry-dev-tools"),
    ];

    // Use the first dir where config.toml exists (matches Python behavior)
    for candidate in &candidates {
        if candidate.join("config.toml").exists() {
            return candidate.join("oauth");
        }
    }

    // No config.toml found anywhere — default to ~/.foundry-dev-tools/oauth/
    candidates[0].join("oauth")
}

/// Validate that the hostname is a plain domain name, not a URL or path-injected string.
/// Rejects any hostname containing characters that could cause URL injection when
/// interpolated into `https://{hostname}/multipass/api/...`.
fn validate_hostname(hostname: &str) -> Result<()> {
    if hostname.is_empty() {
        return Err(Error::InvalidHostname {
            hostname: hostname.to_string(),
        });
    }

    // Must contain only valid domain characters: alphanumeric, hyphens, dots
    let is_valid = hostname
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.');

    if !is_valid {
        return Err(Error::InvalidHostname {
            hostname: hostname.to_string(),
        });
    }

    // Must not start or end with a dot or hyphen
    if hostname.starts_with('.')
        || hostname.starts_with('-')
        || hostname.ends_with('.')
        || hostname.ends_with('-')
    {
        return Err(Error::InvalidHostname {
            hostname: hostname.to_string(),
        });
    }

    Ok(())
}

impl Config {
    /// Build a resolved Config by merging: CLI flags → FDT env vars → defaults.
    pub fn resolve(flags: CliFlags) -> Result<Self> {
        // Track which args were explicitly passed as CLI flags
        let mut cli_args = Vec::new();

        let hostname = flags
            .hostname
            .inspect(|h| cli_args.push(format!("--hostname {h}")))
            .or_else(|| std::env::var("FDT_CREDENTIALS__DOMAIN").ok())
            .ok_or(Error::MissingConfig(
                "hostname (--hostname or FDT_CREDENTIALS__DOMAIN)",
            ))?;

        validate_hostname(&hostname)?;

        let client_id = flags
            .client_id
            .inspect(|c| cli_args.push(format!("--client-id {c}")))
            .or_else(|| std::env::var("FDT_CREDENTIALS__OAUTH__CLIENT_ID").ok())
            .ok_or(Error::MissingConfig(
                "client_id (--client-id or FDT_CREDENTIALS__OAUTH__CLIENT_ID)",
            ))?;

        let client_secret = flags
            .client_secret
            .inspect(|s| cli_args.push(format!("--client-secret {s}")))
            .or_else(|| std::env::var("FDT_CREDENTIALS__OAUTH__CLIENT_SECRET").ok());

        let scopes = flags
            .scopes
            .inspect(|s| cli_args.push(format!("--scopes \"{s}\"")))
            .or_else(|| std::env::var("FDT_CREDENTIALS__OAUTH__SCOPES").ok())
            .map(|s| s.split_whitespace().map(String::from).collect())
            .unwrap_or_else(|| DEFAULT_SCOPES.iter().map(|s| String::from(*s)).collect());

        let config_dir = resolve_config_dir();

        let port = flags
            .port
            .inspect(|p| cli_args.push(format!("--port {p}")))
            .or_else(|| {
                std::env::var("FDT_CREDENTIALS__OAUTH__PORT")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(9876);

        if flags.no_browser {
            cli_args.push("--no-browser".to_string());
        }
        if flags.debug {
            cli_args.push("--debug".to_string());
        }

        let debug = flags.debug
            || std::env::var("FDT_CREDENTIALS__OAUTH__DEBUG")
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);

        let explicit_cli_args = if cli_args.is_empty() {
            String::new()
        } else {
            format!(" {}", cli_args.join(" "))
        };

        Ok(Config {
            hostname,
            client_id,
            client_secret,
            scopes,
            config_dir,
            port,
            no_browser: flags.no_browser,
            debug,
            explicit_cli_args,
        })
    }

    /// Scopes as a space-delimited string (for OAuth requests).
    pub fn scopes_str(&self) -> String {
        self.scopes.join(" ")
    }

    /// Authorization endpoint URL.
    pub fn authorize_url(&self) -> String {
        format!("https://{}/multipass/api/oauth2/authorize", self.hostname)
    }

    /// Token endpoint URL.
    pub fn token_url(&self) -> String {
        format!("https://{}/multipass/api/oauth2/token", self.hostname)
    }

    /// Callback URL used for console (no-browser) mode.
    pub fn callback_url(&self) -> String {
        format!("https://{}/multipass/api/oauth2/callback", self.hostname)
    }

    /// Local redirect URI for browser-based flow.
    pub fn local_redirect_uri(&self, port: u16) -> String {
        format!("http://127.0.0.1:{}/", port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn flags_with_required(hostname: &str, client_id: &str) -> CliFlags {
        CliFlags {
            hostname: Some(hostname.to_string()),
            client_id: Some(client_id.to_string()),
            ..Default::default()
        }
    }

    #[test]
    #[serial]
    fn test_resolve_with_flags() {
        let config = Config::resolve(flags_with_required("host.example.com", "my-client")).unwrap();
        assert_eq!(config.hostname, "host.example.com");
        assert_eq!(config.client_id, "my-client");
        assert_eq!(config.scopes, vec!["offline_access", "api:read-data"]);
        assert_eq!(config.port, 9876);
        assert!(!config.debug);
        assert!(!config.no_browser);
    }

    #[test]
    #[serial]
    fn test_resolve_missing_hostname() {
        let flags = CliFlags {
            client_id: Some("id".into()),
            ..Default::default()
        };
        std::env::remove_var("FDT_CREDENTIALS__DOMAIN");
        let result = Config::resolve(flags);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_resolve_missing_client_id() {
        let flags = CliFlags {
            hostname: Some("host".into()),
            ..Default::default()
        };
        std::env::remove_var("FDT_CREDENTIALS__OAUTH__CLIENT_ID");
        let result = Config::resolve(flags);
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_resolve_custom_scopes() {
        let mut flags = flags_with_required("host", "id");
        flags.scopes = Some("api:read offline_access".into());
        let config = Config::resolve(flags).unwrap();
        assert_eq!(config.scopes, vec!["api:read", "offline_access"]);
    }

    #[test]
    #[serial]
    fn test_resolve_custom_port() {
        let mut flags = flags_with_required("host", "id");
        flags.port = Some(9999);
        let config = Config::resolve(flags).unwrap();
        assert_eq!(config.port, 9999);
    }

    #[test]
    #[serial]
    fn test_resolve_debug_flag() {
        let mut flags = flags_with_required("host", "id");
        flags.debug = true;
        let config = Config::resolve(flags).unwrap();
        assert!(config.debug);
    }

    #[test]
    #[serial]
    fn test_scopes_str() {
        let config = Config::resolve(flags_with_required("host", "id")).unwrap();
        assert_eq!(config.scopes_str(), "offline_access api:read-data");
    }

    #[test]
    #[serial]
    fn test_authorize_url() {
        let config = Config::resolve(flags_with_required("foundry.example.com", "id")).unwrap();
        assert_eq!(
            config.authorize_url(),
            "https://foundry.example.com/multipass/api/oauth2/authorize"
        );
    }

    #[test]
    #[serial]
    fn test_token_url() {
        let config = Config::resolve(flags_with_required("foundry.example.com", "id")).unwrap();
        assert_eq!(
            config.token_url(),
            "https://foundry.example.com/multipass/api/oauth2/token"
        );
    }

    #[test]
    #[serial]
    fn test_callback_url() {
        let config = Config::resolve(flags_with_required("foundry.example.com", "id")).unwrap();
        assert_eq!(
            config.callback_url(),
            "https://foundry.example.com/multipass/api/oauth2/callback"
        );
    }

    #[test]
    #[serial]
    fn test_local_redirect_uri() {
        let config = Config::resolve(flags_with_required("host", "id")).unwrap();
        assert_eq!(config.local_redirect_uri(9876), "http://127.0.0.1:9876/");
        assert_eq!(config.local_redirect_uri(9000), "http://127.0.0.1:9000/");
    }

    // ---- hostname validation tests ----

    #[test]
    fn test_validate_hostname_valid() {
        assert!(validate_hostname("foundry.example.com").is_ok());
        assert!(validate_hostname("my-host.example.co.uk").is_ok());
        assert!(validate_hostname("localhost").is_ok());
        assert!(validate_hostname("a").is_ok());
        assert!(validate_hostname("host-1.test").is_ok());
    }

    #[test]
    fn test_validate_hostname_rejects_empty() {
        assert!(validate_hostname("").is_err());
    }

    #[test]
    fn test_validate_hostname_rejects_slashes() {
        // URL injection: evil.com/steal?x= would redirect credentials
        assert!(validate_hostname("evil.com/steal?x=").is_err());
        assert!(validate_hostname("host/path").is_err());
    }

    #[test]
    fn test_validate_hostname_rejects_colons() {
        assert!(validate_hostname("host:8080").is_err());
        assert!(validate_hostname("https://host").is_err());
    }

    #[test]
    fn test_validate_hostname_rejects_query_and_fragment() {
        assert!(validate_hostname("host?query=1").is_err());
        assert!(validate_hostname("host#fragment").is_err());
    }

    #[test]
    fn test_validate_hostname_rejects_at_sign() {
        // Userinfo injection: user@evil.com
        assert!(validate_hostname("user@evil.com").is_err());
    }

    #[test]
    fn test_validate_hostname_rejects_spaces_and_special() {
        assert!(validate_hostname("host name").is_err());
        assert!(validate_hostname("host\tname").is_err());
        assert!(validate_hostname("host\nname").is_err());
    }

    #[test]
    fn test_validate_hostname_rejects_leading_trailing_dot_hyphen() {
        assert!(validate_hostname(".example.com").is_err());
        assert!(validate_hostname("example.com.").is_err());
        assert!(validate_hostname("-example.com").is_err());
        assert!(validate_hostname("example.com-").is_err());
    }

    #[test]
    #[serial]
    fn test_resolve_rejects_injected_hostname() {
        let flags = flags_with_required("evil.com/steal?x=", "id");
        let result = Config::resolve(flags);
        assert!(result.is_err());
    }
}
