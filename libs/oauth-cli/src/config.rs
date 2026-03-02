use crate::error::{Error, Result};
use std::path::PathBuf;

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

impl Config {
    /// Build a resolved Config by merging: CLI flags → native env vars → FDT env vars → defaults.
    pub fn resolve(flags: CliFlags) -> Result<Self> {
        let hostname = flags
            .hostname
            .or_else(|| std::env::var("FOUNDRY_HOSTNAME").ok())
            .or_else(|| std::env::var("FDT_CREDENTIALS__DOMAIN").ok())
            .ok_or(Error::MissingConfig(
                "hostname (--hostname, FOUNDRY_HOSTNAME, or FDT_CREDENTIALS__DOMAIN)",
            ))?;

        let client_id = flags
            .client_id
            .or_else(|| std::env::var("FOUNDRY_CLIENT_ID").ok())
            .or_else(|| std::env::var("FDT_CREDENTIALS__OAUTH__CLIENT_ID").ok())
            .ok_or(Error::MissingConfig(
                "client_id (--client-id, FOUNDRY_CLIENT_ID, or FDT_CREDENTIALS__OAUTH__CLIENT_ID)",
            ))?;

        let client_secret = flags
            .client_secret
            .or_else(|| std::env::var("FOUNDRY_CLIENT_SECRET").ok());

        let scopes = flags
            .scopes
            .or_else(|| std::env::var("FOUNDRY_SCOPES").ok())
            .map(|s| s.split_whitespace().map(String::from).collect())
            .unwrap_or_else(|| vec!["offline_access".to_string()]);

        let config_dir = resolve_config_dir();

        let port = flags
            .port
            .or_else(|| {
                std::env::var("FOUNDRY_OAUTH_PORT")
                    .ok()
                    .and_then(|s| s.parse().ok())
            })
            .unwrap_or(9876);

        let debug = flags.debug
            || std::env::var("FOUNDRY_OAUTH_DEBUG")
                .ok()
                .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
                .unwrap_or(false);

        Ok(Config {
            hostname,
            client_id,
            client_secret,
            scopes,
            config_dir,
            port,
            no_browser: flags.no_browser,
            debug,
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
        assert_eq!(config.scopes, vec!["offline_access"]);
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
        // Clear env vars that might interfere
        std::env::remove_var("FOUNDRY_HOSTNAME");
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
        std::env::remove_var("FOUNDRY_CLIENT_ID");
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
        assert_eq!(config.scopes_str(), "offline_access");
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
}
