use crate::error::{Error, Result};
use std::path::PathBuf;

/// Resolved configuration for the CLI.
#[derive(Debug, Clone)]
pub struct Config {
    pub hostname: String,
    pub client_id: String,
    pub client_secret: Option<String>,
    pub scopes: Vec<String>,
    pub cache_dir: PathBuf,
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
    pub cache_dir: Option<String>,
    pub port: Option<u16>,
    pub no_browser: bool,
    pub debug: bool,
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

        let cache_dir = flags
            .cache_dir
            .or_else(|| std::env::var("FOUNDRY_CACHE_DIR").ok())
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                dirs::home_dir()
                    .unwrap_or_else(|| PathBuf::from("."))
                    .join(".foundry")
                    .join("oauth-cli")
            });

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
            cache_dir,
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

    fn flags_with_required(hostname: &str, client_id: &str) -> CliFlags {
        CliFlags {
            hostname: Some(hostname.to_string()),
            client_id: Some(client_id.to_string()),
            ..Default::default()
        }
    }

    #[test]
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
    fn test_resolve_custom_scopes() {
        let mut flags = flags_with_required("host", "id");
        flags.scopes = Some("api:read offline_access".into());
        let config = Config::resolve(flags).unwrap();
        assert_eq!(config.scopes, vec!["api:read", "offline_access"]);
    }

    #[test]
    fn test_resolve_custom_port() {
        let mut flags = flags_with_required("host", "id");
        flags.port = Some(9999);
        let config = Config::resolve(flags).unwrap();
        assert_eq!(config.port, 9999);
    }

    #[test]
    fn test_resolve_debug_flag() {
        let mut flags = flags_with_required("host", "id");
        flags.debug = true;
        let config = Config::resolve(flags).unwrap();
        assert!(config.debug);
    }

    #[test]
    fn test_scopes_str() {
        let config = Config::resolve(flags_with_required("host", "id")).unwrap();
        assert_eq!(config.scopes_str(), "offline_access");
    }

    #[test]
    fn test_authorize_url() {
        let config = Config::resolve(flags_with_required("foundry.example.com", "id")).unwrap();
        assert_eq!(
            config.authorize_url(),
            "https://foundry.example.com/multipass/api/oauth2/authorize"
        );
    }

    #[test]
    fn test_token_url() {
        let config = Config::resolve(flags_with_required("foundry.example.com", "id")).unwrap();
        assert_eq!(
            config.token_url(),
            "https://foundry.example.com/multipass/api/oauth2/token"
        );
    }

    #[test]
    fn test_callback_url() {
        let config = Config::resolve(flags_with_required("foundry.example.com", "id")).unwrap();
        assert_eq!(
            config.callback_url(),
            "https://foundry.example.com/multipass/api/oauth2/callback"
        );
    }

    #[test]
    fn test_local_redirect_uri() {
        let config = Config::resolve(flags_with_required("host", "id")).unwrap();
        assert_eq!(config.local_redirect_uri(9876), "http://127.0.0.1:9876/");
        assert_eq!(config.local_redirect_uri(9000), "http://127.0.0.1:9000/");
    }
}
