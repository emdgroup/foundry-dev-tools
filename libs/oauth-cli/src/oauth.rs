use crate::config::Config;
use crate::error::{Error, Result};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use rand::RngExt;
use serde::Deserialize;
use sha2::{Digest, Sha256};

/// PKCE pair: code_verifier and the derived code_challenge.
#[derive(Debug)]
pub struct Pkce {
    pub code_verifier: String,
    pub code_challenge: String,
}

/// Token response from the Foundry OAuth2 token endpoint.
#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    pub refresh_token: Option<String>,
}

/// Characters allowed in a PKCE code_verifier (RFC 7636 §4.1).
const PKCE_CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";

/// Generate a PKCE code_verifier and code_challenge (S256).
pub fn generate_pkce() -> Pkce {
    let mut rng = rand::rng();
    let verifier: String = (0..128)
        .map(|_| {
            let idx = rng.random_range(0..PKCE_CHARS.len());
            PKCE_CHARS[idx] as char
        })
        .collect();

    let digest = Sha256::digest(verifier.as_bytes());
    let challenge = URL_SAFE_NO_PAD.encode(digest);

    Pkce {
        code_verifier: verifier,
        code_challenge: challenge,
    }
}

/// Generate a random state parameter for CSRF protection.
pub fn generate_state() -> String {
    let mut rng = rand::rng();
    let bytes: Vec<u8> = (0..32).map(|_| rng.random()).collect();
    URL_SAFE_NO_PAD.encode(&bytes)
}

/// Build the full authorization URL for the browser redirect.
pub fn build_authorization_url(
    config: &Config,
    pkce: &Pkce,
    state: &str,
    redirect_uri: &str,
) -> String {
    let scopes = config.scopes_str();
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("response_type", "code")
        .append_pair("client_id", &config.client_id)
        .append_pair("redirect_uri", redirect_uri)
        .append_pair("scope", &scopes)
        .append_pair("state", state)
        .append_pair("code_challenge", &pkce.code_challenge)
        .append_pair("code_challenge_method", "S256")
        .finish();

    format!("{}?{}", config.authorize_url(), query)
}

/// Exchange an authorization code for tokens.
pub fn exchange_code(
    config: &Config,
    code: &str,
    code_verifier: &str,
    redirect_uri: &str,
) -> Result<TokenResponse> {
    let mut params: Vec<(&str, &str)> = vec![
        ("grant_type", "authorization_code"),
        ("code", code),
        ("redirect_uri", redirect_uri),
        ("client_id", &config.client_id),
        ("code_verifier", code_verifier),
    ];
    if let Some(ref secret) = config.client_secret {
        params.push(("client_secret", secret));
    }

    let resp = http_client()
        .post(config.token_url())
        .form(&params)
        .send()?;

    let status = resp.status().as_u16();
    if status != 200 {
        let body = resp.text().unwrap_or_default();
        return Err(Error::TokenExchange { status, body });
    }

    Ok(resp.json()?)
}

/// Refresh an access token using a refresh_token.
pub fn refresh_token(config: &Config, refresh_tok: &str) -> Result<TokenResponse> {
    let mut params: Vec<(&str, &str)> = vec![
        ("grant_type", "refresh_token"),
        ("refresh_token", refresh_tok),
        ("client_id", &config.client_id),
    ];
    if let Some(ref secret) = config.client_secret {
        params.push(("client_secret", secret));
    }

    let resp = http_client()
        .post(config.token_url())
        .form(&params)
        .send()?;

    let status = resp.status().as_u16();
    if status != 200 {
        let body = resp.text().unwrap_or_default();
        return Err(Error::TokenRefresh { status, body });
    }

    Ok(resp.json()?)
}

/// Shared HTTP client. Reuses connection pool and TLS sessions across requests.
/// Redirects are disabled per RFC 6749 §3.2 / OAuth 2.0 Security BCP §4.11:
/// token endpoint requests must not follow redirects, as doing so could leak
/// credentials (auth codes, client secrets, PKCE verifiers) to the redirect target.
fn http_client() -> reqwest::blocking::Client {
    use std::sync::OnceLock;
    static CLIENT: OnceLock<reqwest::blocking::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::blocking::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("failed to build HTTP client")
        })
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_pkce_verifier_length() {
        let pkce = generate_pkce();
        assert_eq!(pkce.code_verifier.len(), 128);
    }

    #[test]
    fn test_generate_pkce_verifier_chars() {
        let pkce = generate_pkce();
        let allowed: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
        for c in pkce.code_verifier.chars() {
            assert!(
                allowed.contains(c),
                "unexpected char '{}' in code_verifier",
                c
            );
        }
    }

    #[test]
    fn test_generate_pkce_challenge_is_base64url() {
        let pkce = generate_pkce();
        // base64url_no_pad of sha256(128 bytes) = 43 chars
        assert_eq!(pkce.code_challenge.len(), 43);
        // Should only contain base64url chars
        for c in pkce.code_challenge.chars() {
            assert!(
                c.is_ascii_alphanumeric() || c == '-' || c == '_',
                "unexpected char '{}' in code_challenge",
                c
            );
        }
    }

    #[test]
    fn test_generate_pkce_challenge_is_sha256_of_verifier() {
        let pkce = generate_pkce();
        let expected_digest = sha2::Sha256::digest(pkce.code_verifier.as_bytes());
        let expected_challenge = URL_SAFE_NO_PAD.encode(expected_digest);
        assert_eq!(pkce.code_challenge, expected_challenge);
    }

    #[test]
    fn test_generate_state_unique() {
        let s1 = generate_state();
        let s2 = generate_state();
        assert_ne!(s1, s2);
    }

    #[test]
    fn test_build_authorization_url() {
        let config = Config {
            hostname: "foundry.example.com".into(),
            client_id: "my-client-id".into(),
            client_secret: None,
            scopes: vec!["offline_access".into()],
            config_dir: "/tmp/test".into(),
            port: 8888,
            no_browser: false,
            debug: false,
            explicit_cli_args: String::new(),
        };
        let pkce = Pkce {
            code_verifier: "test-verifier".into(),
            code_challenge: "test-challenge".into(),
        };
        let url = build_authorization_url(&config, &pkce, "test-state", "http://127.0.0.1:8888/");

        assert!(url.starts_with("https://foundry.example.com/multipass/api/oauth2/authorize?"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=my-client-id"));
        assert!(url.contains("redirect_uri=http"));
        assert!(url.contains("scope=offline_access"));
        assert!(url.contains("state=test-state"));
        assert!(url.contains("code_challenge=test-challenge"));
        assert!(url.contains("code_challenge_method=S256"));
    }
}
