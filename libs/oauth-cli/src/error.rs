use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    // Configuration errors
    #[error("missing required configuration: {0}")]
    MissingConfig(&'static str),

    // OAuth errors
    #[error("OAuth authorization failed: {0}")]
    OAuthAuthorization(String),

    #[error("token exchange failed (HTTP {status}): {body}")]
    TokenExchange { status: u16, body: String },

    #[error("token refresh failed (HTTP {status}): {body}")]
    TokenRefresh { status: u16, body: String },

    #[error("state mismatch: expected {expected}, got {got}")]
    StateMismatch { expected: String, got: String },

    // Server errors
    #[error("failed to bind callback server on {addr}: {source}")]
    ServerBind {
        addr: String,
        source: std::io::Error,
    },

    #[error("callback server timed out waiting for authorization code")]
    ServerTimeout,

    #[error("callback server error: {0}")]
    ServerCallback(String),

    // Cache / storage errors
    #[error("cache directory error ({path}): {source}")]
    CacheDir {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("cache file I/O error ({path}): {source}")]
    CacheIo {
        path: PathBuf,
        source: std::io::Error,
    },

    #[error("cache file parse error ({path}): {source}")]
    CacheParse {
        path: PathBuf,
        source: serde_json::Error,
    },

    #[error("failed to acquire file lock after {seconds}s")]
    LockTimeout { seconds: u64 },

    #[error("keyring error: {0}")]
    Keyring(String),

    // HTTP / network errors
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    // General I/O
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    // Login required (no cached token, non-interactive context)
    #[error("no cached credentials found — run `foundry-dev-tools-oauth login` first")]
    LoginRequired,
}

pub type Result<T> = std::result::Result<T, Error>;
