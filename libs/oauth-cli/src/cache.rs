use crate::error::{Error, Result};
use crate::log;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const LOCK_TIMEOUT_SECS: u64 = 30;
const KEYRING_SERVICE: &str = "foundry-dev-tools-oauth";

/// On-disk cache file (Linux only): maps hash keys to refresh tokens.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CacheFile {
    pub tokens: HashMap<String, String>,
}

/// Compute the cache key: sha256 of "{hostname}\0{client_id}\0{sorted scopes space-joined}".
pub fn cache_key(hostname: &str, client_id: &str, scopes: &[String]) -> String {
    let mut sorted_scopes = scopes.to_vec();
    sorted_scopes.sort();
    let input = format!("{}\0{}\0{}", hostname, client_id, sorted_scopes.join(" "));
    let hash = Sha256::digest(input.as_bytes());
    hex::encode(hash)
}

/// Hex encoding (no extra dependency — small inline helper).
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes
            .as_ref()
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect()
    }
}

/// Whether to use OS keyring (macOS/Windows) or JSON file (Linux).
fn use_keyring() -> bool {
    cfg!(target_os = "macos") || cfg!(target_os = "windows")
}

// ---------------------------------------------------------------------------
// Keyring backend (macOS / Windows)
// ---------------------------------------------------------------------------

fn keyring_load(key: &str) -> Result<Option<String>> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, key)
        .map_err(|e| Error::Keyring(format!("failed to create keyring entry: {}", e)))?;
    match entry.get_password() {
        Ok(token) => Ok(Some(token)),
        Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(Error::Keyring(format!(
            "failed to read from keyring: {}",
            e
        ))),
    }
}

fn keyring_save(key: &str, refresh_token: &str) -> Result<()> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, key)
        .map_err(|e| Error::Keyring(format!("failed to create keyring entry: {}", e)))?;
    entry
        .set_password(refresh_token)
        .map_err(|e| Error::Keyring(format!("failed to save to keyring: {}", e)))
}

fn keyring_delete(key: &str) -> Result<bool> {
    let entry = keyring::Entry::new(KEYRING_SERVICE, key)
        .map_err(|e| Error::Keyring(format!("failed to create keyring entry: {}", e)))?;
    match entry.delete_credential() {
        Ok(()) => Ok(true),
        Err(keyring::Error::NoEntry) => Ok(false),
        Err(e) => Err(Error::Keyring(format!(
            "failed to delete from keyring: {}",
            e
        ))),
    }
}

// ---------------------------------------------------------------------------
// JSON file backend (Linux)
// ---------------------------------------------------------------------------

/// Ensure the cache directory exists with 0o700 permissions (Unix).
/// Uses DirBuilder::mode() to set permissions at creation time, avoiding a TOCTOU race.
pub fn ensure_config_dir(config_dir: &Path) -> Result<()> {
    if config_dir.exists() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(config_dir)
            .map_err(|e| Error::CacheDir {
                path: config_dir.to_path_buf(),
                source: e,
            })?;
    }

    #[cfg(not(unix))]
    {
        fs::create_dir_all(config_dir).map_err(|e| Error::CacheDir {
            path: config_dir.to_path_buf(),
            source: e,
        })?;
    }

    Ok(())
}

fn cache_file_path(config_dir: &Path) -> PathBuf {
    config_dir.join("oauth-cache.json")
}

fn lock_file_path(config_dir: &Path) -> PathBuf {
    config_dir.join("oauth-cache.lock")
}

/// Read the cache file, returning the parsed structure.
fn read_cache_file(config_dir: &Path) -> Result<CacheFile> {
    let path = cache_file_path(config_dir);
    if !path.exists() {
        return Ok(CacheFile::default());
    }
    let data = fs::read_to_string(&path).map_err(|e| Error::CacheIo {
        path: path.clone(),
        source: e,
    })?;
    serde_json::from_str(&data).map_err(|e| Error::CacheParse { path, source: e })
}

/// Write the cache file atomically with 0o600 permissions (Unix).
/// Uses OpenOptions::mode() to set permissions at creation time, avoiding a TOCTOU race
/// where the file is briefly world-readable between write and chmod.
fn write_cache_file(config_dir: &Path, cache: &CacheFile) -> Result<()> {
    use std::io::Write;

    let path = cache_file_path(config_dir);
    let data = serde_json::to_string_pretty(cache).expect("cache serialization cannot fail");

    let mut opts = fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }

    let mut file = opts.open(&path).map_err(|e| Error::CacheIo {
        path: path.clone(),
        source: e,
    })?;
    file.write_all(data.as_bytes())
        .map_err(|e| Error::CacheIo {
            path: path.clone(),
            source: e,
        })?;

    Ok(())
}

fn file_load(config_dir: &Path, key: &str) -> Result<Option<String>> {
    let cache = read_cache_file(config_dir)?;
    Ok(cache.tokens.get(key).cloned())
}

fn file_save(config_dir: &Path, key: &str, refresh_token: &str) -> Result<()> {
    ensure_config_dir(config_dir)?;
    let mut cache = read_cache_file(config_dir)?;
    cache
        .tokens
        .insert(key.to_string(), refresh_token.to_string());
    write_cache_file(config_dir, &cache)
}

fn file_delete(config_dir: &Path, key: &str) -> Result<bool> {
    let mut cache = read_cache_file(config_dir)?;
    let removed = cache.tokens.remove(key).is_some();
    if removed {
        write_cache_file(config_dir, &cache)?;
    }
    Ok(removed)
}

// ---------------------------------------------------------------------------
// Public API — dispatches to keyring or file backend
// ---------------------------------------------------------------------------

/// Load a cached refresh token for the given parameters.
pub fn load(
    config_dir: &Path,
    hostname: &str,
    client_id: &str,
    scopes: &[String],
    debug: bool,
) -> Result<Option<String>> {
    let key = cache_key(hostname, client_id, scopes);

    let result = if use_keyring() {
        keyring_load(&key)?
    } else {
        file_load(config_dir, &key)?
    };

    match &result {
        Some(_) => {
            let backend = if use_keyring() { "keyring" } else { "file" };
            log::debug_log(
                debug,
                config_dir,
                "CACHE_HIT",
                &format!("refresh token found in {}", backend),
            );
        }
        None => {
            log::debug_log(
                debug,
                config_dir,
                "CACHE_MISS",
                &format!("no refresh token for key {}", &key[..12]),
            );
        }
    }

    Ok(result)
}

/// Save a refresh token to the cache.
pub fn save(
    config_dir: &Path,
    hostname: &str,
    client_id: &str,
    scopes: &[String],
    refresh_token: &str,
    debug: bool,
) -> Result<()> {
    let key = cache_key(hostname, client_id, scopes);

    if use_keyring() {
        keyring_save(&key, refresh_token)?;
    } else {
        file_save(config_dir, &key, refresh_token)?;
    }

    let backend = if use_keyring() { "keyring" } else { "file" };
    log::debug_log(
        debug,
        config_dir,
        "CACHE_SAVE",
        &format!("refresh token saved to {}", backend),
    );
    Ok(())
}

/// Delete the cached credential for the given parameters.
pub fn delete(
    config_dir: &Path,
    hostname: &str,
    client_id: &str,
    scopes: &[String],
) -> Result<bool> {
    let key = cache_key(hostname, client_id, scopes);

    if use_keyring() {
        keyring_delete(&key)
    } else {
        file_delete(config_dir, &key)
    }
}

/// Execute a closure while holding an exclusive file lock.
pub fn with_lock<F, T>(config_dir: &Path, debug: bool, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    ensure_config_dir(config_dir)?;

    let lock_path = lock_file_path(config_dir);
    let mut lock_opts = fs::OpenOptions::new();
    lock_opts.create(true).truncate(false).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        lock_opts.mode(0o600);
    }
    let lock_file = lock_opts.open(&lock_path).map_err(|e| Error::CacheIo {
        path: lock_path.clone(),
        source: e,
    })?;

    // Try to acquire the lock with a timeout
    let start = Instant::now();
    let timeout = Duration::from_secs(LOCK_TIMEOUT_SECS);
    let mut lock = fd_lock::RwLock::new(lock_file);

    loop {
        match lock.try_write() {
            Ok(_guard) => {
                log::debug_log(
                    debug,
                    config_dir,
                    "LOCK_ACQUIRED",
                    "exclusive lock acquired",
                );
                let result = f();
                // _guard drops here, releasing the lock
                return result;
            }
            Err(_) => {
                if start.elapsed() >= timeout {
                    return Err(Error::LockTimeout {
                        seconds: LOCK_TIMEOUT_SECS,
                    });
                }
                log::debug_log(
                    debug,
                    config_dir,
                    "LOCK_WAIT",
                    "waiting to acquire file lock",
                );
                std::thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_key_deterministic() {
        let key1 = cache_key("host.example.com", "client123", &["offline_access".into()]);
        let key2 = cache_key("host.example.com", "client123", &["offline_access".into()]);
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_cache_key_scope_order_independent() {
        let key1 = cache_key(
            "host.example.com",
            "client123",
            &["offline_access".into(), "api:read".into()],
        );
        let key2 = cache_key(
            "host.example.com",
            "client123",
            &["api:read".into(), "offline_access".into()],
        );
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_cache_key_different_hosts() {
        let key1 = cache_key("host1.example.com", "client123", &["offline_access".into()]);
        let key2 = cache_key("host2.example.com", "client123", &["offline_access".into()]);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_cache_key_is_sha256_hex() {
        let key = cache_key("host.example.com", "client123", &["offline_access".into()]);
        assert_eq!(key.len(), 64); // sha256 hex = 64 chars
        assert!(key.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // File-backend tests (always work, regardless of platform)
    #[test]
    fn test_file_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path();

        let key = cache_key("host.example.com", "client123", &["offline_access".into()]);

        // Initially empty
        assert!(file_load(config_dir, &key).unwrap().is_none());

        // Save
        file_save(config_dir, &key, "refresh_tok").unwrap();

        // Load back
        assert_eq!(file_load(config_dir, &key).unwrap().unwrap(), "refresh_tok");

        // Delete
        assert!(file_delete(config_dir, &key).unwrap());

        // Gone
        assert!(file_load(config_dir, &key).unwrap().is_none());
    }

    // Integration test using the public API (dispatches to keyring on macOS/Windows)
    #[test]
    fn test_cache_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path();

        // Initially empty
        let result = load(
            config_dir,
            "host.example.com",
            "client123",
            &["offline_access".into()],
            false,
        )
        .unwrap();
        assert!(result.is_none());

        // Save
        save(
            config_dir,
            "host.example.com",
            "client123",
            &["offline_access".into()],
            "refresh_tok",
            false,
        )
        .unwrap();

        // Load back
        let result = load(
            config_dir,
            "host.example.com",
            "client123",
            &["offline_access".into()],
            false,
        )
        .unwrap();
        assert_eq!(result.unwrap(), "refresh_tok");

        // Delete
        let removed = delete(
            config_dir,
            "host.example.com",
            "client123",
            &["offline_access".into()],
        )
        .unwrap();
        assert!(removed);

        // Gone
        let result = load(
            config_dir,
            "host.example.com",
            "client123",
            &["offline_access".into()],
            false,
        )
        .unwrap();
        assert!(result.is_none());
    }

    #[cfg(unix)]
    #[test]
    fn test_cache_file_permissions_0o600() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path();
        let key = cache_key("host.example.com", "client123", &["offline_access".into()]);

        file_save(config_dir, &key, "secret_token").unwrap();

        let path = cache_file_path(config_dir);
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "cache file should have 0o600 permissions, got {:o}",
            mode
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_config_dir_permissions_0o700() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path().join("new-oauth-dir");

        ensure_config_dir(&config_dir).unwrap();

        let mode = fs::metadata(&config_dir).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o700,
            "config dir should have 0o700 permissions, got {:o}",
            mode
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_lock_file_permissions_0o600() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let config_dir = dir.path();

        // with_lock creates the lock file
        with_lock(config_dir, false, || Ok(())).unwrap();

        let path = lock_file_path(config_dir);
        let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode, 0o600,
            "lock file should have 0o600 permissions, got {:o}",
            mode
        );
    }
}
