use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// Append a debug log entry to the debug log file if debug mode is enabled.
pub fn debug_log(enabled: bool, cache_dir: &Path, event: &str, message: &str) {
    if !enabled {
        return;
    }

    let log_path = cache_dir.join("oauth-debug.log");
    let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");

    let line = format!("[{}] {} — {}\n", timestamp, event, message);

    // Best-effort: silently ignore write failures for debug logging
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&log_path) {
        let _ = file.write_all(line.as_bytes());
    }
}
