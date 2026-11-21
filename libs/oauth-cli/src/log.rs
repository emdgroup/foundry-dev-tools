use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

/// Append a debug log entry to the debug log file if debug mode is enabled.
pub fn debug_log(enabled: bool, config_dir: &Path, event: &str, message: &str) {
    if !enabled {
        return;
    }

    let log_path = config_dir.join("oauth-debug.log");
    let timestamp = chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f");

    let line = format!("[{}] {} — {}\n", timestamp, event, message);

    // Best-effort: silently ignore write failures for debug logging
    let mut opts = OpenOptions::new();
    opts.create(true).append(true);

    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }

    if let Ok(mut file) = opts.open(&log_path) {
        let _ = file.write_all(line.as_bytes());
    }
}
