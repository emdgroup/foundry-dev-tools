use crate::error::{Error, Result};
use std::collections::HashMap;
use std::time::Duration;
use url::Url;

/// Result of the callback server: the authorization code and state parameter.
pub struct CallbackResult {
    pub code: String,
    pub state: String,
}

const SUCCESS_HTML: &str = r#"<!DOCTYPE html>
<html>
<head><title>Authorization Successful</title></head>
<body style="font-family: sans-serif; text-align: center; padding: 50px;">
  <h1>Authorization successful!</h1>
  <p>You can close this browser tab and return to the terminal.</p>
</body>
</html>"#;

const ERROR_HTML: &str = r#"<!DOCTYPE html>
<html>
<head><title>Authorization Failed</title></head>
<body style="font-family: sans-serif; text-align: center; padding: 50px;">
  <h1>Authorization failed</h1>
  <p>Missing authorization code. Please try again.</p>
</body>
</html>"#;

/// A bound callback server ready to accept the OAuth redirect.
pub struct CallbackServer {
    server: tiny_http::Server,
    port: u16,
}

impl CallbackServer {
    /// Bind a local HTTP server on 127.0.0.1:{port}.
    /// Call this before opening the browser so the port is ready to receive the callback.
    pub fn bind(port: u16) -> Result<Self> {
        let addr = format!("127.0.0.1:{}", port);
        let server = tiny_http::Server::http(&addr).map_err(|e| Error::ServerBind {
            addr,
            source: std::io::Error::new(std::io::ErrorKind::AddrInUse, e.to_string()),
        })?;
        Ok(Self { server, port })
    }

    /// The port this server is bound to.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Wait for the OAuth callback, blocking up to `timeout_secs`.
    pub fn wait_for_callback(self, timeout_secs: u64) -> Result<CallbackResult> {
        let timeout = Duration::from_secs(timeout_secs);
        let request = self
            .server
            .recv_timeout(timeout)
            .map_err(|_| Error::ServerTimeout)?
            .ok_or(Error::ServerTimeout)?;

        let url_str = format!("http://127.0.0.1:{}{}", self.port, request.url());
        let parsed = Url::parse(&url_str).map_err(|e| Error::ServerCallback(e.to_string()))?;
        let params: HashMap<String, String> = parsed.query_pairs().into_owned().collect();

        if let (Some(code), Some(state)) = (params.get("code"), params.get("state")) {
            respond_html(request, SUCCESS_HTML);
            Ok(CallbackResult {
                code: code.clone(),
                state: state.clone(),
            })
        } else if let Some(error) = params.get("error") {
            let desc = params.get("error_description").cloned().unwrap_or_default();
            respond_html(request, ERROR_HTML);
            Err(Error::OAuthAuthorization(format!("{}: {}", error, desc)))
        } else {
            respond_html(request, ERROR_HTML);
            Err(Error::ServerCallback(
                "callback missing 'code' and 'state' parameters".into(),
            ))
        }
    }
}

fn respond_html(request: tiny_http::Request, body: &str) {
    let response = tiny_http::Response::from_string(body).with_header(
        tiny_http::Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..])
            .unwrap(),
    );
    let _ = request.respond(response);
}
