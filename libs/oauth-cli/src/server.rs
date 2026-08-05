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
        // Resolve the actual bound port (important when port 0 is used for OS-assigned ports)
        let actual_port = match server.server_addr() {
            tiny_http::ListenAddr::IP(addr) => addr.port(),
            _ => port,
        };
        Ok(Self {
            server,
            port: actual_port,
        })
    }

    /// The port this server is bound to.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Wait for the OAuth callback, blocking up to `timeout_secs`.
    /// Only accepts GET requests to "/" (the expected redirect path).
    /// Ignores unrelated requests (e.g. from browser extensions, favicon fetches)
    /// and keeps waiting until a valid callback arrives or the timeout expires.
    pub fn wait_for_callback(self, timeout_secs: u64) -> Result<CallbackResult> {
        let deadline = std::time::Instant::now() + Duration::from_secs(timeout_secs);

        loop {
            let remaining = deadline
                .checked_duration_since(std::time::Instant::now())
                .ok_or(Error::ServerTimeout)?;

            let request = self
                .server
                .recv_timeout(remaining)
                .map_err(|_| Error::ServerTimeout)?
                .ok_or(Error::ServerTimeout)?;

            // Only accept GET requests
            if request.method() != &tiny_http::Method::Get {
                respond_html(request, ERROR_HTML);
                continue;
            }

            // Only accept requests to the root path (with query string)
            let request_url = request.url();
            if !request_url.starts_with("/?") && request_url != "/" {
                respond_html(request, ERROR_HTML);
                continue;
            }

            let url_str = format!("http://127.0.0.1:{}{}", self.port, request_url);
            let parsed = Url::parse(&url_str).map_err(|e| Error::ServerCallback(e.to_string()))?;
            let params: HashMap<String, String> = parsed.query_pairs().into_owned().collect();

            if let (Some(code), Some(state)) = (params.get("code"), params.get("state")) {
                respond_html(request, SUCCESS_HTML);
                return Ok(CallbackResult {
                    code: code.clone(),
                    state: state.clone(),
                });
            } else if let Some(error) = params.get("error") {
                let desc = params.get("error_description").cloned().unwrap_or_default();
                respond_html(request, ERROR_HTML);
                return Err(Error::OAuthAuthorization(format!("{}: {}", error, desc)));
            } else {
                // Missing code/state but on the right path — could be a partial redirect.
                // Keep waiting rather than failing, in case the real callback follows.
                respond_html(request, ERROR_HTML);
                continue;
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpStream;

    /// Helper: send a raw HTTP request to the server and return the response.
    fn send_raw_request(port: u16, request: &str) -> String {
        let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        stream.write_all(request.as_bytes()).unwrap();
        stream.flush().unwrap();
        let mut response = String::new();
        // Read with a short timeout so we don't block forever
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let _ = stream.read_to_string(&mut response);
        response
    }

    #[test]
    fn test_callback_accepts_valid_get_with_code_and_state() {
        let server = CallbackServer::bind(0).unwrap();
        let port = server.port();

        let handle = std::thread::spawn(move || server.wait_for_callback(5));

        send_raw_request(
            port,
            "GET /?code=test_code&state=test_state HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        );

        let result = handle.join().unwrap().unwrap();
        assert_eq!(result.code, "test_code");
        assert_eq!(result.state, "test_state");
    }

    #[test]
    fn test_callback_ignores_post_then_accepts_valid_get() {
        let server = CallbackServer::bind(0).unwrap();
        let port = server.port();

        let handle = std::thread::spawn(move || server.wait_for_callback(5));

        // First: POST request — should be ignored
        send_raw_request(
            port,
            "POST /?code=bad&state=bad HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Length: 0\r\n\r\n",
        );

        // Then: valid GET
        send_raw_request(
            port,
            "GET /?code=real_code&state=real_state HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        );

        let result = handle.join().unwrap().unwrap();
        assert_eq!(result.code, "real_code");
        assert_eq!(result.state, "real_state");
    }

    #[test]
    fn test_callback_ignores_favicon_then_accepts_valid_get() {
        let server = CallbackServer::bind(0).unwrap();
        let port = server.port();

        let handle = std::thread::spawn(move || server.wait_for_callback(5));

        // First: favicon request — wrong path, should be ignored
        send_raw_request(port, "GET /favicon.ico HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");

        // Then: valid callback
        send_raw_request(
            port,
            "GET /?code=abc&state=xyz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        );

        let result = handle.join().unwrap().unwrap();
        assert_eq!(result.code, "abc");
        assert_eq!(result.state, "xyz");
    }

    #[test]
    fn test_callback_ignores_root_without_params_then_accepts_valid() {
        let server = CallbackServer::bind(0).unwrap();
        let port = server.port();

        let handle = std::thread::spawn(move || server.wait_for_callback(5));

        // GET / with no query params — should be ignored (missing code/state)
        send_raw_request(port, "GET / HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");

        // Then: valid callback
        send_raw_request(
            port,
            "GET /?code=c&state=s HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        );

        let result = handle.join().unwrap().unwrap();
        assert_eq!(result.code, "c");
        assert_eq!(result.state, "s");
    }

    #[test]
    fn test_callback_returns_error_on_oauth_error_response() {
        let server = CallbackServer::bind(0).unwrap();
        let port = server.port();

        let handle = std::thread::spawn(move || server.wait_for_callback(5));

        send_raw_request(
            port,
            "GET /?error=access_denied&error_description=user+denied HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n",
        );

        let result = handle.join().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_callback_timeout() {
        let server = CallbackServer::bind(0).unwrap();
        // Very short timeout — should expire with no requests
        let result = server.wait_for_callback(1);
        assert!(result.is_err());
    }
}
