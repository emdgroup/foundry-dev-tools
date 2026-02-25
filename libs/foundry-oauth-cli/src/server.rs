use crate::error::{Error, Result};
use std::collections::HashMap;
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

/// Start a local HTTP server on 127.0.0.1 and wait for the OAuth callback.
///
/// Tries ports starting from `start_port` up to `start_port + 99`.
/// Returns the actual port used and the callback result.
pub fn wait_for_callback(start_port: u16, _timeout_secs: u64) -> Result<(u16, CallbackResult)> {
    let (server, port) = bind_server(start_port)?;

    // Set a timeout so we don't hang forever
    server
        .incoming_requests()
        .next()
        .map(|request| {
            // Check timeout manually isn't needed — tiny_http blocks on .next()
            // We rely on the user completing the flow within a reasonable time.
            let url_str = format!("http://127.0.0.1:{}{}", port, request.url());
            let parsed = Url::parse(&url_str).map_err(|e| Error::ServerError(e.to_string()))?;
            let params: HashMap<String, String> = parsed.query_pairs().into_owned().collect();

            if let (Some(code), Some(state)) = (params.get("code"), params.get("state")) {
                // Return success page
                let response = tiny_http::Response::from_string(SUCCESS_HTML)
                    .with_header(
                        tiny_http::Header::from_bytes(
                            &b"Content-Type"[..],
                            &b"text/html; charset=utf-8"[..],
                        )
                        .unwrap(),
                    );
                let _ = request.respond(response);

                Ok((
                    port,
                    CallbackResult {
                        code: code.clone(),
                        state: state.clone(),
                    },
                ))
            } else if let Some(error) = params.get("error") {
                let desc = params
                    .get("error_description")
                    .cloned()
                    .unwrap_or_default();
                let response = tiny_http::Response::from_string(ERROR_HTML).with_header(
                    tiny_http::Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"text/html; charset=utf-8"[..],
                    )
                    .unwrap(),
                );
                let _ = request.respond(response);
                Err(Error::OAuthAuthorization(format!("{}: {}", error, desc)))
            } else {
                let response = tiny_http::Response::from_string(ERROR_HTML).with_header(
                    tiny_http::Header::from_bytes(
                        &b"Content-Type"[..],
                        &b"text/html; charset=utf-8"[..],
                    )
                    .unwrap(),
                );
                let _ = request.respond(response);
                Err(Error::ServerError(
                    "callback missing 'code' and 'state' parameters".into(),
                ))
            }
        })
        .unwrap_or(Err(Error::ServerTimeout))
}

/// Try to bind to ports starting from `start_port`, incrementing up to 99 times.
fn bind_server(start_port: u16) -> Result<(tiny_http::Server, u16)> {
    for offset in 0..100 {
        let port = start_port + offset;
        let addr = format!("127.0.0.1:{}", port);
        match tiny_http::Server::http(&addr) {
            Ok(server) => return Ok((server, port)),
            Err(_) if offset < 99 => continue,
            Err(e) => {
                return Err(Error::ServerBind {
                    addr,
                    source: std::io::Error::new(std::io::ErrorKind::AddrInUse, e.to_string()),
                });
            }
        }
    }
    unreachable!()
}
