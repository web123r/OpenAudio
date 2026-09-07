use crate::discovery::{send_subscribe_request, DiscoveredNode};
use crate::ensure_realtime_audio_thread;
use crate::protocol::parse_packet;

use argon2::password_hash::{
    rand_core::OsRng, PasswordHash, PasswordHasher, SaltString,
};
use argon2::{Argon2, PasswordVerifier};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{
    IpAddr, SocketAddr, TcpListener, TcpStream, UdpSocket,
};
use std::sync::atomic::{
    AtomicBool, AtomicUsize, Ordering,
};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tungstenite::handshake::server::{
    ErrorResponse,
    Request as WebSocketRequest,
    Response as WebSocketResponse,
};
use tungstenite::http::StatusCode;
use tungstenite::{accept_hdr, Message};

const PLAYER_HTML: &str =
    include_str!("../assets/web-player.html");

const MAX_HTTP_REQUEST_BYTES: usize = 16 * 1024;
const MAX_HTTP_HEADERS: usize = 64;
const MAX_HTTP_PATH_BYTES: usize = 2048;
const MAX_UDP_PACKET_BYTES: usize = 65_536;
const MAX_CHANNELS: usize = 255;
const WEB_AUDIO_BATCH_MS: u32 = 20;
const MAX_LOGIN_FAILURES: u32 = 5;
const LOGIN_LOCKOUT_DURATION: Duration =
    Duration::from_secs(30);
const HTTP_READ_TIMEOUT: Duration =
    Duration::from_secs(10);
const HTTP_WRITE_TIMEOUT: Duration =
    Duration::from_secs(10);
const COOKIE_NAME: &str = "openaudio_session";

pub type DiscoveryDirectory =
    Arc<Mutex<HashMap<String, DiscoveredNode>>>;

#[derive(Clone, Debug)]
pub enum GatewayAccess {
    Open,
    PasswordProtected {
        password: String,
    },
}

#[derive(Clone, Debug)]
pub struct WebGatewayConfig {
    pub http_port: u16,
    pub access: GatewayAccess,
    pub max_clients: usize,
    pub session_duration: Duration,
}

impl Default for WebGatewayConfig {
    fn default() -> Self {
        Self {
            http_port: 7100,
            access: GatewayAccess::Open,
            max_clients: 8,
            session_duration: Duration::from_secs(
                8 * 60 * 60,
            ),
        }
    }
}

#[derive(Serialize)]
struct StreamInfo {
    #[serde(rename = "nodeId")]
    node_id: String,

    #[serde(rename = "nodeName")]
    node_name: String,

    #[serde(rename = "streamId")]
    stream_id: u32,

    #[serde(rename = "streamName")]
    stream_name: String,

    ip: String,

    #[serde(rename = "channelCount")]
    channel_count: u8,
}

#[derive(Deserialize)]
struct ClientSelectMessage {
    #[serde(rename = "type")]
    msg_type: String,

    #[serde(rename = "nodeId")]
    node_id: String,
}

#[derive(Deserialize)]
struct LoginRequest {
    password: String,
}

#[derive(Serialize)]
struct AuthenticationStatus {
    authenticated: bool,
    protected: bool,
}

#[derive(Serialize)]
struct ApiMessage<'a> {
    message: &'a str,
}

#[derive(Clone)]
enum RuntimeAccess {
    Open,
    PasswordProtected {
        password_hash: String,
    },
}

struct Session {
    expires_at: Instant,
}

#[derive(Default)]
struct LoginAttempt {
    failures: u32,
    blocked_until: Option<Instant>,
}

struct GatewayState {
    access: RuntimeAccess,
    sessions: Mutex<HashMap<String, Session>>,
    login_attempts:
        Mutex<HashMap<IpAddr, LoginAttempt>>,
    active_clients: AtomicUsize,
    max_clients: usize,
    session_duration: Duration,
}

impl GatewayState {
    fn is_protected(&self) -> bool {
        matches!(
            self.access,
            RuntimeAccess::PasswordProtected { .. }
        )
    }

    fn is_session_valid(
        &self,
        token: Option<&str>,
    ) -> bool {
        if matches!(self.access, RuntimeAccess::Open) {
            return true;
        }

        let Some(token) = token else {
            return false;
        };

        let now = Instant::now();

        let Ok(mut sessions) = self.sessions.lock() else {
            return false;
        };

        sessions.retain(|_, session| {
            session.expires_at > now
        });

        sessions
            .get(token)
            .map(|session| session.expires_at > now)
            .unwrap_or(false)
    }

    fn create_session(&self) -> Result<String, String> {
        let mut random = [0_u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut random);

        let mut token =
            String::with_capacity(random.len() * 2);

        for byte in random {
            use std::fmt::Write as _;
            let _ = write!(&mut token, "{byte:02x}");
        }

        let session = Session {
            expires_at: Instant::now()
                + self.session_duration,
        };

        self.sessions
            .lock()
            .map_err(|_| {
                "session store unavailable".to_string()
            })?
            .insert(token.clone(), session);

        Ok(token)
    }

    fn remove_session(&self, token: Option<&str>) {
        let Some(token) = token else {
            return;
        };

        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.remove(token);
        }
    }

    fn clear_sessions(&self) {
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.clear();
        }
    }

    fn login_is_blocked(&self, ip: IpAddr) -> bool {
        let now = Instant::now();

        let Ok(mut attempts) =
            self.login_attempts.lock()
        else {
            return true;
        };

        let Some(attempt) = attempts.get_mut(&ip) else {
            return false;
        };

        match attempt.blocked_until {
            Some(until) if until > now => true,
            Some(_) => {
                attempt.failures = 0;
                attempt.blocked_until = None;
                false
            }
            None => false,
        }
    }

    fn record_login_failure(&self, ip: IpAddr) {
        let Ok(mut attempts) =
            self.login_attempts.lock()
        else {
            return;
        };

        let attempt = attempts.entry(ip).or_default();

        attempt.failures =
            attempt.failures.saturating_add(1);

        if attempt.failures >= MAX_LOGIN_FAILURES {
            attempt.blocked_until = Some(
                Instant::now()
                    + LOGIN_LOCKOUT_DURATION,
            );
        }
    }

    fn clear_login_failures(&self, ip: IpAddr) {
        if let Ok(mut attempts) =
            self.login_attempts.lock()
        {
            attempts.remove(&ip);
        }
    }
}

struct ClientSlot {
    state: Arc<GatewayState>,
}

impl Drop for ClientSlot {
    fn drop(&mut self) {
        self.state
            .active_clients
            .fetch_sub(1, Ordering::AcqRel);
    }
}

struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

pub fn run_web_gateway(
    config: WebGatewayConfig,
    directory: DiscoveryDirectory,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    validate_config(&config)?;

    let runtime_access =
        create_runtime_access(config.access)?;

    let state = Arc::new(GatewayState {
        access: runtime_access,
        sessions: Mutex::new(HashMap::new()),
        login_attempts: Mutex::new(HashMap::new()),
        active_clients: AtomicUsize::new(0),
        max_clients: config.max_clients,
        session_duration: config.session_duration,
    });

    let http_port = config.http_port;
    let ws_port = http_port + 1;

    let http_listener =
        TcpListener::bind(("0.0.0.0", http_port))
            .map_err(|error| {
                format!(
                    "failed to bind HTTP port \
                     {http_port}: {error}"
                )
            })?;

    let ws_listener =
        TcpListener::bind(("0.0.0.0", ws_port))
            .map_err(|error| {
                format!(
                    "failed to bind WebSocket port \
                     {ws_port}: {error}"
                )
            })?;

    http_listener
        .set_nonblocking(true)
        .map_err(|error| {
            format!(
                "failed to configure HTTP listener: \
                 {error}"
            )
        })?;

    ws_listener
        .set_nonblocking(true)
        .map_err(|error| {
            format!(
                "failed to configure WebSocket listener: \
                 {error}"
            )
        })?;

    let mode = if state.is_protected() {
        "password protected"
    } else {
        "open LAN"
    };

    println!(
        "Web gateway ready ({mode}): HTTP on \
         :{http_port}, WebSocket on :{ws_port}"
    );

    let http_directory = directory.clone();
    let http_state = state.clone();
    let http_keep_running = keep_running.clone();

    let http_thread = thread::spawn(move || {
        run_http_server(
            http_listener,
            http_directory,
            http_state,
            http_keep_running,
        );
    });

    while keep_running.load(Ordering::Acquire) {
        match ws_listener.accept() {
            Ok((stream, address)) => {
                if !reserve_client_slot(&state) {
                    reject_tcp_connection(stream);
                    continue;
                }

                let client_slot = ClientSlot {
                    state: state.clone(),
                };

                let client_directory =
                    directory.clone();

                let client_state = state.clone();

                let client_keep_running =
                    keep_running.clone();

                thread::spawn(move || {
                    let _client_slot = client_slot;

                    if let Err(error) = handle_ws_client(
                        stream,
                        client_directory,
                        client_state,
                        client_keep_running,
                    ) {
                        eprintln!(
                            "audio-core: WebSocket client \
                             {address} ended: {error}"
                        );
                    }
                });
            }

            Err(ref error)
                if error.kind()
                    == std::io::ErrorKind::WouldBlock =>
            {
                thread::sleep(
                    Duration::from_millis(100),
                );
            }

            Err(error) => {
                eprintln!(
                    "audio-core: WebSocket accept error: \
                     {error}"
                );

                thread::sleep(
                    Duration::from_millis(100),
                );
            }
        }
    }

    state.clear_sessions();

    if http_thread.join().is_err() {
        eprintln!(
            "audio-core: HTTP gateway thread panicked"
        );
    }

    println!("Web gateway stopped.");

    Ok(())
}

fn validate_config(
    config: &WebGatewayConfig,
) -> Result<(), String> {
    if config.http_port == u16::MAX {
        return Err(
            "HTTP port cannot be 65535 because \
             WebSocket uses HTTP port + 1"
                .to_string(),
        );
    }

    if config.max_clients == 0 {
        return Err(
            "maximum browser clients must be at least 1"
                .to_string(),
        );
    }

    if config.session_duration.is_zero() {
        return Err(
            "session duration must be greater than zero"
                .to_string(),
        );
    }

    Ok(())
}

fn create_runtime_access(
    access: GatewayAccess,
) -> Result<RuntimeAccess, String> {
    match access {
        GatewayAccess::Open => Ok(RuntimeAccess::Open),

        GatewayAccess::PasswordProtected { password } => {
            if password.chars().count() < 8 {
                return Err(
                    "browser password must contain at \
                     least 8 characters"
                        .to_string(),
                );
            }

            let salt =
                SaltString::generate(&mut OsRng);

            let password_hash = Argon2::default()
                .hash_password(
                    password.as_bytes(),
                    &salt,
                )
                .map_err(|error| {
                    format!(
                        "failed to hash browser password: \
                         {error}"
                    )
                })?
                .to_string();

            Ok(RuntimeAccess::PasswordProtected {
                password_hash,
            })
        }
    }
}

fn run_http_server(
    listener: TcpListener,
    directory: DiscoveryDirectory,
    state: Arc<GatewayState>,
    keep_running: Arc<AtomicBool>,
) {
    while keep_running.load(Ordering::Acquire) {
        match listener.accept() {
            Ok((stream, address)) => {
                let request_directory =
                    directory.clone();

                let request_state = state.clone();

                thread::spawn(move || {
                    if let Err(error) =
                        handle_http_request(
                            stream,
                            address,
                            &request_directory,
                            &request_state,
                        )
                    {
                        eprintln!(
                            "audio-core: HTTP client \
                             {address} ended: {error}"
                        );
                    }
                });
            }

            Err(ref error)
                if error.kind()
                    == std::io::ErrorKind::WouldBlock =>
            {
                thread::sleep(
                    Duration::from_millis(100),
                );
            }

            Err(error) => {
                eprintln!(
                    "audio-core: HTTP accept error: \
                     {error}"
                );

                thread::sleep(
                    Duration::from_millis(100),
                );
            }
        }
    }
}

fn reserve_client_slot(
    state: &Arc<GatewayState>,
) -> bool {
    state
        .active_clients
        .fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| {
                (current < state.max_clients)
                    .then_some(current + 1)
            },
        )
        .is_ok()
}

fn reject_tcp_connection(mut stream: TcpStream) {
    let body =
        br#"{"message":"Too many browser clients"}"#;

    let _ = write_http_response(
        &mut stream,
        "503 Service Unavailable",
        "application/json; charset=utf-8",
        body,
        &[],
    );
}


fn handle_http_request(
    mut stream: TcpStream,
    address: SocketAddr,
    directory: &DiscoveryDirectory,
    state: &Arc<GatewayState>,
) -> std::io::Result<()> {
    // Accepted sockets can inherit nonblocking behavior on Windows.
    // Request parsing requires a blocking stream with a bounded timeout.
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(10)))?;

    let request = match read_http_request(&mut stream) {
        Ok(request) => request,
        Err(error) => {
            eprintln!(
                "audio-core: rejected HTTP request from {address}: {error}"
            );

            let message = match error.kind() {
                std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::WouldBlock => {
                    "HTTP request timed out before it was complete."
                }
                std::io::ErrorKind::InvalidData => {
                    "The HTTP request format was invalid."
                }
                std::io::ErrorKind::UnexpectedEof => {
                    "The HTTP request ended before it was complete."
                }
                _ => "The HTTP request could not be read.",
            };

            let body = serde_json::to_vec(&ApiMessage { message })
                .unwrap_or_else(|_| {
                    br#"{"message":"Invalid request"}"#.to_vec()
                });

            let _ = write_http_response(
                &mut stream,
                "400 Bad Request",
                "application/json; charset=utf-8",
                &body,
                &[],
            );

            return Err(error);
        }
    };

    let cookie_token = request
        .headers
        .get("cookie")
        .and_then(|header| cookie_value(header, COOKIE_NAME));

    // Keep the existing match statement and routes below this point.


    match (
        request.method.as_str(),
        request.path.as_str(),
    ) {
        ("GET", "/")
        | ("GET", "/index.html") => {
            write_http_response(
                &mut stream,
                "200 OK",
                "text/html; charset=utf-8",
                PLAYER_HTML.as_bytes(),
                &[],
            )
        }

        ("GET", "/favicon.ico") => {
            write_http_response(
                &mut stream,
                "204 No Content",
                "image/x-icon",
                &[],
                &[],
            )
        }

        ("GET", "/api/auth/status") => {
            handle_authentication_status(
                &mut stream,
                state,
                cookie_token.as_deref(),
            )
        }

        ("POST", "/api/login") => {
            handle_login(
                &mut stream,
                address.ip(),
                state,
                &request.body,
            )
        }

        ("POST", "/api/logout") => {
            handle_logout(
                &mut stream,
                state,
                cookie_token.as_deref(),
            )
        }

        ("GET", "/api/streams") => {
            handle_stream_list(
                &mut stream,
                directory,
                state,
                cookie_token.as_deref(),
            )
        }

        _ => {
            let body =
                br#"{"message":"Not found"}"#;

            write_http_response(
                &mut stream,
                "404 Not Found",
                "application/json; charset=utf-8",
                body,
                &[],
            )
        }
    }
}

fn handle_authentication_status(
    stream: &mut TcpStream,
    state: &GatewayState,
    token: Option<&str>,
) -> std::io::Result<()> {
    let status = AuthenticationStatus {
        authenticated: state.is_session_valid(token),
        protected: state.is_protected(),
    };

    let body = serde_json::to_vec(&status)
        .unwrap_or_else(|_| b"{}".to_vec());

    write_http_response(
        stream,
        "200 OK",
        "application/json; charset=utf-8",
        &body,
        &[],
    )
}

fn handle_login(
    stream: &mut TcpStream,
    client_ip: IpAddr,
    state: &GatewayState,
    request_body: &[u8],
) -> std::io::Result<()> {
    if !state.is_protected() {
        let status = AuthenticationStatus {
            authenticated: true,
            protected: false,
        };

        let body = serde_json::to_vec(&status)
            .unwrap_or_else(|_| b"{}".to_vec());

        return write_http_response(
            stream,
            "200 OK",
            "application/json; charset=utf-8",
            &body,
            &[],
        );
    }

    if state.login_is_blocked(client_ip) {
        let body = serde_json::to_vec(&ApiMessage {
            message: "Too many failed attempts. Try \
                      again in 30 seconds.",
        })
        .unwrap_or_else(|_| b"{}".to_vec());

        return write_http_response(
            stream,
            "429 Too Many Requests",
            "application/json; charset=utf-8",
            &body,
            &[("Retry-After", "30".to_string())],
        );
    }

    let login: LoginRequest =
        match serde_json::from_slice(request_body) {
            Ok(login) => login,

            Err(_) => {
                let body = serde_json::to_vec(
                    &ApiMessage {
                        message:
                            "Invalid login request.",
                    },
                )
                .unwrap_or_else(|_| b"{}".to_vec());

                return write_http_response(
                    stream,
                    "400 Bad Request",
                    "application/json; charset=utf-8",
                    &body,
                    &[],
                );
            }
        };

    let verified = verify_password(
        &state.access,
        &login.password,
    );

    if !verified {
        state.record_login_failure(client_ip);

        let body =
            serde_json::to_vec(&ApiMessage {
                message: "Incorrect password.",
            })
            .unwrap_or_else(|_| b"{}".to_vec());

        return write_http_response(
            stream,
            "401 Unauthorized",
            "application/json; charset=utf-8",
            &body,
            &[],
        );
    }

    state.clear_login_failures(client_ip);

    let token = match state.create_session() {
        Ok(token) => token,

        Err(_) => {
            let body =
                serde_json::to_vec(&ApiMessage {
                    message:
                        "Could not create a session.",
                })
                .unwrap_or_else(|_| b"{}".to_vec());

            return write_http_response(
                stream,
                "500 Internal Server Error",
                "application/json; charset=utf-8",
                &body,
                &[],
            );
        }
    };

    let max_age = state.session_duration.as_secs();

    let cookie = format!(
        "{COOKIE_NAME}={token}; Path=/; HttpOnly; \
         SameSite=Strict; Max-Age={max_age}"
    );

    let body =
        serde_json::to_vec(&AuthenticationStatus {
            authenticated: true,
            protected: true,
        })
        .unwrap_or_else(|_| b"{}".to_vec());

    write_http_response(
        stream,
        "200 OK",
        "application/json; charset=utf-8",
        &body,
        &[("Set-Cookie", cookie)],
    )
}

fn verify_password(
    access: &RuntimeAccess,
    password: &str,
) -> bool {
    match access {
        RuntimeAccess::Open => true,

        RuntimeAccess::PasswordProtected {
            password_hash,
        } => PasswordHash::new(password_hash)
            .ok()
            .map(|parsed_hash| {
                Argon2::default()
                    .verify_password(
                        password.as_bytes(),
                        &parsed_hash,
                    )
                    .is_ok()
            })
            .unwrap_or(false),
    }
}

fn handle_logout(
    stream: &mut TcpStream,
    state: &GatewayState,
    token: Option<&str>,
) -> std::io::Result<()> {
    state.remove_session(token);

    let expired_cookie = format!(
        "{COOKIE_NAME}=deleted; Path=/; HttpOnly; \
         SameSite=Strict; Max-Age=0"
    );

    let body =
        serde_json::to_vec(&ApiMessage {
            message: "Logged out.",
        })
        .unwrap_or_else(|_| b"{}".to_vec());

    write_http_response(
        stream,
        "200 OK",
        "application/json; charset=utf-8",
        &body,
        &[("Set-Cookie", expired_cookie)],
    )
}

fn handle_stream_list(
    stream: &mut TcpStream,
    directory: &DiscoveryDirectory,
    state: &GatewayState,
    token: Option<&str>,
) -> std::io::Result<()> {
    if !state.is_session_valid(token) {
        let body =
            serde_json::to_vec(&ApiMessage {
                message: "Authentication required.",
            })
            .unwrap_or_else(|_| b"{}".to_vec());

        return write_http_response(
            stream,
            "401 Unauthorized",
            "application/json; charset=utf-8",
            &body,
            &[],
        );
    }

    let streams = match directory.lock() {
        Ok(directory) => directory
            .values()
            .map(|node| StreamInfo {
                node_id: node.node_id.clone(),
                node_name: node.node_name.clone(),
                stream_id: node.stream_id,
                stream_name: node.stream_name.clone(),
                ip: node.ip.clone(),
                channel_count: node.channel_count,
            })
            .collect::<Vec<_>>(),

        Err(_) => Vec::new(),
    };

    let body = serde_json::to_vec(&streams)
        .unwrap_or_else(|_| b"[]".to_vec());

    write_http_response(
        stream,
        "200 OK",
        "application/json; charset=utf-8",
        &body,
        &[],
    )
}

fn read_http_request(stream: &mut TcpStream) -> std::io::Result<HttpRequest> {
    use std::io::{Error, ErrorKind};

    let mut bytes = Vec::with_capacity(4096);
    let mut chunk = [0_u8; 4096];

    let (method, path, headers, header_length, content_length) = loop {
        if bytes.len() >= MAX_HTTP_REQUEST_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "HTTP request headers exceeded the size limit",
            ));
        }

        let remaining_capacity = MAX_HTTP_REQUEST_BYTES - bytes.len();
        let read_size = remaining_capacity.min(chunk.len());

        let read = match stream.read(&mut chunk[..read_size]) {
            Ok(0) if bytes.is_empty() => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "connection closed without sending a request",
                ));
            }
            Ok(0) => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "connection closed before HTTP headers were complete",
                ));
            }
            Ok(read) => read,
            Err(ref error)
    if matches!(
        error.kind(),
        ErrorKind::Interrupted | ErrorKind::WouldBlock
    ) =>
{
    thread::sleep(Duration::from_millis(1));
    continue;
}

            Err(error) => return Err(error),
        };

        bytes.extend_from_slice(&chunk[..read]);

        let mut parsed_headers = [httparse::EMPTY_HEADER; MAX_HTTP_HEADERS];
        let mut parsed_request = httparse::Request::new(&mut parsed_headers);

        match parsed_request.parse(&bytes) {
            Ok(httparse::Status::Partial) => {
                continue;
            }

            Ok(httparse::Status::Complete(header_length)) => {
                let method = parsed_request
                    .method
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::InvalidData,
                            "missing HTTP method",
                        )
                    })?
                    .to_ascii_uppercase();

                let raw_path = parsed_request.path.ok_or_else(|| {
                    Error::new(
                        ErrorKind::InvalidData,
                        "missing HTTP path",
                    )
                })?;

                let path = raw_path
                    .split('?')
                    .next()
                    .unwrap_or("/")
                    .to_string();

                if path.len() > MAX_HTTP_PATH_BYTES {
                    return Err(Error::new(
                        ErrorKind::InvalidData,
                        "HTTP request path was too long",
                    ));
                }

                let mut headers = HashMap::new();
                let mut content_length = 0_usize;

                for header in parsed_request.headers.iter() {
                    let name = header.name.trim().to_ascii_lowercase();

                    let value = std::str::from_utf8(header.value)
                        .map_err(|_| {
                            Error::new(
                                ErrorKind::InvalidData,
                                "HTTP header value was not valid UTF-8",
                            )
                        })?
                        .trim()
                        .to_string();

                    if name == "content-length" {
                        content_length = value.parse::<usize>().map_err(|_| {
                            Error::new(
                                ErrorKind::InvalidData,
                                "invalid Content-Length header",
                            )
                        })?;
                    }

                    headers
                        .entry(name)
                        .and_modify(|existing: &mut String| {
                            existing.push_str(", ");
                            existing.push_str(&value);
                        })
                        .or_insert(value);
                }

                break (
                    method,
                    path,
                    headers,
                    header_length,
                    content_length,
                );
            }

            Err(error) => {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!("invalid HTTP request: {error}"),
                ));
            }
        }
    };

    let total_length = header_length
        .checked_add(content_length)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidData,
                "HTTP request length overflow",
            )
        })?;

    if total_length > MAX_HTTP_REQUEST_BYTES {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "HTTP request exceeded the size limit",
        ));
    }

    while bytes.len() < total_length {
        let remaining = total_length - bytes.len();
        let read_size = remaining.min(chunk.len());

        let read = match stream.read(&mut chunk[..read_size]) {
            Ok(0) => {
                return Err(Error::new(
                    ErrorKind::UnexpectedEof,
                    "connection closed before HTTP body was complete",
                ));
            }
            Ok(read) => read,
            Err(ref error) if error.kind() == ErrorKind::Interrupted => {
                continue;
            }
            Err(error) => return Err(error),
        };

        bytes.extend_from_slice(&chunk[..read]);
    }

    eprintln!(
        "audio-core: HTTP {} {} ({} body bytes)",
        method, path, content_length
    );

    Ok(HttpRequest {
        method,
        path,
        headers,
        body: bytes[header_length..total_length].to_vec(),
    })
}



fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
    extra_headers: &[(&str, String)],
) -> std::io::Result<()> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         Cache-Control: no-store\r\n\
         X-Content-Type-Options: nosniff\r\n\
         X-Frame-Options: DENY\r\n\
         Referrer-Policy: no-referrer\r\n\
         Content-Security-Policy: default-src 'self'; \
         style-src 'self' 'unsafe-inline'; \
         script-src 'self' 'unsafe-inline'; \
         connect-src 'self' ws: wss:\r\n",
        body.len(),
    );

    for (name, value) in extra_headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }

    response.push_str("\r\n");

    stream.write_all(response.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn cookie_value(
    cookie_header: &str,
    name: &str,
) -> Option<String> {
    cookie_header.split(';').find_map(|part| {
        let (cookie_name, value) =
            part.trim().split_once('=')?;

        if cookie_name == name && !value.is_empty() {
            Some(value.to_string())
        } else {
            None
        }
    })
}

fn websocket_cookie(
    request: &WebSocketRequest,
) -> Option<String> {
    let cookie_header = request
        .headers()
        .get("cookie")?
        .to_str()
        .ok()?;

    cookie_value(cookie_header, COOKIE_NAME)
}

fn unauthorized_websocket_response()
-> ErrorResponse {
    tungstenite::http::Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header(
            "Content-Type",
            "text/plain; charset=utf-8",
        )
        .body(Some(
            "Authentication required".to_string(),
        ))
        .unwrap_or_else(|_| {
            ErrorResponse::new(Some(
                "Authentication required".to_string(),
            ))
        })
}

fn handle_ws_client(
    stream: TcpStream,
    directory: DiscoveryDirectory,
    state: Arc<GatewayState>,
    global_keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    stream
        .set_nonblocking(false)
        .map_err(|error| error.to_string())?;

    stream
        .set_read_timeout(Some(Duration::from_secs(
            10,
        )))
        .map_err(|error| error.to_string())?;

    let auth_state = state.clone();

    let mut ws = accept_hdr(
        stream,
        move |
            request: &WebSocketRequest,
            response: WebSocketResponse,
        | {
            let token = websocket_cookie(request);

            if auth_state
                .is_session_valid(token.as_deref())
            {
                Ok(response)
            } else {
                Err(
                    unauthorized_websocket_response(),
                )
            }
        },
    )
    .map_err(|error| {
        format!(
            "WebSocket handshake failed: {error}"
        )
    })?;

    let node = wait_for_stream_selection(
        &mut ws,
        &directory,
        &global_keep_running,
    )?;

    let socket = UdpSocket::bind("0.0.0.0:0")
        .map_err(|error| {
            format!(
                "failed to create browser UDP \
                 receiver: {error}"
            )
        })?;

    socket
        .set_read_timeout(Some(
            Duration::from_millis(200),
        ))
        .map_err(|error| error.to_string())?;

    let local_port = socket
        .local_addr()
        .map_err(|error| error.to_string())?
        .port();

    let session_active =
        Arc::new(AtomicBool::new(true));

    start_resubscribe_thread(
        node.clone(),
        local_port,
        session_active.clone(),
        global_keep_running.clone(),
    );

    let result = relay_audio_to_websocket(
        &mut ws,
        &socket,
        &node,
        &global_keep_running,
    );

    session_active.store(false, Ordering::Release);

    result
}

fn wait_for_stream_selection(
    ws: &mut tungstenite::WebSocket<TcpStream>,
    directory: &DiscoveryDirectory,
    global_keep_running: &AtomicBool,
) -> Result<DiscoveredNode, String> {
    loop {
        if !global_keep_running.load(Ordering::Acquire)
        {
            return Err(
                "gateway stopped".to_string(),
            );
        }

        let message = ws.read().map_err(|error| {
            format!(
                "WebSocket read failed: {error}"
            )
        })?;

        match message {
            Message::Text(text) => {
                let selection =
                    match serde_json::from_str::<
                        ClientSelectMessage,
                    >(text.as_ref())
                    {
                        Ok(selection) => selection,

                        Err(_) => {
                            send_ws_error(
                                ws,
                                "Invalid selection \
                                 message",
                            );
                            continue;
                        }
                    };

                if selection.msg_type != "select" {
                    send_ws_error(
                        ws,
                        "Unsupported message type",
                    );
                    continue;
                }

                let found = directory
                    .lock()
                    .ok()
                    .and_then(|directory| {
                        directory
                            .get(&selection.node_id)
                            .cloned()
                    });

                match found {
                    Some(node) => return Ok(node),

                    None => {
                        send_ws_error(
                            ws,
                            "Stream no longer \
                             available",
                        );
                    }
                }
            }

            Message::Close(_) => {
                return Err(
                    "WebSocket client closed"
                        .to_string(),
                );
            }

            Message::Ping(payload) => {
                let _ =
                    ws.send(Message::Pong(payload));
            }

            _ => {}
        }
    }
}

fn start_resubscribe_thread(
    node: DiscoveredNode,
    local_port: u16,
    session_active: Arc<AtomicBool>,
    global_keep_running: Arc<AtomicBool>,
) {
    thread::spawn(move || {
        while session_active.load(Ordering::Acquire)
            && global_keep_running
                .load(Ordering::Acquire)
        {
            let _ = send_subscribe_request(
                &node.ip,
                node.control_port,
                node.stream_id,
                local_port,
            );

            for _ in 0..50 {
                if !session_active
                    .load(Ordering::Acquire)
                    || !global_keep_running
                        .load(Ordering::Acquire)
                {
                    return;
                }

                thread::sleep(
                    Duration::from_millis(100),
                );
            }
        }
    });
}

fn relay_audio_to_websocket(
    ws: &mut tungstenite::WebSocket<TcpStream>,
    socket: &UdpSocket,
    node: &DiscoveredNode,
    global_keep_running: &AtomicBool,
) -> Result<(), String> {
    let mut buffer = [0_u8; MAX_UDP_PACKET_BYTES];
    let mut batched_payload = Vec::new();
    let mut batched_frames = 0usize;
    let mut batch_started = Instant::now();

    let mut expected_format:
        Option<(u16, u32)> = None;

    loop {
        if !global_keep_running.load(Ordering::Acquire)
        {
            return Ok(());
        }

        match socket.recv_from(&mut buffer) {
            Ok((length, _source)) => {
                let packet = &buffer[..length];

                let Some(parsed) =
                    parse_packet(packet)
                else {
                    continue;
                };

                let channels =
                    parsed.channel_count as usize;

                if channels == 0
                    || channels > MAX_CHANNELS
                {
                    continue;
                }

                if parsed.sample_rate == 0 {
                    continue;
                }

                let sample_count =
                    match (parsed.samples_per_channel
                        as usize)
                        .checked_mul(channels)
                    {
                        Some(count) => count,
                        None => continue,
                    };

                let payload_bytes =
                    match sample_count.checked_mul(
                        std::mem::size_of::<f32>(),
                    ) {
                        Some(bytes) => bytes,
                        None => continue,
                    };

                let payload_end =
                    match parsed
                        .payload_offset
                        .checked_add(payload_bytes)
                    {
                        Some(end) => end,
                        None => continue,
                    };

                if parsed.payload_offset > packet.len()
                    || payload_end > packet.len()
                {
                    continue;
                }

                let packet_format = (
                    parsed.channel_count as u16,
                    parsed.sample_rate,
                );

                match expected_format {
                    None => {
                        expected_format =
                            Some(packet_format);

                        if send_ws_format(
                            ws,
                            packet_format,
                            &node.stream_name,
                            &node.channel_labels,
                        )
                        .is_err()
                        {
                            return Ok(());
                        }
                    }

                    Some(expected)
                        if expected != packet_format =>
                    {
                        continue;
                    }

                    Some(_) => {}
                }

                let payload = packet
                    [parsed.payload_offset..payload_end]
                    .to_vec();

                let target_batch_frames =
                    (parsed.sample_rate as usize * WEB_AUDIO_BATCH_MS as usize / 1_000)
                        .max(parsed.samples_per_channel as usize);

                if batched_payload.is_empty() {
                    batch_started = Instant::now();
                }

                batched_payload.extend_from_slice(&payload);
                batched_frames += parsed.samples_per_channel as usize;

                if batched_frames >= target_batch_frames {
                    if flush_web_audio_batch(ws, &mut batched_payload).is_err() {
                        return Ok(());
                    }
                    batched_frames = 0;
                }
            }

            Err(ref error)
                if error.kind()
                    == std::io::ErrorKind::WouldBlock =>
            {
                if !batched_payload.is_empty()
                    && batch_started.elapsed()
                        >= Duration::from_millis(WEB_AUDIO_BATCH_MS as u64)
                {
                    if flush_web_audio_batch(ws, &mut batched_payload).is_err() {
                        return Ok(());
                    }
                    batched_frames = 0;
                }
            }

            Err(ref error)
                if error.kind()
                    == std::io::ErrorKind::TimedOut =>
            {
                if !batched_payload.is_empty()
                    && batch_started.elapsed()
                        >= Duration::from_millis(WEB_AUDIO_BATCH_MS as u64)
                {
                    if flush_web_audio_batch(ws, &mut batched_payload).is_err() {
                        return Ok(());
                    }
                    batched_frames = 0;
                }
            }

            Err(ref error)
                if error.kind()
                    == std::io::ErrorKind::ConnectionReset =>
            {
                return Ok(());
            }

            Err(error) => {
                return Err(format!(
                    "UDP receive error: {error}"
                ));
            }
        }
    }
}

fn flush_web_audio_batch(
    ws: &mut tungstenite::WebSocket<TcpStream>,
    payload: &mut Vec<u8>,
) -> Result<(), tungstenite::Error> {
    if payload.is_empty() {
        return Ok(());
    }

    let message = Message::Binary(std::mem::take(payload).into());
    ws.send(message)
}

fn send_ws_format(
    ws: &mut tungstenite::WebSocket<TcpStream>,
    format: (u16, u32),
    stream_name: &str,
    channel_labels: &[String],
) -> Result<(), tungstenite::Error> {
    let payload = serde_json::json!({
        "type": "format",
        "channels": format.0,
        "sampleRate": format.1,
        "streamName": stream_name,
        "channelLabels": channel_labels,
    });

    ws.send(Message::Text(
        payload.to_string().into(),
    ))
}

fn send_ws_error(
    ws: &mut tungstenite::WebSocket<TcpStream>,
    message: &str,
) {
    let payload = serde_json::json!({
        "type": "error",
        "message": message,
    });

    let _ = ws.send(Message::Text(
        payload.to_string().into(),
    ));
}