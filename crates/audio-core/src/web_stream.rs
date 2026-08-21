use crate::ensure_realtime_audio_thread;

use crate::protocol::parse_packet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tungstenite::{accept, Message};

const PLAYER_HTML: &str = include_str!("../assets/web-player.html");

/// Receives one network audio stream over UDP and serves it two ways
/// on the same process, no external files needed:
/// - Plain HTTP on `http_port`: serves the bundled web-player.html
///   for any request, so any browser on the LAN can just navigate to
///   http://<this-machine-ip>:<http_port>/
/// - WebSocket on `http_port + 1`: streams raw interleaved Float32
///   PCM binary frames, preceded by a one-time JSON "format" message
///   (channel count, sample rate, and the stream's display name) so
///   the browser knows how to play it and what to show.
pub fn receive_and_serve_web(
    bind_addr: &str,
    http_port: u16,
    stream_name: String,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();

    let ws_port = http_port + 1;

    let socket = UdpSocket::bind(bind_addr).map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;
    socket
        .set_read_timeout(Some(Duration::from_millis(200)))
        .map_err(|e| format!("failed to set read timeout: {e}"))?;

    println!("Web-stream listening for UDP on {bind_addr}, waiting for first packet...");

    // Sized for worst case: protocol allows up to 255 channels at
    // MAX_FRAMES_PER_PACKET=58 frames, 4 bytes/sample:
    // 255 * 58 * 4 + headers ≈ 59KB. The old 1500-byte (MTU-sized)
    // buffer caused WSAEMSGSIZE the moment a packet carried more
    // than ~9 channels in one datagram (e.g. 32ch ASIO input).
    let mut buf = [0u8; 65536];
    let (channel_count, sample_rate) = loop {
        if !keep_running.load(Ordering::Relaxed) {
            return Ok(());
        }
        match socket.recv_from(&mut buf) {
            Ok((len, _src)) => {
                if let Some(parsed) = parse_packet(&buf[..len]) {
                    break (parsed.channel_count as u16, parsed.sample_rate);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => continue,
            Err(e) => return Err(format!("recv error while waiting for first packet: {e}")),
        }
    };

    println!(
        "Web-stream detected {channel_count}ch @ {sample_rate}Hz. Starting HTTP on :{http_port} and WebSocket on :{ws_port}..."
    );

    // Plain HTTP server: serves the bundled player page for any request.
    let http_keep_running = keep_running.clone();
    thread::spawn(move || {
        let listener = match TcpListener::bind(("0.0.0.0", http_port)) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("audio-core: failed to bind HTTP port {http_port}: {e}");
                return;
            }
        };
        let _ = listener.set_nonblocking(true);

        while http_keep_running.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    thread::spawn(move || {
                        let _ = serve_http_page(stream);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(_) => thread::sleep(Duration::from_millis(100)),
            }
        }
    });

    // WebSocket server: streams the actual audio + format/name info.
    let clients: Arc<Mutex<Vec<std::sync::mpsc::Sender<Vec<u8>>>>> = Arc::new(Mutex::new(Vec::new()));

    let ws_listener = TcpListener::bind(("0.0.0.0", ws_port))
        .map_err(|e| format!("failed to bind websocket port {ws_port}: {e}"))?;
    ws_listener
        .set_nonblocking(true)
        .map_err(|e| format!("failed to set listener nonblocking: {e}"))?;

    let clients_for_accept = clients.clone();
    let accept_keep_running = keep_running.clone();
    let format_json = format!(
        r#"{{"type":"format","channels":{channel_count},"sampleRate":{sample_rate},"streamName":{}}}"#,
        serde_json::to_string(&stream_name).unwrap_or_else(|_| "\"stream\"".to_string())
    );

    thread::spawn(move || {
        while accept_keep_running.load(Ordering::Relaxed) {
            match ws_listener.accept() {
                Ok((stream, _addr)) => {
                    let format_json = format_json.clone();
                    let clients_for_conn = clients_for_accept.clone();
                    thread::spawn(move || {
                        let _ = stream.set_nonblocking(false);
                        let mut ws = match accept(stream) {
                            Ok(ws) => ws,
                            Err(_) => return,
                        };
                        if ws.send(Message::Text(format_json.into())).is_err() {
                            return;
                        }
                        let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();
                        clients_for_conn.lock().unwrap().push(tx);
                        for chunk in rx {
                            if ws.send(Message::Binary(chunk.into())).is_err() {
                                break;
                            }
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(100));
                }
                Err(_) => thread::sleep(Duration::from_millis(100)),
            }
        }
    });

    let mut packets_received: u32 = 0;

    while keep_running.load(Ordering::Relaxed) {
        let (len, _src) = match socket.recv_from(&mut buf) {
            Ok(r) => r,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => continue,
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => continue,
            Err(e) => return Err(format!("recv error: {e}")),
        };

        let Some(parsed) = parse_packet(&buf[..len]) else { continue };
        if parsed.channel_count as u16 != channel_count || parsed.sample_rate != sample_rate {
            continue;
        }
        packets_received += 1;

        let sample_count = parsed.samples_per_channel as usize * parsed.channel_count as usize;
        let payload = buf[parsed.payload_offset..parsed.payload_offset + sample_count * 4].to_vec();

        let mut clients_guard = clients.lock().unwrap();
        clients_guard.retain(|tx| tx.send(payload.clone()).is_ok());
    }

    println!("Web-stream done. {packets_received} packets received.");
    Ok(())
}

fn serve_http_page(mut stream: TcpStream) -> std::io::Result<()> {
    // We don't need to parse the request at all -- read whatever's
    // sent (to avoid a connection-reset on some browsers that wait
    // for the server to read before closing) and always respond with
    // the same page, regardless of path.
    let mut discard = [0u8; 1024];
    let _ = stream.read(&mut discard);

    let body = PLAYER_HTML.as_bytes();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}