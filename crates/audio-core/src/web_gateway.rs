use crate::ensure_realtime_audio_thread;
use crate::discovery::{send_subscribe_request, DiscoveredNode};
use crate::protocol::parse_packet;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use tungstenite::{accept, Message};

const PLAYER_HTML: &str = include_str!("../assets/web-player.html");

pub type DiscoveryDirectory = Arc<Mutex<HashMap<String, DiscoveredNode>>>;

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

/// Runs ONE HTTP+WebSocket gateway for the whole app (not per-stream).
/// Serves the player page + a live /api/streams JSON list backed by
/// the same `directory` your discovery listener already maintains.
/// Each browser connection independently selects a stream and gets
/// its own UDP subscription + relay thread.
pub fn run_web_gateway(
    http_port: u16,
    directory: DiscoveryDirectory,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();
    let ws_port = http_port + 1;

    // --- HTTP: page + /api/streams ---
    let http_directory = directory.clone();
    let http_keep_running = keep_running.clone();
    thread::spawn(move || {
        let listener = match TcpListener::bind(("0.0.0.0", http_port)) {
            Ok(l) => l,
            Err(e) => { eprintln!("audio-core: failed to bind HTTP port {http_port}: {e}"); return; }
        };
        let _ = listener.set_nonblocking(true);
        while http_keep_running.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    let dir = http_directory.clone();
                    thread::spawn(move || { let _ = handle_http_request(stream, &dir); });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(100)),
                Err(_) => thread::sleep(Duration::from_millis(100)),
            }
        }
    });

    // --- WebSocket: per-client stream selection + relay ---
    let ws_listener = TcpListener::bind(("0.0.0.0", ws_port))
        .map_err(|e| format!("failed to bind websocket port {ws_port}: {e}"))?;
    ws_listener.set_nonblocking(true).map_err(|e| e.to_string())?;

    println!("Web gateway ready: HTTP on :{http_port}, WebSocket on :{ws_port}");

    while keep_running.load(Ordering::Relaxed) {
        match ws_listener.accept() {
            Ok((stream, addr)) => {
                let dir = directory.clone();
                let client_keep_running = keep_running.clone();
                thread::spawn(move || {
                    if let Err(e) = handle_ws_client(stream, dir, client_keep_running) {
                        eprintln!("audio-core: ws client {addr} ended: {e}");
                    }
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(100)),
            Err(_) => thread::sleep(Duration::from_millis(100)),
        }
    }
    Ok(())
}

fn handle_http_request(mut stream: TcpStream, directory: &DiscoveryDirectory) -> std::io::Result<()> {
    let mut buf = [0u8; 2048];
    let n = stream.read(&mut buf).unwrap_or(0);
    let request = String::from_utf8_lossy(&buf[..n]);
    let path = request.lines().next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    if path.starts_with("/api/streams") {
        let streams: Vec<StreamInfo> = directory.lock().unwrap().values().map(|n| StreamInfo {
            node_id: n.node_id.clone(),
            node_name: n.node_name.clone(),
            stream_id: n.stream_id,
            stream_name: n.stream_name.clone(),
            ip: n.ip.clone(),
            channel_count: n.channel_count,
        }).collect();
        let body = serde_json::to_vec(&streams).unwrap_or_else(|_| b"[]".to_vec());
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(response.as_bytes())?;
        stream.write_all(&body)?;
        return stream.flush();
    }

    let body = PLAYER_HTML.as_bytes();
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(response.as_bytes())?;
    stream.write_all(body)?;
    stream.flush()
}

fn handle_ws_client(
    stream: TcpStream,
    directory: DiscoveryDirectory,
    global_keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    ensure_realtime_audio_thread();
    stream.set_nonblocking(false).map_err(|e| e.to_string())?;
    let mut ws = accept(stream).map_err(|e| format!("ws handshake failed: {e}"))?;

    // Block until the browser tells us which stream it wants.
    let node = loop {
        let msg = ws.read().map_err(|e| format!("ws read failed: {e}"))?;
        if let Message::Text(text) = msg {
            if let Ok(sel) = serde_json::from_str::<ClientSelectMessage>(&text) {
                if sel.msg_type == "select" {
                    let found = directory.lock().unwrap().get(&sel.node_id).cloned();
                    match found {
                        Some(n) => break n,
                        None => {
                            let _ = ws.send(Message::Text(
                                r#"{"type":"error","message":"Stream no longer available"}"#.into(),
                            ));
                            continue;
                        }
                    }
                }
            }
        }
    };

    // Independent UDP socket per browser session -- lets multiple tabs
    // subscribe to different (or the same) streams concurrently.
    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| e.to_string())?;
    socket.set_read_timeout(Some(Duration::from_millis(200))).map_err(|e| e.to_string())?;
    let my_port = socket.local_addr().map_err(|e| e.to_string())?.port();

    let session_active = Arc::new(AtomicBool::new(true));

    // Re-send the subscribe request periodically -- discovery.rs's
    // subscriber registry never expires entries, so this is cheap
    // insurance in case the publisher process restarts mid-session.
    let resub_node = node.clone();
    let resub_active = session_active.clone();
    let resub_keep_running = global_keep_running.clone();
    thread::spawn(move || {
        while resub_active.load(Ordering::Relaxed) && resub_keep_running.load(Ordering::Relaxed) {
            let _ = send_subscribe_request(&resub_node.ip, resub_node.control_port, resub_node.stream_id, my_port);
            thread::sleep(Duration::from_secs(5));
        }
    });

    let mut buf = [0u8; 65536];
    let mut format_sent = false;
    let mut channel_count: u16 = 0;
    let mut sample_rate: u32 = 0;

    let result = loop {
        if !global_keep_running.load(Ordering::Relaxed) { break Ok(()); }
        match socket.recv_from(&mut buf) {
            Ok((len, _src)) => {
                let Some(parsed) = parse_packet(&buf[..len]) else { continue };

                if !format_sent {
                    channel_count = parsed.channel_count as u16;
                    sample_rate = parsed.sample_rate;
                    let format_json = format!(
                        r#"{{"type":"format","channels":{channel_count},"sampleRate":{sample_rate},"streamName":{}}}"#,
                        serde_json::to_string(&node.stream_name).unwrap_or_else(|_| "\"stream\"".to_string())
                    );
                    if ws.send(Message::Text(format_json.into())).is_err() { break Ok(()); }
                    format_sent = true;
                }

                if parsed.channel_count as u16 != channel_count || parsed.sample_rate != sample_rate {
                    continue;
                }

                let sample_count = parsed.samples_per_channel as usize * parsed.channel_count as usize;
                let payload = buf[parsed.payload_offset..parsed.payload_offset + sample_count * 4].to_vec();
                if ws.send(Message::Binary(payload.into())).is_err() { break Ok(()); }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(e) => break Err(format!("udp recv error: {e}")),
        }
    };

    session_active.store(false, Ordering::Relaxed);
    result
}
