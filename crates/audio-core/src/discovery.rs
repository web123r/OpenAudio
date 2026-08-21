use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const DISCOVERY_MULTICAST_ADDR: &str = "239.19.84.1";
pub const DISCOVERY_PORT: u16 = 5450;
pub const CONTROL_PORT: u16 = 7000;

pub type SubscriberRegistry = Arc<Mutex<HashMap<u32, Vec<SocketAddr>>>>;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NodeAdvertisement {
    pub node_id: String,
    pub node_name: String,
    pub stream_id: u32,
    pub stream_name: String,
    pub control_port: u16,
    pub channel_count: u8,
}

#[derive(Clone, Debug)]
pub struct DiscoveredNode {
    pub node_id: String,
    pub node_name: String,
    pub stream_id: u32,
    pub stream_name: String,
    pub ip: String,
    pub control_port: u16,
    pub channel_count: u8,
    pub last_seen: Instant,
}

fn generate_node_id(stream_id: u32) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("node-{pid}-{stream_id}-{nanos}")
}

/// Returns all local, non-loopback IPv4 addresses (one per active adapter).
fn get_local_ipv4_addrs() -> Vec<Ipv4Addr> {
    if_addrs::get_if_addrs()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|iface| match iface.addr {
            if_addrs::IfAddr::V4(v4) if !iface.is_loopback() => Some(v4.ip),
            _ => None,
        })
        .collect()
}

/// Broadcasts a Stream Advertisement on EVERY local network interface,
/// once per second, until `keep_running` is false. Sending on every
/// interface (instead of letting the OS pick one) guarantees delivery
/// on multi-adapter Windows machines where UNSPECIFIED selection is
/// unreliable.
pub fn start_advertising(
    node_name: String,
    stream_id: u32,
    stream_name: String,
    channel_count: u8,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    let dest: SocketAddr = format!("{DISCOVERY_MULTICAST_ADDR}:{DISCOVERY_PORT}")
        .parse()
        .map_err(|e| format!("bad discovery address: {e}"))?;

    let node_id = generate_node_id(stream_id);
    let advert = NodeAdvertisement {
        node_id,
        node_name,
        stream_id,
        stream_name,
        control_port: CONTROL_PORT,
        channel_count,
    };
    let payload = serde_json::to_vec(&advert)
        .map_err(|e| format!("failed to encode advertisement: {e}"))?;

    while keep_running.load(Ordering::Relaxed) {
        let interfaces = get_local_ipv4_addrs();

        if interfaces.is_empty() {
            eprintln!("audio-core: no local network interfaces found for advertising");
        }

        for iface_ip in &interfaces {
            let sock = match Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("audio-core: failed to create send socket: {e}");
                    continue;
                }
            };
            if let Err(e) = sock.set_reuse_address(true) {
                eprintln!("audio-core: set_reuse_address failed: {e}");
            }

            let bind_addr: SocketAddr = SocketAddr::new((*iface_ip).into(), 0);
            if let Err(e) = sock.bind(&bind_addr.into()) {
                eprintln!("audio-core: bind to {iface_ip} failed: {e}");
                continue;
            }
            if let Err(e) = sock.set_multicast_if_v4(iface_ip) {
                eprintln!("audio-core: set_multicast_if_v4 on {iface_ip} failed: {e}");
                continue;
            }
            if let Err(e) = sock.set_broadcast(true) {
                eprintln!("audio-core: set_broadcast failed: {e}");
            }

            let udp: UdpSocket = sock.into();
            if let Err(e) = udp.send_to(&payload, dest) {
                eprintln!("audio-core: send via {iface_ip} failed: {e}");
            }
        }

        std::thread::sleep(Duration::from_secs(1));
    }

    Ok(())
}

/// Listens for Stream Advertisements from ANY node/stream on ANY local
/// interface, and maintains a live directory, pruning entries not seen
/// in 3+ seconds.
pub fn start_discovery_listener(
    directory: Arc<Mutex<HashMap<String, DiscoveredNode>>>,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))
        .map_err(|e| format!("failed to create discovery socket: {e}"))?;
    sock.set_reuse_address(true)
        .map_err(|e| format!("failed to set reuse_address: {e}"))?;

    let bind_addr: SocketAddr = format!("0.0.0.0:{DISCOVERY_PORT}")
        .parse()
        .map_err(|e| format!("bad bind address: {e}"))?;
    sock.bind(&bind_addr.into())
        .map_err(|e| format!("failed to bind discovery listener on port {DISCOVERY_PORT}: {e}"))?;

    let multicast_ip: Ipv4Addr = DISCOVERY_MULTICAST_ADDR
        .parse()
        .map_err(|e| format!("bad multicast addr: {e}"))?;

    let interfaces = get_local_ipv4_addrs();
    if interfaces.is_empty() {
        eprintln!("audio-core: WARNING no interfaces found, falling back to UNSPECIFIED");
        sock.join_multicast_v4(&multicast_ip, &Ipv4Addr::UNSPECIFIED)
            .map_err(|e| format!("failed to join multicast group: {e}"))?;
    } else {
        for iface_ip in &interfaces {
            match sock.join_multicast_v4(&multicast_ip, iface_ip) {
                Ok(_) => println!("audio-core: joined multicast group on {iface_ip}"),
                Err(e) => eprintln!("audio-core: failed to join multicast on {iface_ip}: {e}"),
            }
        }
    }

    sock.set_read_timeout(Some(Duration::from_millis(500)))
        .map_err(|e| format!("failed to set read timeout: {e}"))?;

    let socket: UdpSocket = sock.into();
    let mut buf = [0u8; 1024];

    while keep_running.load(Ordering::Relaxed) {
        match socket.recv_from(&mut buf) {
            Ok((len, src)) => {
                if let Ok(advert) = serde_json::from_slice::<NodeAdvertisement>(&buf[..len]) {
                    let node = DiscoveredNode {
                        node_id: advert.node_id.clone(),
                        node_name: advert.node_name,
                        stream_id: advert.stream_id,
                        stream_name: advert.stream_name,
                        ip: src.ip().to_string(),
                        control_port: advert.control_port,
                        channel_count: advert.channel_count,
                        last_seen: Instant::now(),
                    };
                    directory.lock().unwrap().insert(advert.node_id, node);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(e) => eprintln!("audio-core: discovery recv error: {e}"),
        }

        let mut dir = directory.lock().unwrap();
        dir.retain(|_, node| node.last_seen.elapsed() < Duration::from_secs(3));
    }

    Ok(())
}

#[derive(Serialize, Deserialize)]
struct SubscribeRequest {
    stream_id: u32,
    receiver_port: u16,
}

pub fn send_subscribe_request(
    publisher_ip: &str,
    control_port: u16,
    stream_id: u32,
    my_receive_port: u16,
) -> Result<(), String> {
    let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| format!("failed to bind socket: {e}"))?;
    let dest = format!("{publisher_ip}:{control_port}");
    let req = SubscribeRequest {
        stream_id,
        receiver_port: my_receive_port,
    };
    let payload = serde_json::to_vec(&req)
        .map_err(|e| format!("failed to encode subscribe request: {e}"))?;
    socket
        .send_to(&payload, &dest)
        .map_err(|e| format!("failed to send subscribe request to {dest}: {e}"))?;
    Ok(())
}

pub fn start_control_listener(
    subscribers_by_stream: SubscriberRegistry,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    let socket = UdpSocket::bind(("0.0.0.0", CONTROL_PORT))
        .map_err(|e| format!("failed to bind control listener on port {CONTROL_PORT}: {e}"))?;
    socket
        .set_read_timeout(Some(Duration::from_millis(500)))
        .map_err(|e| format!("failed to set read timeout: {e}"))?;

    let mut buf = [0u8; 256];

    while keep_running.load(Ordering::Relaxed) {
        match socket.recv_from(&mut buf) {
            Ok((len, src)) => {
                if let Ok(req) = serde_json::from_slice::<SubscribeRequest>(&buf[..len]) {
                    let dest = SocketAddr::new(src.ip(), req.receiver_port);
                    let mut map = subscribers_by_stream.lock().unwrap();
                    let list = map.entry(req.stream_id).or_insert_with(Vec::new);
                    if !list.contains(&dest) {
                        println!(
                            "audio-core: new subscriber for stream {}: {dest}",
                            req.stream_id
                        );
                        list.push(dest);
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(e) => eprintln!("audio-core: control listener recv error: {e}"),
        }
    }

    Ok(())
}