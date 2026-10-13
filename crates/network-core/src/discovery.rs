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
const MAX_DISCOVERED_NODES: usize = 256;
const MAX_SUBSCRIBER_STREAMS: usize = 1_024;
const MAX_SUBSCRIBERS_PER_STREAM: usize = 64;
const SUBSCRIBER_LEASE: Duration = Duration::from_secs(15);
const MAX_NODE_ID_BYTES: usize = 128;
const MAX_NODE_NAME_BYTES: usize = 128;
const MAX_STREAM_NAME_BYTES: usize = 128;
const MAX_CHANNEL_LABEL_BYTES: usize = 128;
const MAX_SUPPORTED_CHANNELS: u8 = 64;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct NodeAdvertisement {
    pub node_id: String,
    pub node_name: String,
    pub stream_id: u32,
    pub stream_name: String,
    pub control_port: u16,
    pub channel_count: u8,
    #[serde(default)]
    pub channel_labels: Vec<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SubscribeRequest {
    pub stream_id: u32,
    pub receiver_port: u16,
}

#[derive(Clone)]
pub struct Subscriber {
    pub address: SocketAddr,
    last_seen: Instant,
}

pub fn default_channel_labels(channel_count: u8) -> Vec<String> {
    (0..channel_count)
        .map(|index| format!("Channel {}", index + 1))
        .collect()
}

pub fn normalize_channel_labels(channel_count: u8, mut labels: Vec<String>) -> Vec<String> {
    let expected = channel_count as usize;
    labels.truncate(expected);

    while labels.len() < expected {
        labels.push(format!("Channel {}", labels.len() + 1));
    }

    labels
}

fn valid_advertisement(advert: &NodeAdvertisement) -> bool {
    let valid_text = |value: &str, maximum_bytes: usize| {
        !value.trim().is_empty()
            && value.len() <= maximum_bytes
            && !value.chars().any(char::is_control)
    };

    valid_text(&advert.node_id, MAX_NODE_ID_BYTES)
        && valid_text(&advert.node_name, MAX_NODE_NAME_BYTES)
        && valid_text(&advert.stream_name, MAX_STREAM_NAME_BYTES)
        && advert.control_port != 0
        && (1..=MAX_SUPPORTED_CHANNELS).contains(&advert.channel_count)
        && advert.channel_labels.len() <= advert.channel_count as usize
        && advert
            .channel_labels
            .iter()
            .all(|label| valid_text(label, MAX_CHANNEL_LABEL_BYTES))
}

fn valid_subscription_request(request: &SubscribeRequest) -> bool {
    request.receiver_port != 0
}

pub type SubscriberRegistry = Arc<Mutex<HashMap<u32, Vec<Subscriber>>>>;

pub fn active_subscribers(
    subscribers_by_stream: &SubscriberRegistry,
    stream_id: u32,
) -> Option<Vec<SocketAddr>> {
    let now = Instant::now();
    let mut registry = subscribers_by_stream
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    for subscribers in registry.values_mut() {
        subscribers
            .retain(|subscriber| now.duration_since(subscriber.last_seen) < SUBSCRIBER_LEASE);
    }
    registry.retain(|_, subscribers| !subscribers.is_empty());

    registry.get(&stream_id).map(|subscribers| {
        subscribers
            .iter()
            .map(|subscriber| subscriber.address)
            .collect()
    })
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
    pub channel_labels: Vec<String>,
    pub last_seen: Instant,
}

pub fn generate_node_id(stream_id: u32) -> String {
    let pid = std::process::id();
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    format!("node-{pid}-{stream_id}-{nanos}")
}

/// Returns all local, non-loopback IPv4 addresses (one per active adapter).
pub fn get_local_ipv4_addrs() -> Vec<Ipv4Addr> {
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
    control_port: u16,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    start_advertising_with_labels(
        node_name,
        stream_id,
        stream_name,
        channel_count,
        default_channel_labels(channel_count),
        control_port,
        keep_running,
    )
}

pub fn start_advertising_with_labels(
    node_name: String,
    stream_id: u32,
    stream_name: String,
    channel_count: u8,
    channel_labels: Vec<String>,
    control_port: u16,
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
        control_port,
        channel_count,
        channel_labels: normalize_channel_labels(channel_count, channel_labels),
    };
    let payload =
        serde_json::to_vec(&advert).map_err(|e| format!("failed to encode advertisement: {e}"))?;

    while keep_running.load(Ordering::Relaxed) {
        let interfaces = get_local_ipv4_addrs();

        if interfaces.is_empty() {
            eprintln!("network-core: no local network interfaces found for advertising");
        }

        for iface_ip in &interfaces {
            let sock = match Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP)) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("network-core: failed to create send socket: {e}");
                    continue;
                }
            };
            if let Err(e) = sock.set_reuse_address(true) {
                eprintln!("network-core: set_reuse_address failed: {e}");
            }

            let bind_addr: SocketAddr = SocketAddr::new((*iface_ip).into(), 0);
            if let Err(e) = sock.bind(&bind_addr.into()) {
                eprintln!("network-core: bind to {iface_ip} failed: {e}");
                continue;
            }
            if let Err(e) = sock.set_multicast_if_v4(iface_ip) {
                eprintln!("network-core: set_multicast_if_v4 on {iface_ip} failed: {e}");
                continue;
            }
            if let Err(e) = sock.set_broadcast(true) {
                eprintln!("network-core: set_broadcast failed: {e}");
            }

            let udp: UdpSocket = sock.into();
            if let Err(e) = udp.send_to(&payload, dest) {
                eprintln!("network-core: send via {iface_ip} failed: {e}");
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
        eprintln!("network-core: WARNING no interfaces found, falling back to UNSPECIFIED");
        sock.join_multicast_v4(&multicast_ip, &Ipv4Addr::UNSPECIFIED)
            .map_err(|e| format!("failed to join multicast group: {e}"))?;
    } else {
        for iface_ip in &interfaces {
            match sock.join_multicast_v4(&multicast_ip, iface_ip) {
                Ok(_) => println!("network-core: joined multicast group on {iface_ip}"),
                Err(e) => eprintln!("network-core: failed to join multicast on {iface_ip}: {e}"),
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
                    if valid_advertisement(&advert) {
                        let node = DiscoveredNode {
                            node_id: advert.node_id.clone(),
                            node_name: advert.node_name,
                            stream_id: advert.stream_id,
                            stream_name: advert.stream_name,
                            ip: src.ip().to_string(),
                            control_port: advert.control_port,
                            channel_count: advert.channel_count,
                            channel_labels: normalize_channel_labels(
                                advert.channel_count,
                                advert.channel_labels,
                            ),
                            last_seen: Instant::now(),
                        };

                        let mut directory = directory.lock().unwrap();
                        if directory.contains_key(&advert.node_id)
                            || directory.len() < MAX_DISCOVERED_NODES
                        {
                            directory.insert(advert.node_id, node);
                        }
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(e) => eprintln!("network-core: discovery recv error: {e}"),
        }

        let mut dir = directory.lock().unwrap();
        dir.retain(|_, node| node.last_seen.elapsed() < Duration::from_secs(3));
    }

    Ok(())
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
    let payload =
        serde_json::to_vec(&req).map_err(|e| format!("failed to encode subscribe request: {e}"))?;
    socket
        .send_to(&payload, &dest)
        .map_err(|e| format!("failed to send subscribe request to {dest}: {e}"))?;
    Ok(())
}

pub fn start_control_listener(
    subscribers_by_stream: SubscriberRegistry,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    run_control_listener(
        bind_control_listener()?,
        subscribers_by_stream,
        keep_running,
    )
}

pub fn bind_control_listener() -> Result<UdpSocket, String> {
    bind_control_listener_on(CONTROL_PORT)
}

pub fn bind_control_listener_on(port: u16) -> Result<UdpSocket, String> {
    let socket = UdpSocket::bind(("0.0.0.0", port))
        .map_err(|e| format!("failed to bind control listener on port {port}: {e}"))?;
    socket
        .set_read_timeout(Some(Duration::from_millis(500)))
        .map_err(|e| format!("failed to set read timeout: {e}"))?;

    Ok(socket)
}

pub fn run_control_listener(
    socket: UdpSocket,
    subscribers_by_stream: SubscriberRegistry,
    keep_running: Arc<AtomicBool>,
) -> Result<(), String> {
    let mut buf = [0u8; 256];

    while keep_running.load(Ordering::Relaxed) {
        match socket.recv_from(&mut buf) {
            Ok((len, src)) => {
                if let Ok(req) = serde_json::from_slice::<SubscribeRequest>(&buf[..len]) {
                    if valid_subscription_request(&req) {
                        let dest = SocketAddr::new(src.ip(), req.receiver_port);
                        let mut map = subscribers_by_stream.lock().unwrap();

                        let now = Instant::now();
                        for subscribers in map.values_mut() {
                            subscribers.retain(|subscriber| {
                                now.duration_since(subscriber.last_seen) < SUBSCRIBER_LEASE
                            });
                        }
                        map.retain(|_, subscribers| !subscribers.is_empty());

                        if map.contains_key(&req.stream_id) || map.len() < MAX_SUBSCRIBER_STREAMS {
                            let list = map.entry(req.stream_id).or_default();
                            if let Some(subscriber) = list
                                .iter_mut()
                                .find(|subscriber| subscriber.address == dest)
                            {
                                subscriber.last_seen = now;
                            } else if list.len() < MAX_SUBSCRIBERS_PER_STREAM {
                                println!(
                                    "network-core: new subscriber for stream {}: {dest}",
                                    req.stream_id
                                );
                                list.push(Subscriber {
                                    address: dest,
                                    last_seen: now,
                                });
                            }
                        }
                    }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(e) => eprintln!("network-core: control listener recv error: {e}"),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        active_subscribers, bind_control_listener_on, valid_advertisement,
        valid_subscription_request, NodeAdvertisement, SubscribeRequest, Subscriber,
        SubscriberRegistry, MAX_SUPPORTED_CHANNELS, SUBSCRIBER_LEASE,
    };
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    #[test]
    fn control_listener_supports_advertised_ephemeral_ports() {
        let socket = bind_control_listener_on(0).unwrap();
        assert_ne!(socket.local_addr().unwrap().port(), 0);
    }

    fn advertisement() -> NodeAdvertisement {
        NodeAdvertisement {
            node_id: "node-1".to_string(),
            node_name: "Sender".to_string(),
            stream_id: 1,
            stream_name: "Main".to_string(),
            control_port: 7000,
            channel_count: 2,
            channel_labels: vec!["Left".to_string(), "Right".to_string()],
        }
    }

    #[test]
    fn discovery_advertisement_requires_valid_metadata_and_supported_channels() {
        assert!(valid_advertisement(&advertisement()));

        let mut invalid = advertisement();
        invalid.control_port = 0;
        assert!(!valid_advertisement(&invalid));

        let mut invalid = advertisement();
        invalid.channel_count = MAX_SUPPORTED_CHANNELS + 1;
        assert!(!valid_advertisement(&invalid));

        let mut invalid = advertisement();
        invalid.node_name = " \n".to_string();
        assert!(!valid_advertisement(&invalid));
    }

    #[test]
    fn zero_receiver_port_is_not_a_valid_subscription() {
        let request = SubscribeRequest {
            stream_id: 1,
            receiver_port: 0,
        };
        assert!(!valid_subscription_request(&request));

        let request = SubscribeRequest {
            stream_id: 1,
            receiver_port: 7050,
        };
        assert!(valid_subscription_request(&request));
    }

    #[test]
    fn subscriber_snapshot_expires_inactive_receivers() {
        let registry: SubscriberRegistry = Arc::new(Mutex::new(HashMap::from([(
            1,
            vec![Subscriber {
                address: "127.0.0.1:7050".parse::<SocketAddr>().unwrap(),
                last_seen: Instant::now() - SUBSCRIBER_LEASE - Duration::from_secs(1),
            }],
        )])));

        assert!(active_subscribers(&registry, 1).is_none());
        assert!(registry.lock().unwrap().is_empty());
    }
}
