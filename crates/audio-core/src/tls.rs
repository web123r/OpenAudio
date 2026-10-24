//! TLS / HTTPS support for the OpenAudio Web Gateway.
//!
//! Provides automatic self-signed certificate generation (covering localhost
//! and LAN IP addresses) and stream abstraction for plain TCP and TLS streams.

use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::ServerConnection;
use rustls::{ServerConfig, StreamOwned};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TlsConfig {
    /// Plain unencrypted HTTP and WS (default).
    Disabled,
    /// Automatically generate an ephemeral self-signed certificate on startup
    /// covering `localhost`, `127.0.0.1`, and all detected local LAN IPv4 addresses.
    /// This satisfies the mobile browser "Secure Context" requirement for microphone capture.
    AutoSelfSigned,
    /// Load certificate and private key from custom PEM files.
    Custom {
        cert_path: String,
        key_path: String,
    },
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self::Disabled
    }
}

/// A network stream that can be either plain TCP or TLS-encrypted.
pub enum MaybeTlsStream {
    Plain(TcpStream),
    Tls(Box<StreamOwned<ServerConnection, TcpStream>>),
}

impl MaybeTlsStream {
    pub fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        match self {
            Self::Plain(s) => s.peer_addr(),
            Self::Tls(s) => s.get_ref().peer_addr(),
        }
    }

    pub fn set_read_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.set_read_timeout(timeout),
            Self::Tls(s) => s.get_ref().set_read_timeout(timeout),
        }
    }

    pub fn set_write_timeout(&self, timeout: Option<Duration>) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.set_write_timeout(timeout),
            Self::Tls(s) => s.get_ref().set_write_timeout(timeout),
        }
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.set_nonblocking(nonblocking),
            Self::Tls(s) => s.get_ref().set_nonblocking(nonblocking),
        }
    }

    pub fn is_tls(&self) -> bool {
        matches!(self, Self::Tls(_))
    }
}

impl Read for MaybeTlsStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::Tls(s) => s.read(buf),
        }
    }
}

impl Write for MaybeTlsStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::Tls(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::Tls(s) => s.flush(),
        }
    }
}

/// Creates a `rustls::ServerConfig` based on the specified `TlsConfig`.
pub fn create_server_config(config: &TlsConfig) -> Result<Option<Arc<ServerConfig>>, String> {
    match config {
        TlsConfig::Disabled => Ok(None),
        TlsConfig::AutoSelfSigned => {
            let server_config = generate_self_signed_config()?;
            Ok(Some(Arc::new(server_config)))
        }
        TlsConfig::Custom {
            cert_path,
            key_path,
        } => {
            let cert_pem = std::fs::read(cert_path)
                .map_err(|e| format!("failed to read cert file '{cert_path}': {e}"))?;
            let key_pem = std::fs::read(key_path)
                .map_err(|e| format!("failed to read key file '{key_path}': {e}"))?;

            let certs = rustls_pemfile::certs(&mut &cert_pem[..])
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| format!("invalid certificate PEM: {e}"))?;
            if certs.is_empty() {
                return Err("certificate PEM file contained no certificates".into());
            }

            let key = rustls_pemfile::private_key(&mut &key_pem[..])
                .map_err(|e| format!("invalid private key PEM: {e}"))?
                .ok_or_else(|| "no private key found in PEM file".to_string())?;

            let server_config = ServerConfig::builder()
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| format!("failed to build TLS server config: {e}"))?;

            Ok(Some(Arc::new(server_config)))
        }
    }
}

/// Generates an in-memory self-signed certificate and builds a `ServerConfig`.
fn generate_self_signed_config() -> Result<ServerConfig, String> {
    let mut sans = vec!["localhost".to_string(), "127.0.0.1".to_string()];

    if let Ok(interfaces) = if_addrs::get_if_addrs() {
        for iface in interfaces {
            if !iface.is_loopback() {
                if let IpAddr::V4(ipv4) = iface.ip() {
                    let ip_str = ipv4.to_string();
                    if !sans.contains(&ip_str) {
                        sans.push(ip_str);
                    }
                }
            }
        }
    }

    let cert = rcgen::generate_simple_self_signed(sans)
        .map_err(|e| format!("failed to generate self-signed certificate: {e}"))?;

    let cert_der = cert.cert.der().to_vec();
    let key_der = cert.key_pair.serialized_der().to_vec();

    let certs = vec![CertificateDer::from(cert_der)];
    let key = PrivateKeyDer::Pkcs8(key_der.into());

    ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("failed to configure TLS server with generated cert: {e}"))
}

/// Performs a TLS server handshake on an accepted `TcpStream`.
pub fn accept_tls_stream(
    stream: TcpStream,
    config: Arc<ServerConfig>,
) -> Result<MaybeTlsStream, String> {
    let conn = ServerConnection::new(config)
        .map_err(|e| format!("failed to create TLS server connection: {e}"))?;
    let owned = StreamOwned::new(conn, stream);
    Ok(MaybeTlsStream::Tls(Box::new(owned)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_config_returns_none() {
        let res = create_server_config(&TlsConfig::Disabled).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_auto_self_signed_generates_valid_server_config() {
        let res = create_server_config(&TlsConfig::AutoSelfSigned).unwrap();
        assert!(res.is_some());
    }
}
