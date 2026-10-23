use aes_gcm::{
    aead::{Aead, KeyInit, Payload},
    Aes256Gcm, Nonce,
};
use argon2::Argon2;
use sha2::Digest;

pub struct StreamCipher {
    cipher: Aes256Gcm,
}

impl StreamCipher {
    pub fn new(key: &[u8; 32]) -> Self {
        let cipher = Aes256Gcm::new(key.into());
        Self { cipher }
    }

    /// Derives a 256-bit cryptographic key deterministically from a user-provided passphrase
    /// using Argon2id with an OpenAudio domain separation salt.
    pub fn from_passphrase(passphrase: &str) -> Self {
        let key = derive_key_from_passphrase(passphrase);
        Self::new(&key)
    }

    /// Derives a 256-bit key from a user-provided passphrase and credential-specific salt.
    /// This prevents precomputation / rainbow table attacks across different sessions or streams.
    pub fn from_passphrase_with_salt(passphrase: &str, salt: &[u8]) -> Self {
        let key = derive_key_from_passphrase_with_salt(passphrase, salt);
        Self::new(&key)
    }

    pub fn encrypt(&self, nonce_bytes: &[u8; 12], plaintext: &[u8]) -> Result<Vec<u8>, String> {
        let nonce = Nonce::from_slice(nonce_bytes);
        self.cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| format!("encryption failed: {e}"))
    }

    pub fn decrypt(&self, nonce_bytes: &[u8; 12], ciphertext: &[u8]) -> Result<Vec<u8>, String> {
        let nonce = Nonce::from_slice(nonce_bytes);
        self.cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| format!("decryption failed: {e}"))
    }

    pub fn encrypt_with_aad(&self, nonce_bytes: &[u8; 12], plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, String> {
        let nonce = Nonce::from_slice(nonce_bytes);
        let payload = Payload {
            msg: plaintext,
            aad,
        };
        self.cipher
            .encrypt(nonce, payload)
            .map_err(|e| format!("authenticated encryption failed: {e}"))
    }

    pub fn decrypt_with_aad(&self, nonce_bytes: &[u8; 12], ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>, String> {
        let nonce = Nonce::from_slice(nonce_bytes);
        let payload = Payload {
            msg: ciphertext,
            aad,
        };
        self.cipher
            .decrypt(nonce, payload)
            .map_err(|e| format!("authenticated decryption failed: {e}"))
    }
}

/// Default OpenAudio domain separation salt.
pub const DEFAULT_ARGON2_DOMAIN_SALT: &[u8] = b"OpenAudio-V1-Argon2id-Salt";

/// Derives a 32-byte key from a user passphrase using Argon2id with default domain salt.
pub fn derive_key_from_passphrase(passphrase: &str) -> [u8; 32] {
    derive_key_from_passphrase_with_salt(passphrase, DEFAULT_ARGON2_DOMAIN_SALT)
}

/// Derives a 32-byte key from a user passphrase using Argon2id with a custom per-stream/per-user salt.
pub fn derive_key_from_passphrase_with_salt(passphrase: &str, salt: &[u8]) -> [u8; 32] {
    let effective_salt = if salt.is_empty() {
        DEFAULT_ARGON2_DOMAIN_SALT
    } else {
        salt
    };
    let mut key = [0u8; 32];
    let argon2 = Argon2::default();
    if argon2.hash_password_into(passphrase.as_bytes(), effective_salt, &mut key).is_err() {
        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, effective_salt);
        sha2::Digest::update(&mut hasher, passphrase.as_bytes());
        let result = sha2::Digest::finalize(hasher);
        key.copy_from_slice(&result);
    }
    key
}

/// Constructs a unique 12-byte (96-bit) nonce per packet for AES-GCM.
/// Binds stream_id, sequence_number, and presentation_timestamp_ns to ensure
/// nonces are never reused across restarts or sequence rollovers.
pub fn packet_nonce(stream_id: u32, sequence_number: u32, timestamp_ns: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[0..4].copy_from_slice(&stream_id.to_be_bytes());
    nonce[4..8].copy_from_slice(&sequence_number.to_be_bytes());
    let ts_folded = ((timestamp_ns >> 32) ^ timestamp_ns) as u32;
    nonce[8..12].copy_from_slice(&ts_folded.to_be_bytes());
    nonce
}

/// Encrypts an unencrypted OpenAudio Float32 audio packet into an encrypted OpenAudio packet.
/// Plaintext encompasses the AudioPayloadHeader and PCM samples.
/// The standard 24-byte PacketHeader is authenticated as Associated Authenticated Data (AAD)
/// to prevent header tampering.
pub fn encrypt_audio_packet(cipher: &StreamCipher, raw_packet: &[u8]) -> Result<Vec<u8>, String> {
    if raw_packet.len() < 32 {
        return Err("raw packet too short to encrypt".to_string());
    }
    let stream_id = u32::from_be_bytes(raw_packet[8..12].try_into().map_err(|_| "invalid stream_id")?);
    let sequence_number = u32::from_be_bytes(raw_packet[12..16].try_into().map_err(|_| "invalid seq")?);
    let timestamp_ns = u64::from_be_bytes(raw_packet[16..24].try_into().map_err(|_| "invalid timestamp")?);
    let nonce = packet_nonce(stream_id, sequence_number, timestamp_ns);

    let mut header = raw_packet[0..24].to_vec();
    header[5] = network_core::protocol::PACKET_TYPE_AUDIO_ENCRYPTED;

    let ciphertext = cipher.encrypt_with_aad(&nonce, &raw_packet[24..], &header)?;
    let mut encrypted = Vec::with_capacity(24 + ciphertext.len());
    encrypted.extend_from_slice(&header);
    encrypted.extend_from_slice(&ciphertext);
    Ok(encrypted)
}

/// Decrypts an encrypted OpenAudio packet back into a standard OpenAudio Float32 audio packet.
/// Validates both ciphertext authenticity and packet header AAD integrity.
pub fn decrypt_audio_packet(cipher: &StreamCipher, encrypted_packet: &[u8]) -> Result<Vec<u8>, String> {
    if encrypted_packet.len() < 24 + 16 {
        return Err("encrypted packet too short".to_string());
    }
    if &encrypted_packet[0..4] != b"OAv1"
        || encrypted_packet[4] != 1
        || encrypted_packet[5] != network_core::protocol::PACKET_TYPE_AUDIO_ENCRYPTED
    {
        return Err("not an encrypted OpenAudio packet".to_string());
    }

    let stream_id = u32::from_be_bytes(encrypted_packet[8..12].try_into().map_err(|_| "invalid stream_id")?);
    let sequence_number = u32::from_be_bytes(encrypted_packet[12..16].try_into().map_err(|_| "invalid seq")?);
    let timestamp_ns = u64::from_be_bytes(encrypted_packet[16..24].try_into().map_err(|_| "invalid timestamp")?);
    let nonce = packet_nonce(stream_id, sequence_number, timestamp_ns);

    let header = &encrypted_packet[0..24];
    let plaintext = cipher.decrypt_with_aad(&nonce, &encrypted_packet[24..], header)?;
    let mut raw_packet = Vec::with_capacity(24 + plaintext.len());
    raw_packet.extend_from_slice(&encrypted_packet[0..24]);
    raw_packet[5] = network_core::protocol::PACKET_TYPE_AUDIO;
    raw_packet.extend_from_slice(&plaintext);
    Ok(raw_packet)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_cipher_round_trip() {
        let key = [0x42u8; 32];
        let nonce = [0x07u8; 12];
        let cipher = StreamCipher::new(&key);

        let pcm_payload = vec![0.1f32, -0.5f32, 0.9f32];
        let raw_bytes: Vec<u8> = pcm_payload.iter().flat_map(|f| f.to_le_bytes()).collect();

        let encrypted = cipher
            .encrypt(&nonce, &raw_bytes)
            .expect("encryption succeeds");
        assert_ne!(encrypted, raw_bytes);

        let decrypted = cipher
            .decrypt(&nonce, &encrypted)
            .expect("decryption succeeds");
        assert_eq!(decrypted, raw_bytes);
    }

    #[test]
    fn test_packet_encryption_and_decryption_round_trip() {
        let cipher = StreamCipher::from_passphrase("StudioSecretKey#2026");
        let header = network_core::protocol::PacketHeader {
            sub_stream_index: 0,
            stream_id: 42,
            sequence_number: 100,
            presentation_timestamp_ns: 1_000_000_000,
        };

        let samples = vec![0.5f32, -0.25f32, 0.75f32, -0.1f32];
        let raw_packet = network_core::protocol::encode_float32_packet(&header, 2, 48000, &samples)
            .expect("encode float32");

        let encrypted = encrypt_audio_packet(&cipher, &raw_packet).expect("encrypt packet");
        assert_eq!(encrypted[5], network_core::protocol::PACKET_TYPE_AUDIO_ENCRYPTED);
        assert_ne!(&encrypted[24..], &raw_packet[24..]);

        let decrypted = decrypt_audio_packet(&cipher, &encrypted).expect("decrypt packet");
        assert_eq!(decrypted[5], network_core::protocol::PACKET_TYPE_AUDIO);
        assert_eq!(decrypted, raw_packet);

        let parsed = network_core::protocol::parse_packet(&decrypted).expect("parse decrypted packet");
        let decoded = network_core::protocol::decode_float32(&decrypted, &parsed).expect("decode float32");
        assert_eq!(decoded, samples);
    }

    #[test]
    fn test_custom_salt_derives_distinct_key() {
        let key1 = derive_key_from_passphrase("StudioPass");
        let key2 = derive_key_from_passphrase_with_salt("StudioPass", b"custom-salt-12345");
        assert_ne!(key1, key2, "Custom salt must produce distinct key for same passphrase");
    }

    #[test]
    fn test_packet_decryption_rejects_wrong_passphrase() {
        let sender_cipher = StreamCipher::from_passphrase("CorrectPassphrase");
        let receiver_cipher = StreamCipher::from_passphrase("WrongPassphrase");

        let header = network_core::protocol::PacketHeader {
            sub_stream_index: 0,
            stream_id: 42,
            sequence_number: 100,
            presentation_timestamp_ns: 1_000_000_000,
        };
        let samples = vec![0.5f32, -0.25f32];
        let raw_packet = network_core::protocol::encode_float32_packet(&header, 2, 48000, &samples).unwrap();

        let encrypted = encrypt_audio_packet(&sender_cipher, &raw_packet).unwrap();
        let result = decrypt_audio_packet(&receiver_cipher, &encrypted);
        assert!(result.is_err(), "Decryption must fail when passphrase does not match");
    }

    #[test]
    fn test_packet_decryption_rejects_corrupted_ciphertext() {
        let cipher = StreamCipher::from_passphrase("StudioKey");
        let header = network_core::protocol::PacketHeader {
            sub_stream_index: 0,
            stream_id: 42,
            sequence_number: 100,
            presentation_timestamp_ns: 1_000_000_000,
        };
        let samples = vec![0.5f32, -0.25f32];
        let raw_packet = network_core::protocol::encode_float32_packet(&header, 2, 48000, &samples).unwrap();

        let mut encrypted = encrypt_audio_packet(&cipher, &raw_packet).unwrap();
        // Tamper with a byte in ciphertext
        if let Some(b) = encrypted.last_mut() {
            *b ^= 0xFF;
        }
        let result = decrypt_audio_packet(&cipher, &encrypted);
        assert!(result.is_err(), "Decryption must reject tampered ciphertext");
    }

    #[test]
    fn test_packet_decryption_rejects_tampered_header() {
        let cipher = StreamCipher::from_passphrase("StudioKey");
        let header = network_core::protocol::PacketHeader {
            sub_stream_index: 0,
            stream_id: 42,
            sequence_number: 100,
            presentation_timestamp_ns: 1_000_000_000,
        };
        let samples = vec![0.5f32, -0.25f32];
        let raw_packet = network_core::protocol::encode_float32_packet(&header, 2, 48000, &samples).unwrap();

        let mut encrypted = encrypt_audio_packet(&cipher, &raw_packet).unwrap();
        // Tamper with sequence number in the header (byte 15)
        encrypted[15] ^= 0x01;
        let result = decrypt_audio_packet(&cipher, &encrypted);
        assert!(result.is_err(), "Decryption must reject header tampering due to AAD authentication");
    }
}
