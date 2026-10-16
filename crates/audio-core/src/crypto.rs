use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use sha2::{Digest, Sha256};

pub struct StreamCipher {
    cipher: Aes256Gcm,
}

impl StreamCipher {
    pub fn new(key: &[u8; 32]) -> Self {
        let cipher = Aes256Gcm::new(key.into());
        Self { cipher }
    }

    /// Derives a 256-bit cryptographic key deterministically from a user-provided passphrase
    /// using SHA-256 with an OpenAudio domain separation salt.
    pub fn from_passphrase(passphrase: &str) -> Self {
        let key = derive_key_from_passphrase(passphrase);
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
}

/// Derives a 32-byte key from a user passphrase using SHA-256 with OpenAudio domain salt.
pub fn derive_key_from_passphrase(passphrase: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(b"OpenAudio-AES-GCM-256-Key-Salt:");
    hasher.update(passphrase.as_bytes());
    let result = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&result);
    key
}

/// Constructs a unique 12-byte (96-bit) nonce per packet for AES-GCM.
/// Uses 4 bytes of stream_id, 4 bytes of sequence_number, and 4 bytes constant domain.
pub fn packet_nonce(stream_id: u32, sequence_number: u32) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[0..4].copy_from_slice(&stream_id.to_be_bytes());
    nonce[4..8].copy_from_slice(&sequence_number.to_be_bytes());
    nonce[8..12].copy_from_slice(b"OAN1");
    nonce
}

/// Encrypts an unencrypted OpenAudio Float32 audio packet into an encrypted OpenAudio packet.
/// Plaintext encompasses the AudioPayloadHeader and PCM samples, leaving the standard 24-byte
/// PacketHeader clear with packet_type = PACKET_TYPE_AUDIO_ENCRYPTED.
pub fn encrypt_audio_packet(cipher: &StreamCipher, raw_packet: &[u8]) -> Result<Vec<u8>, String> {
    if raw_packet.len() < 32 {
        return Err("raw packet too short to encrypt".to_string());
    }
    let stream_id = u32::from_be_bytes(raw_packet[8..12].try_into().map_err(|_| "invalid stream_id")?);
    let sequence_number = u32::from_be_bytes(raw_packet[12..16].try_into().map_err(|_| "invalid seq")?);
    let nonce = packet_nonce(stream_id, sequence_number);

    let ciphertext = cipher.encrypt(&nonce, &raw_packet[24..])?;
    let mut encrypted = Vec::with_capacity(24 + ciphertext.len());
    encrypted.extend_from_slice(&raw_packet[0..24]);
    encrypted[5] = network_core::protocol::PACKET_TYPE_AUDIO_ENCRYPTED;
    encrypted.extend_from_slice(&ciphertext);
    Ok(encrypted)
}

/// Decrypts an encrypted OpenAudio packet back into a standard OpenAudio Float32 audio packet.
/// Returns an Err if authentication fails (wrong passphrase or tampered bytes).
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
    let nonce = packet_nonce(stream_id, sequence_number);

    let plaintext = cipher.decrypt(&nonce, &encrypted_packet[24..])?;
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
}
