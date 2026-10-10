use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};

pub struct StreamCipher {
    cipher: Aes256Gcm,
}

impl StreamCipher {
    pub fn new(key: &[u8; 32]) -> Self {
        let cipher = Aes256Gcm::new(key.into());
        Self { cipher }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_cipher_round_trip() {
        let key = [0x42u8; 32];
        let nonce = [0x07u8; 12];
        let cipher = StreamCipher::new(&key);

        let pcm_payload = vec![0.1f32, -0.5f32, 0.9f32];
        let raw_bytes: Vec<u8> = pcm_payload
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect();

        let encrypted = cipher.encrypt(&nonce, &raw_bytes).expect("encryption succeeds");
        assert_ne!(encrypted, raw_bytes);

        let decrypted = cipher.decrypt(&nonce, &encrypted).expect("decryption succeeds");
        assert_eq!(decrypted, raw_bytes);
    }
}
