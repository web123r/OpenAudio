use audio_core::crypto::{
    decrypt_audio_packet, encrypt_audio_packet, StreamCipher,
};
use audio_core::opus::{OpusChannelDecoder, OpusChannelEncoder, OPUS_FRAME_SAMPLES};
use network_core::protocol::{
    decode_float32, encode_float32_packet, encode_opus_packet, opus_payload, parse_packet,
    PacketHeader, SAMPLE_FORMAT_FLOAT32, SAMPLE_FORMAT_OPUS,
};
use network_core::receiver::{ReceiveEvent, StreamReceiver};
use std::net::UdpSocket;
use std::time::Duration;

#[test]
fn test_end_to_end_pcm_loopback() {
    let stream_id = 101;
    let receive_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
    let receive_port = receive_socket.local_addr().unwrap().port();
    drop(receive_socket);

    let mut receiver = StreamReceiver::new("127.0.0.1", 18050, stream_id, receive_port).unwrap();
    let sender = UdpSocket::bind("127.0.0.1:0").unwrap();

    let sample_rate = 48000;
    let channels = 2;
    let samples_per_channel = 64;
    let expected_samples: Vec<f32> = (0..(samples_per_channel * channels))
        .map(|i| ((i as f32) * 0.05).sin())
        .collect();

    let header = PacketHeader {
        sub_stream_index: 0,
        stream_id,
        sequence_number: 1,
        presentation_timestamp_ns: 1_000_000,
    };

    let packet = encode_float32_packet(&header, channels as u8, sample_rate, &expected_samples)
        .expect("encode float32 packet");

    sender
        .send_to(&packet, format!("127.0.0.1:{receive_port}"))
        .expect("send UDP packet");

    let mut received_pcm = None;
    for _ in 0..20 {
        match receiver.next_event() {
            Ok(Some(ReceiveEvent::Packet(pkt, pcm))) if pkt.stream_id == stream_id => {
                received_pcm = Some(pcm);
                break;
            }
            _ => std::thread::sleep(Duration::from_millis(10)),
        }
    }

    let pcm = received_pcm.expect("must receive pcm packet");
    assert_eq!(pcm.len(), expected_samples.len());
    for (orig, rec) in expected_samples.iter().zip(pcm.iter()) {
        assert!((orig - rec).abs() < 1e-6, "PCM sample mismatch: {orig} vs {rec}");
    }
}

#[test]
fn test_end_to_end_encrypted_stream_loopback() {
    let passphrase = "SecureBroadcastKey2026!";
    let sender_cipher = StreamCipher::from_passphrase(passphrase);
    let receiver_cipher = StreamCipher::from_passphrase(passphrase);

    let stream_id = 202;
    let header = PacketHeader {
        sub_stream_index: 0,
        stream_id,
        sequence_number: 55,
        presentation_timestamp_ns: 2_500_000,
    };
    let test_samples = vec![0.125f32, -0.75f32, 0.5f32, -0.25f32];
    let raw_packet = encode_float32_packet(&header, 2, 48000, &test_samples).unwrap();

    let encrypted_packet = encrypt_audio_packet(&sender_cipher, &raw_packet).unwrap();
    assert_ne!(encrypted_packet, raw_packet);

    let decrypted_packet = decrypt_audio_packet(&receiver_cipher, &encrypted_packet)
        .expect("decryption must succeed with matching key");
    assert_eq!(decrypted_packet, raw_packet);

    let parsed = parse_packet(&decrypted_packet).expect("decrypted packet must parse");
    assert_eq!(parsed.stream_id, stream_id);
    assert_eq!(parsed.sample_format, SAMPLE_FORMAT_FLOAT32);
    let pcm = decode_float32(&decrypted_packet, &parsed).expect("must decode pcm");
    assert_eq!(pcm, test_samples);
}

#[test]
fn test_end_to_end_opus_loopback_streaming() {
    let sample_rate = 48000;
    let channels = 2;
    let bitrate_kbps = 128;

    let mut encoder = OpusChannelEncoder::new(sample_rate, channels, bitrate_kbps).unwrap();
    let mut decoder = OpusChannelDecoder::new(sample_rate, channels).unwrap();

    // Generate 10ms of stereo 440Hz sine wave (480 stereo frames = 960 floats)
    let pcm_input: Vec<f32> = (0..OPUS_FRAME_SAMPLES * channels)
        .map(|i| {
            let frame = i / channels;
            (2.0 * std::f32::consts::PI * 440.0 * frame as f32 / sample_rate as f32).sin() * 0.5
        })
        .collect();

    let opus_encoded = encoder
        .push_samples(&pcm_input)
        .unwrap()
        .expect("must produce opus packet");

    let stream_id = 303;
    let header = PacketHeader {
        sub_stream_index: 0,
        stream_id,
        sequence_number: 1,
        presentation_timestamp_ns: 5_000_000,
    };

    let packet = encode_opus_packet(
        &header,
        channels as u8,
        sample_rate,
        OPUS_FRAME_SAMPLES as u16,
        &opus_encoded,
    )
    .unwrap();

    let parsed = parse_packet(&packet).expect("must parse opus packet");
    assert_eq!(parsed.sample_format, SAMPLE_FORMAT_OPUS);
    assert_eq!(parsed.channel_count, channels as u8);

    let opus_raw = opus_payload(&packet, &parsed).expect("must extract opus payload");
    let recovered_pcm = decoder
        .decode_packet(opus_raw)
        .expect("must decode opus packet");

    assert_eq!(recovered_pcm.len(), pcm_input.len());
}
