#![allow(clippy::unwrap_used, clippy::expect_used)] // examples favour brevity
//! # Encryption at Rest (AES-256-GCM)
//!
//! Demonstrates the pluggable encryption service: key providers,
//! encrypt/decrypt round-trip, and key rotation.
//!
//! ```sh
//! cargo run -p chronix --example encryption
//! ```

use chronix_security::auth::encryption::{EncryptionService, FileKeyProvider};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── 1. Create a key provider with a hex-encoded 256-bit key ─
    println!("─── 1. Setting up FileKeyProvider ───");
    let hex_key = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let provider = FileKeyProvider::from_hex_key(hex_key, "key-v1")?;
    println!(
        "   Key 'key-v1' loaded ({} hex chars = 256 bits)",
        hex_key.len()
    );

    // ── 2. Encrypt some data ───────────────────────────────────
    println!("\n─── 2. Encrypting data ───");
    let service = EncryptionService::new(Box::new(provider));

    let plaintext = b"measurement=cpu,host=web-1 usage=72.5 1609459200000000000";
    println!("   Plaintext : {} bytes", plaintext.len());

    let (ciphertext, key_id) = service.encrypt(plaintext)?;
    println!(
        "   Ciphertext: {} bytes (key_id={key_id})",
        ciphertext.len()
    );
    println!(
        "   Overhead  : {} bytes (nonce + tag)",
        ciphertext.len() - plaintext.len()
    );

    // ── 3. Decrypt and verify ──────────────────────────────────
    println!("\n─── 3. Decrypting ───");
    let decrypted = service.decrypt(&ciphertext, &key_id)?;
    assert_eq!(decrypted, plaintext);
    println!("   Decrypted : {} bytes", decrypted.len());
    println!("   Content   : {}", String::from_utf8_lossy(&decrypted));
    println!("   ✅ Round-trip verified");

    // ── 4. Key rotation ────────────────────────────────────────
    println!("\n─── 4. Key rotation ───");
    let new_hex = "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210";
    let mut provider2 = FileKeyProvider::from_hex_key(hex_key, "key-v1")?;
    provider2.add_key(new_hex, "key-v2")?;

    // New encryptions use the latest key (key-v2)
    let service2 = EncryptionService::new(Box::new(provider2));
    let (ciphertext2, key_id2) = service2.encrypt(plaintext)?;
    println!("   New encryption uses key: {key_id2}");

    // Old ciphertext with key-v1 can still be decrypted
    let decrypted_old = service2.decrypt(&ciphertext, &key_id)?;
    assert_eq!(decrypted_old, plaintext);
    println!("   Old ciphertext (key={key_id}) still decryptable ✅");

    // New ciphertext
    let decrypted_new = service2.decrypt(&ciphertext2, &key_id2)?;
    assert_eq!(decrypted_new, plaintext);
    println!("   New ciphertext (key={key_id2}) decrypts correctly ✅");

    // ── 5. Multiple data blocks ────────────────────────────────
    println!("\n─── 5. Batch encryption ───");
    let payloads: Vec<&[u8]> = vec![
        b"segment_header_v2",
        b"wal_entry_000001",
        b"bloom_filter_data_block",
    ];

    for (i, payload) in payloads.iter().enumerate() {
        let (ct, kid) = service2.encrypt(payload)?;
        let dt = service2.decrypt(&ct, &kid)?;
        assert_eq!(&dt, payload);
        println!(
            "   [{i}] {} bytes → {} bytes (key={kid}) ✅",
            payload.len(),
            ct.len()
        );
    }

    println!("\n✅ Done");
    Ok(())
}
