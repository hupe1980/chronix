//! Block compression for encoded column data (LZ4 and Zstd).
//!
//! Each encoded column block can be optionally compressed after
//! type-specific encoding. The compressed format prepends a codec tag
//! and size header to enable deterministic decompression.
//!
//! ## Compressed block format (v2)
//!
//! ```text
//! [codec_tag: u8]              — 0x00=None, 0x01=Lz4, 0x02=Zstd
//! [compressed_size: u32]
//! [uncompressed_size: u32]
//! [compressed_data: compressed_size bytes]
//! ```
//!
//! When `compressed_size == 0`, the block is stored uncompressed.
//! The codec tag enables deterministic dispatch — no trial decompression.

use chronix_core::config::CompressionCodec;

use crate::segment::error::{Result, SegmentError};

/// Maximum block size that fits in a `u32` length header.
const MAX_BLOCK_SIZE: usize = u32::MAX as usize;

/// Minimum compression ratio for auto-skip.
///
/// If type-specific encoding already achieved this ratio, LZ4 is skipped.
const AUTO_SKIP_RATIO: f64 = 8.0;

/// Size of the compression block header in bytes.
///
/// Layout: `[codec_tag: u8][compressed_size: u32][uncompressed_size: u32]`.
const COMPRESSION_HEADER_SIZE: usize = 9;

/// Codec tag: data stored without compression.
const CODEC_TAG_NONE: u8 = 0x00;
/// Codec tag: LZ4 block compression.
const CODEC_TAG_LZ4: u8 = 0x01;
/// Codec tag: Zstd compression.
const CODEC_TAG_ZSTD: u8 = 0x02;
/// Codec tag: Zstd compression with trained dictionary.
const CODEC_TAG_ZSTD_DICT: u8 = 0x03;

/// Map a [`CompressionCodec`] to its wire tag.
const fn codec_to_tag(codec: CompressionCodec) -> u8 {
    match codec {
        CompressionCodec::None => CODEC_TAG_NONE,
        CompressionCodec::Lz4 => CODEC_TAG_LZ4,
        CompressionCodec::Zstd => CODEC_TAG_ZSTD,
    }
}

/// Compress a block using LZ4.
///
/// Returns the compressed data with the 8-byte header, or the original data
/// if compression didn't achieve meaningful savings.
///
/// # Errors
///
/// Returns an error if the data exceeds `u32::MAX` bytes.
pub fn compress_block(data: &[u8]) -> Result<Vec<u8>> {
    compress_block_with_codec(data, CompressionCodec::Lz4)
}

/// Compress a block using the specified compression codec.
///
/// # Errors
///
/// Returns an error if the data exceeds `u32::MAX` bytes or compression fails.
pub fn compress_block_with_codec(data: &[u8], codec: CompressionCodec) -> Result<Vec<u8>> {
    if data.len() > MAX_BLOCK_SIZE {
        return Err(SegmentError::CorruptFile {
            detail: format!(
                "block too large for compression header: {} bytes (max {})",
                data.len(),
                MAX_BLOCK_SIZE
            ),
        });
    }

    if data.is_empty() {
        let mut buf = Vec::with_capacity(COMPRESSION_HEADER_SIZE);
        buf.push(codec_to_tag(codec));
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        return Ok(buf);
    }

    if matches!(codec, CompressionCodec::None) {
        return Ok(write_uncompressed(data));
    }

    let compressed = match codec {
        CompressionCodec::Lz4 => compress_lz4(data),
        CompressionCodec::Zstd => compress_zstd(data, DEFAULT_ZSTD_LEVEL),
        CompressionCodec::None => return Ok(write_uncompressed(data)),
    };

    // If compression didn't help, store uncompressed
    if compressed.is_empty() || compressed.len() >= data.len() {
        return Ok(write_uncompressed(data));
    }

    let mut buf = Vec::with_capacity(COMPRESSION_HEADER_SIZE + compressed.len());
    buf.push(codec_to_tag(codec));
    buf.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(&compressed);
    Ok(buf)
}

/// Default Zstd compression level (balanced speed/ratio).
const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// Maximum Zstd compression level for cold-tier recompression.
pub const ZSTD_HIGH_LEVEL: i32 = 9;

/// Compress a block using Zstd at a specific level.
///
/// # Errors
///
/// Returns an error if compression fails.
pub fn compress_block_zstd(data: &[u8], level: i32) -> Result<Vec<u8>> {
    if data.len() > MAX_BLOCK_SIZE {
        return Err(SegmentError::CorruptFile {
            detail: format!(
                "block too large for compression header: {} bytes (max {})",
                data.len(),
                MAX_BLOCK_SIZE
            ),
        });
    }

    if data.is_empty() {
        let mut buf = Vec::with_capacity(COMPRESSION_HEADER_SIZE);
        buf.push(CODEC_TAG_ZSTD);
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        return Ok(buf);
    }

    let compressed = compress_zstd(data, level);
    if compressed.is_empty() || compressed.len() >= data.len() {
        return Ok(write_uncompressed(data));
    }

    let mut buf = Vec::with_capacity(COMPRESSION_HEADER_SIZE + compressed.len());
    buf.push(CODEC_TAG_ZSTD);
    buf.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(&compressed);
    Ok(buf)
}

fn write_uncompressed(data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(COMPRESSION_HEADER_SIZE + data.len());
    buf.push(CODEC_TAG_NONE);
    buf.extend_from_slice(&0u32.to_le_bytes()); // compressed_size = 0 → uncompressed
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(data);
    buf
}

fn compress_lz4(data: &[u8]) -> Vec<u8> {
    let max_compressed = lz4_flex::block::get_maximum_output_size(data.len());
    let mut compressed = vec![0u8; max_compressed];
    match lz4_flex::compress_into(data, &mut compressed) {
        Ok(len) if len > 0 => {
            compressed.truncate(len);
            compressed
        }
        Ok(_) => {
            tracing::warn!(
                len = data.len(),
                "lz4 compression produced empty output, falling back to uncompressed"
            );
            Vec::new()
        }
        Err(e) => {
            tracing::warn!(error = %e, len = data.len(), "lz4 compression failed, falling back to uncompressed");
            Vec::new()
        }
    }
}

fn compress_zstd(data: &[u8], level: i32) -> Vec<u8> {
    match zstd::bulk::compress(data, level) {
        Ok(compressed) => compressed,
        Err(e) => {
            tracing::warn!(error = %e, len = data.len(), level, "zstd compression failed, falling back to uncompressed");
            Vec::new()
        }
    }
}

// ── Zstd dictionary training and compression ─────────────────

/// Minimum number of samples and combined byte size before dictionary
/// training is worthwhile. Below these thresholds the dictionary would
/// over-fit to noise.
const MIN_DICT_SAMPLES: usize = 8;
const MIN_DICT_SAMPLE_BYTES: usize = 16_384;

/// Default dictionary size produced by `zstd::dict::from_samples`.
const DICT_MAX_SIZE: usize = 16 * 1024;

/// Train a Zstd compression dictionary from a set of sample buffers.
///
/// Returns `None` if the input is too small to produce a useful dictionary.
///
/// # Errors
///
/// Returns an error only when dictionary training itself fails (OOM, etc.).
pub fn train_zstd_dictionary(samples: &[&[u8]]) -> Result<Option<Vec<u8>>> {
    if samples.len() < MIN_DICT_SAMPLES {
        return Ok(None);
    }
    let total: usize = samples.iter().map(|s| s.len()).sum();
    if total < MIN_DICT_SAMPLE_BYTES {
        return Ok(None);
    }

    let owned: Vec<Vec<u8>> = samples.iter().map(|s| s.to_vec()).collect();
    match zstd::dict::from_samples(&owned, DICT_MAX_SIZE) {
        Ok(dict) if !dict.is_empty() => Ok(Some(dict)),
        Ok(_) => Ok(None),
        Err(e) => {
            tracing::warn!(error = %e, samples = samples.len(), "zstd dictionary training failed");
            Ok(None)
        }
    }
}

/// Compress a block with a trained Zstd dictionary.
///
/// The output uses `CODEC_TAG_ZSTD_DICT` so the reader can route to
/// [`decompress_block_zstd_dict`] with the segment's stored dictionary.
///
/// # Errors
///
/// Returns an error if the block exceeds `MAX_BLOCK_SIZE`.
pub fn compress_block_zstd_dict(data: &[u8], level: i32, dict: &[u8]) -> Result<Vec<u8>> {
    if data.len() > MAX_BLOCK_SIZE {
        return Err(SegmentError::CorruptFile {
            detail: format!(
                "block too large for compression header: {} bytes (max {})",
                data.len(),
                MAX_BLOCK_SIZE
            ),
        });
    }

    if data.is_empty() {
        let mut buf = Vec::with_capacity(COMPRESSION_HEADER_SIZE);
        buf.push(CODEC_TAG_ZSTD_DICT);
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        return Ok(buf);
    }

    let compressed = {
        let cdict = zstd::dict::EncoderDictionary::copy(dict, level);
        let mut out = Vec::new();
        match zstd::Encoder::with_prepared_dictionary(&mut out, &cdict) {
            Ok(mut encoder) => {
                use std::io::Write;
                if encoder.write_all(data).is_err() || encoder.finish().is_err() {
                    tracing::warn!(
                        len = data.len(),
                        "zstd dict compression failed, falling back to uncompressed"
                    );
                    Vec::new()
                } else {
                    out
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "zstd dict encoder creation failed, falling back to uncompressed");
                Vec::new()
            }
        }
    };

    if compressed.is_empty() || compressed.len() >= data.len() {
        return Ok(write_uncompressed(data));
    }

    let mut buf = Vec::with_capacity(COMPRESSION_HEADER_SIZE + compressed.len());
    buf.push(CODEC_TAG_ZSTD_DICT);
    buf.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
    buf.extend_from_slice(&compressed);
    Ok(buf)
}

/// Decompress a block that was compressed with a Zstd dictionary.
///
/// # Errors
///
/// Returns an error if dictionary decompression fails or sizes mismatch.
pub fn decompress_block_zstd_dict(
    payload: &[u8],
    uncompressed_size: usize,
    dict: &[u8],
) -> Result<Vec<u8>> {
    use std::io::Read;
    let ddict = zstd::dict::DecoderDictionary::copy(dict);
    let mut decoder = zstd::Decoder::with_prepared_dictionary(payload, &ddict).map_err(|e| {
        SegmentError::CorruptFile {
            detail: format!("zstd dict decoder creation failed: {e}"),
        }
    })?;
    let mut out = Vec::with_capacity(uncompressed_size);
    decoder
        .read_to_end(&mut out)
        .map_err(|e| SegmentError::CorruptFile {
            detail: format!("zstd dict decompression failed: {e}"),
        })?;
    if out.len() != uncompressed_size {
        return Err(SegmentError::CorruptFile {
            detail: format!(
                "zstd dict decompressed size mismatch: expected {uncompressed_size}, got {}",
                out.len()
            ),
        });
    }
    Ok(out)
}

/// Maximum allowed uncompressed block size (256 MiB).
///
/// Guards against adversarial sizes in corrupt segment files that could
/// cause multi-gigabyte allocations.
const MAX_UNCOMPRESSED_SIZE: usize = 256 * 1024 * 1024;

/// Decompress a block that was compressed with [`compress_block`] or
/// [`compress_block_with_codec`].
///
/// Reads the codec tag from the header to deterministically select
/// the decompression algorithm — no trial decompression needed.
///
/// For blocks compressed with a Zstd dictionary (`CODEC_TAG_ZSTD_DICT`),
/// use [`decompress_block_with_optional_dict`] instead.
///
/// # Errors
///
/// Returns an error if the data is malformed or decompression fails.
pub fn decompress_block(data: &[u8]) -> Result<Vec<u8>> {
    decompress_block_with_optional_dict(data, None)
}

/// Decompress a block, optionally using a Zstd dictionary for
/// `CODEC_TAG_ZSTD_DICT` blocks.
///
/// # Errors
///
/// Returns an error if the data is malformed, decompression fails,
/// or a dictionary-compressed block is encountered without a dictionary.
pub fn decompress_block_with_optional_dict(data: &[u8], dict: Option<&[u8]>) -> Result<Vec<u8>> {
    if data.len() < COMPRESSION_HEADER_SIZE {
        return Err(crate::segment::error::SegmentError::CorruptFile {
            detail: "compressed block too short for header".to_string(),
        });
    }

    let codec_tag = data[0];
    let compressed_size = u32::from_le_bytes([data[1], data[2], data[3], data[4]]) as usize;
    let uncompressed_size = u32::from_le_bytes([data[5], data[6], data[7], data[8]]) as usize;

    if uncompressed_size > MAX_UNCOMPRESSED_SIZE {
        return Err(crate::segment::error::SegmentError::CorruptFile {
            detail: format!(
                "uncompressed size {uncompressed_size} exceeds maximum ({MAX_UNCOMPRESSED_SIZE})"
            ),
        });
    }

    if compressed_size == 0 {
        // Data was stored uncompressed
        if data.len() < COMPRESSION_HEADER_SIZE + uncompressed_size {
            return Err(crate::segment::error::SegmentError::CorruptFile {
                detail: "uncompressed block data truncated".to_string(),
            });
        }
        return Ok(
            data[COMPRESSION_HEADER_SIZE..COMPRESSION_HEADER_SIZE + uncompressed_size].to_vec(),
        );
    }

    if data.len() < COMPRESSION_HEADER_SIZE + compressed_size {
        return Err(crate::segment::error::SegmentError::CorruptFile {
            detail: "compressed block data truncated".to_string(),
        });
    }

    let payload = &data[COMPRESSION_HEADER_SIZE..COMPRESSION_HEADER_SIZE + compressed_size];

    let decompressed = match codec_tag {
        CODEC_TAG_LZ4 => lz4_flex::decompress(payload, uncompressed_size).map_err(|e| {
            SegmentError::CorruptFile {
                detail: format!("LZ4 decompression failed: {e}"),
            }
        })?,
        CODEC_TAG_ZSTD => zstd::bulk::decompress(payload, uncompressed_size).map_err(|e| {
            SegmentError::CorruptFile {
                detail: format!("Zstd decompression failed: {e}"),
            }
        })?,
        CODEC_TAG_ZSTD_DICT => {
            // Dictionary-compressed block
            let d = dict.ok_or_else(|| SegmentError::CorruptFile {
                detail: "ZSTD_DICT block but no dictionary provided".to_string(),
            })?;
            decompress_block_zstd_dict(payload, uncompressed_size, d)?
        }
        unknown => {
            return Err(SegmentError::CorruptFile {
                detail: format!("unknown compression codec tag: 0x{unknown:02x}"),
            });
        }
    };

    if decompressed.len() != uncompressed_size {
        return Err(SegmentError::CorruptFile {
            detail: format!(
                "decompressed size mismatch: expected {uncompressed_size}, got {}",
                decompressed.len()
            ),
        });
    }

    Ok(decompressed)
}

/// Returns `true` if the type-specific encoding already achieved enough
/// compression that LZ4 should be skipped.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn should_skip_compression(encoded_size: usize, raw_size: usize) -> bool {
    if raw_size == 0 {
        return true;
    }
    let ratio = raw_size as f64 / encoded_size as f64;
    ratio >= AUTO_SKIP_RATIO
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_compressible_data() {
        let data = vec![42u8; 10_000];
        let compressed = compress_block(&data).unwrap();
        assert!(compressed.len() < data.len());
        let decompressed = decompress_block(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn roundtrip_incompressible_data() {
        // Random-looking data that doesn't compress well
        let data: Vec<u8> = (0..1000).map(|i| (i * 37 + 13) as u8).collect();
        let compressed = compress_block(&data).unwrap();
        let decompressed = decompress_block(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn roundtrip_small_data() {
        let data = vec![1, 2, 3];
        let compressed = compress_block(&data).unwrap();
        let decompressed = decompress_block(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn roundtrip_empty() {
        let data: Vec<u8> = vec![];
        let compressed = compress_block(&data).unwrap();
        let decompressed = decompress_block(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn should_skip_compression_logic() {
        // 1000 raw, 100 encoded → 10× → skip
        assert!(should_skip_compression(100, 1000));
        // 1000 raw, 500 encoded → 2× → don't skip
        assert!(!should_skip_compression(500, 1000));
        // 0 raw → skip
        assert!(should_skip_compression(100, 0));
    }

    #[test]
    fn roundtrip_zstd_compressible() {
        let data = vec![42u8; 10_000];
        let compressed = compress_block_with_codec(&data, CompressionCodec::Zstd).unwrap();
        assert!(compressed.len() < data.len());
        let decompressed = decompress_block(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn roundtrip_zstd_level_9() {
        let data = vec![42u8; 10_000];
        let compressed = compress_block_zstd(&data, 9).unwrap();
        let decompressed = decompress_block(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn roundtrip_zstd_incompressible() {
        let data: Vec<u8> = (0..1000).map(|i| (i * 37 + 13) as u8).collect();
        let compressed = compress_block_with_codec(&data, CompressionCodec::Zstd).unwrap();
        let decompressed = decompress_block(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn roundtrip_no_compression_codec() {
        let data = vec![42u8; 100];
        let compressed = compress_block_with_codec(&data, CompressionCodec::None).unwrap();
        // Should store uncompressed
        assert_eq!(compressed.len(), 9 + data.len());
        let decompressed = decompress_block(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn zstd_better_ratio_than_lz4() {
        // Regular repeating data — Zstd should compress better
        let data: Vec<u8> = (0..50_000).map(|i| (i % 256) as u8).collect();
        let lz4 = compress_block_with_codec(&data, CompressionCodec::Lz4).unwrap();
        let zstd = compress_block_with_codec(&data, CompressionCodec::Zstd).unwrap();
        assert!(
            zstd.len() <= lz4.len(),
            "Zstd {} should be <= LZ4 {}",
            zstd.len(),
            lz4.len()
        );
    }

    #[test]
    fn corrupt_data_detected() {
        // Less than 9-byte header → error
        assert!(decompress_block(&[0; 3]).is_err());
        assert!(decompress_block(&[0; 8]).is_err());
    }

    #[test]
    fn unknown_codec_tag_rejected() {
        // Valid header size but unknown codec tag 0xFF
        let mut data = vec![0xFF, 0, 0, 0, 1, 5, 0, 0, 0];
        data.extend_from_slice(&[1, 2, 3, 4, 5]); // fake payload
        assert!(decompress_block(&data).is_err());
    }

    #[test]
    fn p09_train_dict_too_few_samples() {
        let samples: Vec<&[u8]> = vec![b"hello"; 3]; // below MIN_DICT_SAMPLES
        let result = train_zstd_dictionary(&samples).unwrap();
        assert!(result.is_none(), "should return None for too few samples");
    }

    #[test]
    fn p09_zstd_dict_compress_decompress_roundtrip() {
        // Generate enough samples for dictionary training (need >= MIN_DICT_SAMPLE_BYTES total)
        let sample_data: Vec<Vec<u8>> = (0..300)
            .map(|i| {
                format!(
                    "host-{i},cpu=idle,region=us-east-1,dc=dc-{},rack=rack-{},value={}",
                    i % 4,
                    i % 16,
                    i * 42
                )
                .into_bytes()
            })
            .collect();
        let sample_refs: Vec<&[u8]> = sample_data.iter().map(std::vec::Vec::as_slice).collect();

        let dict = train_zstd_dictionary(&sample_refs)
            .unwrap()
            .expect("should produce a dictionary");
        assert!(!dict.is_empty());

        // Compress a block with the dictionary
        let payload = b"host-99,cpu=idle,region=us-east-1,dc=dc-3,rack=rack-15,value=4242";
        let compressed = compress_block_zstd_dict(payload, 3, &dict).unwrap();

        // Decompress with dictionary
        let decompressed = decompress_block_with_optional_dict(&compressed, Some(&dict)).unwrap();
        assert_eq!(decompressed, payload.as_slice());
    }
}
