//! The file the WAL writes to.
//!
//! [`WalSink`] is [`DurableFile`](crate::durable::DurableFile) plus `Seek`:
//! the WAL rewinds to a durable offset and the catalog's manifest only
//! appends, so the seekable half is the WAL's alone. Everything else — the
//! three file operations a rewind needs, and the test file that reports the
//! disk full — is shared, because two copies of a fault-injection seam is two
//! places for the *fault* to be modelled differently.

use std::io::Seek;

pub use crate::durable::DurableFile;

/// A WAL data file: a durable file that can also seek.
///
/// Boxed behind the 64 KiB `BufWriter`, so the dynamic call happens once per
/// buffer flush rather than once per record.
pub trait WalSink: DurableFile + Seek {}

impl<T: DurableFile + Seek + ?Sized> WalSink for T {}

#[cfg(test)]
pub(crate) use crate::durable::FullDiskFile as FullDiskSink;
