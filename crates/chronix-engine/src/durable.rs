//! The files the durable paths write to, behind a trait so a test can make
//! them fail.
//!
//! **A durability path is defined by what it does when the write fails**, and
//! a real filesystem will not fail on request — `ENOSPC` is the failure an
//! embedded deployment on flash storage actually meets. The write-ahead log
//! has had this seam since the pass that found a full disk poisoning it for
//! ever; the catalog, which is the durable record of *what may be deleted*,
//! did not, and its recovery path was argued rather than exercised. It was
//! wrong: a short write left a fragment, the next append put a valid record
//! after it, and replay then stopped at the fragment and **silently dropped
//! every transition that followed** — segments the caller had been told were
//! registered, whose files `open()`'s orphan sweep then deletes.
//!
//! One trait for both, and no runtime surface: a fault is worth injecting
//! only where a verdict is checked, not behind an endpoint on a live
//! database.

use std::fs::File;
use std::io::{self, Write};

/// A file a durable path appends to.
///
/// `Write` plus the three file operations a rewind needs. Boxed behind a
/// `BufWriter` where there is one, so the dynamic call happens once per
/// buffer flush rather than once per record.
pub trait DurableFile: Write + Send + Sync + std::fmt::Debug {
    /// Flush the file's data to stable storage.
    ///
    /// # Errors
    ///
    /// The underlying `fsync` error. A failure here is **not** recoverable by
    /// retrying: on a failed `fsync` the kernel may discard the dirty pages
    /// and clear the error, so a second call can succeed while the data is
    /// gone. The caller poisons itself instead.
    fn sync_data(&self) -> io::Result<()>;

    /// Flush the file's data and metadata to stable storage.
    ///
    /// # Errors
    ///
    /// The underlying `fsync` error.
    fn sync_all(&self) -> io::Result<()>;

    /// Truncate or extend the file to `size` bytes.
    ///
    /// # Errors
    ///
    /// The underlying `ftruncate` error.
    fn set_len(&self, size: u64) -> io::Result<()>;
}

impl DurableFile for File {
    fn sync_data(&self) -> io::Result<()> {
        File::sync_data(self)
    }

    fn sync_all(&self) -> io::Result<()> {
        File::sync_all(self)
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        File::set_len(self, size)
    }
}

/// A file that reports the disk full after a set number of bytes.
///
/// Wraps a real file, so truncation, seeking and replay behave exactly as in
/// production and only the budget is artificial. The budget is shared, so a
/// test can refill it — which is the case that matters: the disk fills, a
/// retention pass frees space, and the next append succeeds *after* the
/// fragment the failed one left.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct FullDiskFile {
    file: File,
    budget: std::sync::Arc<std::sync::atomic::AtomicI64>,
}

#[cfg(test)]
impl FullDiskFile {
    /// Wrap `file`, accepting `budget` more bytes before the disk "fills".
    pub(crate) fn new(file: File, budget: std::sync::Arc<std::sync::atomic::AtomicI64>) -> Self {
        Self { file, budget }
    }

    pub(crate) fn enospc() -> io::Error {
        io::Error::new(io::ErrorKind::StorageFull, "No space left on device")
    }
}

#[cfg(test)]
impl Write for FullDiskFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        use std::sync::atomic::Ordering;
        let remaining = self.budget.load(Ordering::SeqCst);
        if remaining <= 0 {
            return Err(Self::enospc());
        }
        // A short write is what a real filesystem does at the boundary, and
        // it is the case that leaves a partial record behind — so this
        // produces it rather than failing cleanly on the whole buffer.
        let n = buf
            .len()
            .min(usize::try_from(remaining).unwrap_or(usize::MAX));
        let written = self.file.write(&buf[..n])?;
        self.budget
            .fetch_sub(i64::try_from(written).unwrap_or(i64::MAX), Ordering::SeqCst);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
impl std::io::Seek for FullDiskFile {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

#[cfg(test)]
impl DurableFile for FullDiskFile {
    fn sync_data(&self) -> io::Result<()> {
        // A full disk does not stop `fsync` from working on what is already
        // written; keeping this honest is what lets an ENOSPC test isolate
        // the *write* path from the fsync path, which fail differently and
        // for different reasons.
        self.file.sync_data()
    }

    fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn set_len(&self, size: u64) -> io::Result<()> {
        self.file.set_len(size)
    }
}
