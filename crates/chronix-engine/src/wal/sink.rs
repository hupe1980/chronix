//! The file the WAL writes to, behind a trait so a test can make it fail.
//!
//! A durability path is defined by what it does when the write **fails**, and
//! a real filesystem will not fail on request — `ENOSPC` is the failure an
//! embedded deployment on flash storage actually meets. One trait, one test
//! implementation, and no runtime surface: a fault is worth injecting only
//! where a verdict is checked, not behind an endpoint on a live database.

use std::fs::File;
use std::io::{self, Seek, Write};

/// A WAL data file.
///
/// `Write + Seek` plus the three file operations the writer needs. Boxed
/// behind the 64 KiB `BufWriter`, so the dynamic call happens once per buffer
/// flush rather than once per record.
pub trait WalSink: Write + Seek + Send {
    /// Flush the file's data to stable storage.
    ///
    /// # Errors
    ///
    /// The underlying `fsync` error. A failure here is **not** recoverable by
    /// retrying: on a failed `fsync` the kernel may discard the dirty pages
    /// and clear the error, so a second call can succeed while the data is
    /// gone. The writer poisons itself instead.
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

impl WalSink for File {
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

/// A sink that reports the disk full after a set number of bytes.
///
/// Wraps a real file, so truncation, seeking and replay behave exactly as in
/// production and only the budget is artificial.
#[cfg(test)]
pub(crate) struct FullDiskSink {
    file: File,
    /// Bytes still accepted before every write returns `ENOSPC`.
    budget: std::sync::Arc<std::sync::atomic::AtomicI64>,
}

#[cfg(test)]
impl FullDiskSink {
    /// Wrap `file`, accepting `budget` more bytes before the disk "fills".
    pub(crate) fn new(file: File, budget: std::sync::Arc<std::sync::atomic::AtomicI64>) -> Self {
        Self { file, budget }
    }

    fn enospc() -> io::Error {
        io::Error::new(io::ErrorKind::StorageFull, "No space left on device")
    }
}

#[cfg(test)]
impl Write for FullDiskSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        use std::sync::atomic::Ordering;
        let remaining = self.budget.load(Ordering::SeqCst);
        if remaining <= 0 {
            return Err(Self::enospc());
        }
        // A short write is what a real filesystem does at the boundary, and
        // it is the case that leaves a partial record behind — so the sink
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
impl Seek for FullDiskSink {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        self.file.seek(pos)
    }
}

#[cfg(test)]
impl WalSink for FullDiskSink {
    fn sync_data(&self) -> io::Result<()> {
        // A full disk does not stop `fsync` from working on what is already
        // written; keeping this honest is what lets the ENOSPC test isolate
        // the *write* path from the fsync path, which poison differently and
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
