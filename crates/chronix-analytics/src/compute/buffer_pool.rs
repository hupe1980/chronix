//! Reusable buffer pool to minimize allocation in hot compute paths.

use parking_lot::Mutex;

/// Thread-safe pool of reusable `Vec<T>` buffers.
///
/// Buffers are cleared (but not deallocated) on return, preserving capacity.
///
/// # Capacity model
///
/// The pool enforces both a **count-based** limit (`max_size`) and a
/// **byte-capacity** limit (`max_pool_bytes`).  A buffer is only cached
/// when *both* limits allow it, preventing a small number of
/// oversized buffers from consuming unbounded memory.
///
/// # Examples
///
/// ```no_run
/// use chronix_analytics::compute::BufferPool;
///
/// let pool = BufferPool::<f64>::new(4);
/// let mut buf = pool.get(100);
/// buf.extend_from_slice(&[1.0, 2.0, 3.0]);
/// assert_eq!(buf.len(), 3);
/// pool.put(buf);
/// // Next get reuses the returned buffer's capacity
/// let buf2 = pool.get(3);
/// assert!(buf2.capacity() >= 100);
/// ```
/// Default byte-capacity budget: 64 MiB.
const DEFAULT_MAX_POOL_BYTES: usize = 64 * 1024 * 1024;

/// A pool of reusable `Vec<T>` buffers that avoids repeated allocation on
/// hot compute paths: `get()` hands out a cleared buffer, `put()` returns it.
pub struct BufferPool<T> {
    pool: Mutex<Vec<Vec<T>>>,
    max_size: usize,
    max_pool_bytes: usize,
    current_pool_bytes: Mutex<usize>,
}

impl<T> BufferPool<T> {
    /// Creates a new buffer pool with the given maximum number of cached
    /// buffers and a default byte-capacity budget of 64 MiB.
    pub fn new(max_size: usize) -> Self {
        Self {
            pool: Mutex::new(Vec::with_capacity(max_size)),
            max_size,
            max_pool_bytes: DEFAULT_MAX_POOL_BYTES,
            current_pool_bytes: Mutex::new(0),
        }
    }

    /// Creates a buffer pool with an explicit byte-capacity budget.
    pub fn with_max_bytes(max_size: usize, max_pool_bytes: usize) -> Self {
        Self {
            pool: Mutex::new(Vec::with_capacity(max_size)),
            max_size,
            max_pool_bytes,
            current_pool_bytes: Mutex::new(0),
        }
    }

    /// Returns the total byte capacity currently held by pooled buffers.
    pub fn pooled_bytes(&self) -> usize {
        *self.current_pool_bytes.lock()
    }

    /// Gets a buffer from the pool or allocates a new one.
    ///
    /// The returned buffer is empty but may have pre-allocated capacity from
    /// a previous use. If `min_capacity` is specified, the buffer is guaranteed
    /// to have at least that capacity.
    pub fn get(&self, min_capacity: usize) -> Vec<T> {
        let mut pool = self.pool.lock();
        if let Some(mut buf) = pool.pop() {
            let buf_bytes = std::mem::size_of::<T>() * buf.capacity();
            *self.current_pool_bytes.lock() -= buf_bytes;
            if buf.capacity() < min_capacity {
                buf.reserve(min_capacity - buf.len());
            }
            buf
        } else {
            Vec::with_capacity(min_capacity)
        }
    }

    /// Returns a buffer to the pool for reuse.
    ///
    /// The buffer is cleared but its capacity is preserved.
    pub fn put(&self, mut buf: Vec<T>) {
        buf.clear();
        let buf_bytes = std::mem::size_of::<T>() * buf.capacity();
        let mut pool = self.pool.lock();
        let mut bytes = self.current_pool_bytes.lock();
        if pool.len() < self.max_size && *bytes + buf_bytes <= self.max_pool_bytes {
            *bytes += buf_bytes;
            pool.push(buf);
        }
        // else: drop the buffer (pool full or byte budget exceeded)
    }

    /// Returns the number of buffers currently in the pool.
    pub fn available(&self) -> usize {
        self.pool.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_and_put() {
        let pool = BufferPool::<f64>::new(2);
        let mut buf = pool.get(64);
        buf.extend_from_slice(&[1.0, 2.0, 3.0]);
        assert_eq!(buf.len(), 3);
        let cap = buf.capacity();

        pool.put(buf);
        assert_eq!(pool.available(), 1);

        let buf2 = pool.get(1);
        assert!(buf2.capacity() >= cap);
        assert!(buf2.is_empty());
        assert_eq!(pool.available(), 0);
    }

    #[test]
    fn pool_max_size_respected() {
        let pool = BufferPool::<f64>::new(1);
        pool.put(vec![1.0, 2.0]);
        pool.put(vec![3.0, 4.0]); // exceeds max — dropped
        assert_eq!(pool.available(), 1);
    }

    #[test]
    fn min_capacity_honored() {
        let pool = BufferPool::<f64>::new(2);
        pool.put(Vec::with_capacity(4));
        let buf = pool.get(100);
        assert!(buf.capacity() >= 100);
    }

    #[test]
    fn byte_budget_rejects_oversized_buffer() {
        // Allow at most 64 bytes in the pool (8 f64 elements).
        let pool = BufferPool::<f64>::with_max_bytes(4, 64);
        // A buffer with capacity 100 f64 = 800 bytes — exceeds the budget.
        pool.put(Vec::<f64>::with_capacity(100));
        assert_eq!(pool.available(), 0, "oversized buffer should be dropped");
        assert_eq!(pool.pooled_bytes(), 0);
    }

    #[test]
    fn byte_budget_accepts_small_buffer() {
        let pool = BufferPool::<f64>::with_max_bytes(4, 1024);
        pool.put(Vec::<f64>::with_capacity(8)); // 64 bytes
        assert_eq!(pool.available(), 1);
        assert_eq!(pool.pooled_bytes(), 64);
    }

    #[test]
    fn byte_budget_decremented_on_get() {
        let pool = BufferPool::<f64>::with_max_bytes(4, 1024);
        pool.put(Vec::<f64>::with_capacity(8));
        assert_eq!(pool.pooled_bytes(), 64);

        let _buf = pool.get(1);
        assert_eq!(pool.pooled_bytes(), 0);
    }
}
