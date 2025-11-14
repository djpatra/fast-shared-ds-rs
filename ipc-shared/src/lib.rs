use memmap2::{MmapMut, MmapOptions};
use std::cell::UnsafeCell;
use std::fs::OpenOptions;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[repr(C)]
pub struct RingIndices {
    write_index: AtomicUsize,
    read_index: AtomicUsize,
}

pub struct ShmRegion<const BUFFER_SIZE: usize> {
    pub mmap: UnsafeCell<MmapMut>,
}

pub struct RingBuffer<const N: usize> {
    shm: Arc<ShmRegion<N>>,
}

unsafe impl<const BUFFER_SIZE: usize> Sync for ShmRegion<BUFFER_SIZE> {}

impl<const BUFFER_SIZE: usize> ShmRegion<BUFFER_SIZE> {
    const INDEX_REGION_SIZE: usize = std::mem::size_of::<RingIndices>();
    const SHM_TOTAL_SIZE: usize = BUFFER_SIZE + Self::INDEX_REGION_SIZE;

    pub fn new(name: &str) -> std::io::Result<Self> {
        let mut path = std::env::temp_dir();
        path.push(name);

        // Try to create the file atomically. If it already exists, open it.
        // When we create it, we are responsible for initializing the small
        // index region. Avoid touching (zeroing) the whole buffer, which
        // could fault in many pages for large buffers.
        let (file, is_new) = match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
        {
            Ok(f) => (f, true),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                let f = OpenOptions::new().read(true).write(true).open(&path)?;
                (f, false)
            }
            Err(e) => return Err(e),
        };

        if is_new {
            // Only set length for a freshly created file. Avoid truncating an
            // existing file which could clobber data.
            file.set_len(Self::SHM_TOTAL_SIZE as u64)?;
        }

        let mmap = unsafe {
            let mut mmap = MmapOptions::new()
                .len(Self::SHM_TOTAL_SIZE)
                .map_mut(&file)?;

            if is_new {
                // Initialize only the indices region (small) instead of the
                // whole buffer. Construct RingIndices in-place with zeroed
                // atomics so other processes can observe a sane initial state.
                let indices_ptr = mmap.as_mut_ptr() as *mut RingIndices;
                std::ptr::write(
                    indices_ptr,
                    RingIndices {
                        write_index: AtomicUsize::new(0),
                        read_index: AtomicUsize::new(0),
                    },
                );
                // Optionally flush to ensure visibility to other processes that
                // may map the file immediately. Not strictly necessary for
                // most IPC patterns, but can be enabled if needed:
                // mmap.flush_async()?;
            }

            mmap
        };

        Ok(Self {
            mmap: UnsafeCell::new(mmap),
        })
    }

    pub fn indices(&self) -> &RingIndices {
        unsafe {
            let ptr = (*self.mmap.get()).as_ptr() as *const RingIndices;
            &*ptr
        }
    }

    // pub fn buffer_mut(&mut self) -> &mut [u8] {
    //     unsafe {
    //         let data_ptr = (*self.mmap.get()).as_mut_ptr().add(INDEX_REGION_SIZE);
    //         slice::from_raw_parts_mut(data_ptr, BUFFER_SIZE)
    //     }
    // }

    pub fn buffer_ptr(&self) -> *mut u8 {
        unsafe { (*self.mmap.get()).as_ptr().add(Self::INDEX_REGION_SIZE) as *mut u8 }
    }
}

impl<const BUFFER_SIZE: usize> RingBuffer<BUFFER_SIZE> {
    pub fn new(store_file_name: &str) -> std::io::Result<Self> {
        let shm = ShmRegion::<BUFFER_SIZE>::new(store_file_name)?;
        Ok(Self { shm: Arc::new(shm) })
    }

    pub fn try_send(&self, msg: &[u8]) -> Result<(), &'static str> {
        let len = msg.len();
        let aligned_len_u64 = (len + 7) & !7;

        let indices = self.shm.indices();
        let current_write = indices.write_index.load(Ordering::SeqCst);
        let current_read = indices.read_index.load(Ordering::SeqCst);

        let used_len = std::mem::size_of::<u64>();

        println!("{}  {}  {}", current_write, current_read, len);

        let used_size = if current_write >= current_read {
            current_write - current_read
        } else {
            BUFFER_SIZE - current_read + current_write
        };
        
        if used_size + aligned_len_u64 + used_len > BUFFER_SIZE {
            return Err("Buffer full");
        }

        let mut available_space = BUFFER_SIZE - current_write;

        let buffer_start_ptr = self.shm.buffer_ptr();

        let (next_write, buffer_ptr) = if available_space == 0 {
            available_space = current_read;
            (
                aligned_len_u64 + used_len - available_space,
                buffer_start_ptr,
            )
        } else if available_space < aligned_len_u64 + used_len {
            (
                aligned_len_u64 + used_len - available_space,
                buffer_start_ptr.wrapping_add(current_write),
            )
        } else {
            (
                current_write.wrapping_add(aligned_len_u64 + used_len),
                buffer_start_ptr.wrapping_add(current_write),
            )
        };

        match self.shm.indices().write_index.compare_exchange(
            current_write,
            next_write,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => unsafe {
                std::ptr::copy_nonoverlapping(len.to_le_bytes().as_ptr(), buffer_ptr, used_len);

                if aligned_len_u64 + used_len <= available_space {
                    std::ptr::copy_nonoverlapping(
                        msg.as_ptr(),
                        buffer_ptr.wrapping_add(used_len),
                        aligned_len_u64,
                    );
                } else {
                    std::ptr::copy_nonoverlapping(
                        msg.as_ptr(),
                        buffer_ptr.wrapping_add(used_len),
                        available_space,
                    );

                    println!("{}   {}", aligned_len_u64, available_space);

                    std::ptr::copy_nonoverlapping(
                        msg.as_ptr().wrapping_add(available_space),
                        buffer_start_ptr,
                        aligned_len_u64 - available_space,
                    );
                }
            },
            Err(_) => return Err("Could not reserve space on buffer"),
        }

        Ok(())
    }

    pub fn try_receive(&self) -> Result<Option<Vec<u8>>, &'static str> {
        let indices = self.shm.indices();
        let current_read = indices.read_index.load(Ordering::SeqCst);
        let current_write = indices.write_index.load(Ordering::SeqCst);

        if current_write == current_read {
            return Ok(None);
        }

        let used_len = std::mem::size_of::<u64>();
        let mut buffer_ptr = self.shm.buffer_ptr().wrapping_add(current_read);

        let len = unsafe {
            u64::from_le_bytes(
                std::slice::from_raw_parts(buffer_ptr, used_len)
                    .try_into()
                    .unwrap(),
            ) as usize
        };

        let mut dst_buffer = Vec::<u8>::with_capacity(len as usize);
        let dst_ptr = dst_buffer.as_mut_ptr();

        let next_read = len + used_len;

        match self.shm.indices().read_index.compare_exchange(
            current_read,
            next_read,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => unsafe {
                buffer_ptr = buffer_ptr.wrapping_add(used_len);

                if current_read + used_len + len <= BUFFER_SIZE {
                    std::ptr::copy_nonoverlapping(buffer_ptr, dst_ptr, len);
                } else {
                    let first_part_len = BUFFER_SIZE - current_read;
                    std::ptr::copy_nonoverlapping(buffer_ptr, dst_ptr, first_part_len);

                    std::ptr::copy_nonoverlapping(
                        buffer_ptr.wrapping_add(first_part_len),
                        dst_ptr.wrapping_add(first_part_len),
                        len - first_part_len,
                    );
                }
            },
            Err(_) => return Err("Could not read; try again"),
        }

        Ok(Some(dst_buffer))
    }
}

impl<const N: usize> Clone for RingBuffer<N> {
    fn clone(&self) -> Self {
        Self {
            shm: Arc::clone(&self.shm),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_single_thread() {
        let buffer = RingBuffer::<32>::new("temp2").unwrap();
        for i in 0..8 {
            println!("i = {}", i);
            let _ = buffer.try_send(&(i as u64).to_le_bytes());

            let e = buffer.try_receive().unwrap();

            dbg!(e);
        }
    }
}
