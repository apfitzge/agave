use {
    rts_alloc::Allocator,
    std::{
        ffi::CStr,
        fs::{File, OpenOptions},
        io,
        os::fd::FromRawFd,
        path::Path,
    },
    thiserror::Error,
};

const HUGE_PAGE_SIZE: usize = 2 * 1024 * 1024;
const REGULAR_PAGE_SIZE: usize = 4096;

#[derive(Debug, Error)]
pub enum SharedMemoryError {
    #[error("io; err={0}")]
    Io(#[from] io::Error),
    #[error("rts alloc; err={0:?}")]
    RtsAlloc(#[from] rts_alloc::error::Error),
    #[error("shaq; err={0:?}")]
    Shaq(#[from] shaq::error::Error),
}

pub fn create_allocator(
    name: &CStr,
    allocator_size: usize,
    allocator_count: u32,
    slab_size: u32,
) -> Result<(File, Allocator), SharedMemoryError> {
    let create = |huge: bool| {
        let file = create_shmem(name, huge)?;
        let file_size = align_file_size(allocator_size, huge);
        let allocator = unsafe { Allocator::create(&file, file_size, allocator_count, slab_size) }?;

        Ok((file, allocator))
    };

    create(true).or_else(|_| create(false))
}

pub fn create_producer<T>(
    name: &CStr,
    capacity: usize,
    huge: bool,
) -> Result<(File, shaq::spsc::Producer<T>), SharedMemoryError> {
    let create = |huge: bool| {
        let file = create_shmem(name, huge)?;
        let minimum_file_size = shaq::spsc::minimum_file_size::<T>(capacity);
        let file_size = align_file_size(minimum_file_size, huge);
        let producer = unsafe { shaq::spsc::Producer::create(&file, file_size) }?;

        Ok((file, producer))
    };

    match huge {
        true => create(true).or_else(|_| create(false)),
        false => create(false),
    }
}

pub fn create_consumer<T>(
    name: &CStr,
    capacity: usize,
    huge: bool,
) -> Result<(File, shaq::spsc::Consumer<T>), SharedMemoryError> {
    let create = |huge: bool| {
        let file = create_shmem(name, huge)?;
        let minimum_file_size = shaq::spsc::minimum_file_size::<T>(capacity);
        let file_size = align_file_size(minimum_file_size, huge);
        let consumer = unsafe { shaq::spsc::Consumer::create(&file, file_size) }?;

        Ok((file, consumer))
    };

    match huge {
        true => create(true).or_else(|_| create(false)),
        false => create(false),
    }
}

pub fn create_queue_pair<T>(
    name: &CStr,
    capacity: usize,
    huge: bool,
) -> Result<(shaq::spsc::Producer<T>, shaq::spsc::Consumer<T>), SharedMemoryError> {
    let (file, producer) = create_producer(name, capacity, huge)?;
    let consumer = unsafe { shaq::spsc::Consumer::join(&file) }?;

    Ok((producer, consumer))
}

pub fn create_mpmc_queue_pair<T>(
    name: &CStr,
    capacity: usize,
    huge: bool,
) -> Result<(shaq::mpmc::Producer<T>, shaq::mpmc::Consumer<T>), SharedMemoryError> {
    let create = |huge: bool| {
        let file = create_shmem(name, huge)?;
        let minimum_file_size = shaq::mpmc::minimum_file_size::<T>(capacity);
        let file_size = align_file_size(minimum_file_size, huge);
        let producer = unsafe { shaq::mpmc::Producer::create(&file, file_size) }?;
        let consumer = unsafe { shaq::mpmc::Consumer::join(&file) }?;

        Ok((producer, consumer))
    };

    match huge {
        true => create(true).or_else(|_| create(false)),
        false => create(false),
    }
}

pub fn create_broadcast_producer_at_path<T>(
    path: &Path,
    capacity: usize,
) -> Result<shaq::broadcast::Producer<T>, SharedMemoryError> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    let minimum_file_size = shaq::broadcast::minimum_file_size::<T>(capacity);
    let file_size = align_file_size(minimum_file_size, false);
    file.set_len(file_size.try_into().map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "broadcast queue file size does not fit in u64",
        )
    })?)?;
    let producer = unsafe { shaq::broadcast::Producer::create(&file, file_size) }?;

    Ok(producer)
}

pub fn join_broadcast_consumer_at_path<T>(
    path: &Path,
) -> Result<shaq::broadcast::Consumer<T>, SharedMemoryError> {
    let file = OpenOptions::new().read(true).write(true).open(path)?;
    let consumer = unsafe { shaq::broadcast::Consumer::join(&file) }?;

    Ok(consumer)
}

#[cfg(any(
    target_os = "linux",
    target_os = "l4re",
    target_os = "android",
    target_os = "emscripten"
))]
fn create_shmem(name: &CStr, huge: bool) -> Result<File, io::Error> {
    let flags = match huge {
        true => libc::MFD_HUGETLB | libc::MFD_HUGE_2MB,
        false => 0,
    };

    let ret = unsafe { libc::memfd_create(name.as_ptr(), flags) };
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }

    Ok(unsafe { File::from_raw_fd(ret) })
}

#[cfg(all(
    unix,
    not(any(
        target_os = "linux",
        target_os = "l4re",
        target_os = "android",
        target_os = "emscripten"
    ))
))]
fn create_shmem(name: &CStr, huge: bool) -> Result<File, io::Error> {
    if huge {
        return Err(io::ErrorKind::Unsupported.into());
    }

    let ret = unsafe { libc::shm_unlink(name.as_ptr()) };
    if ret == -1 {
        let err = io::Error::last_os_error();
        if err.kind() != io::ErrorKind::NotFound {
            return Err(err);
        }
    }

    let ret = unsafe {
        libc::shm_open(
            name.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
            #[cfg(not(any(target_os = "macos", target_os = "ios")))]
            {
                libc::S_IRUSR | libc::S_IWUSR
            },
            #[cfg(any(target_os = "macos", target_os = "ios"))]
            {
                (libc::S_IRUSR | libc::S_IWUSR) as libc::c_uint
            },
        )
    };
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }
    let file = unsafe { File::from_raw_fd(ret) };

    let ret = unsafe { libc::shm_unlink(name.as_ptr()) };
    if ret == -1 {
        return Err(io::Error::last_os_error());
    }

    Ok(file)
}

fn align_file_size(size: usize, huge: bool) -> usize {
    match huge {
        true => size.next_multiple_of(HUGE_PAGE_SIZE),
        false => size.next_multiple_of(REGULAR_PAGE_SIZE),
    }
}

#[cfg(test)]
mod tests {
    use {super::*, core::sync::atomic::Ordering};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(C)]
    struct TestBroadcastEvent {
        value: u64,
    }

    #[test]
    fn filesystem_broadcast_queue_round_trips_events() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("agave_events.ipc");
        let producer = create_broadcast_producer_at_path::<TestBroadcastEvent>(&path, 8).unwrap();
        let mut consumer = join_broadcast_consumer_at_path::<TestBroadcastEvent>(&path).unwrap();

        producer
            .try_write(TestBroadcastEvent { value: 42 }, Ordering::Relaxed)
            .unwrap();

        assert_eq!(
            consumer.try_read(Ordering::Relaxed).unwrap(),
            Some(TestBroadcastEvent { value: 42 }),
        );
        assert_eq!(consumer.try_read(Ordering::Relaxed).unwrap(), None);
    }
}
