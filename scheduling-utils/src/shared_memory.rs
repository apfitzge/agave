use {
    rts_alloc::Allocator,
    std::{ffi::CStr, fs::File, io, os::fd::FromRawFd},
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
