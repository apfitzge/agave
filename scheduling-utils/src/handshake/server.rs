use {
    crate::{
        handshake::{
            AgaveHandshakeError, AgaveTpuToPackSession, AgaveWorkerSession, ClientLogon,
            shared::{
                AgaveSession, GLOBAL_ALLOCATORS, LOGON_FAILURE, LOGON_SUCCESS,
                MAX_ALLOCATOR_HANDLES, MAX_WORKERS, VERSION,
            },
        },
        shared_memory::{self, SharedMemoryError},
    },
    agave_scheduler_bindings::PackToWorkerMessage,
    nix::sys::socket::{self, ControlMessage, MsgFlags, UnixAddr},
    rts_alloc::Allocator,
    std::{
        ffi::CStr,
        fs::File,
        io::{IoSlice, Read, Write},
        os::{
            fd::AsRawFd,
            unix::net::{UnixListener, UnixStream},
        },
        path::Path,
        time::{Duration, Instant},
    },
};

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);
const SHMEM_NAME: &CStr = c"/agave-scheduler-bindings";
const ALLOCATOR_SLAB_SIZE: u32 = 2 * 1024 * 1024;

/// Implements the Agave side of the scheduler bindings handshake protocol.
pub struct Server {
    listener: UnixListener,

    buffer: [u8; 1024],
}

impl Server {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let listener = UnixListener::bind(path)?;

        Ok(Self {
            listener,
            buffer: [0; 1024],
        })
    }

    pub fn accept(&mut self) -> Result<AgaveSession, AgaveHandshakeError> {
        // Wait for next stream.
        let (mut stream, _) = self.listener.accept()?;
        stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;

        match self.handle_logon(&mut stream) {
            Ok(session) => Ok(session),
            Err(err) => {
                let reason = err.to_string();
                let reason_len = u8::try_from(reason.len()).unwrap_or(u8::MAX);

                let buffer_len = 2usize.checked_add(usize::from(reason_len)).unwrap();
                self.buffer[0] = LOGON_FAILURE;
                self.buffer[1] = reason_len;
                self.buffer[2..buffer_len]
                    .copy_from_slice(&reason.as_bytes()[..usize::from(reason_len)]);

                stream.set_nonblocking(true)?;
                // NB: Caller will still error out even if our write fails so it's fine to ignore the
                // result.
                let _ = stream.write(&self.buffer[..buffer_len])?;

                Err(err)
            }
        }
    }

    fn handle_logon(
        &mut self,
        stream: &mut UnixStream,
    ) -> Result<AgaveSession, AgaveHandshakeError> {
        // Receive & validate the logon message.
        let logon = self.recv_logon(stream)?;

        // Setup the requested shared memory regions.
        let (session, files) = Self::setup_session(logon)?;

        // Send the file descriptors to the client.
        let fds_raw: Vec<_> = files.iter().map(|file| file.as_raw_fd()).collect();
        let iov = [IoSlice::new(&[LOGON_SUCCESS])];
        let cmsgs = [ControlMessage::ScmRights(&fds_raw)];
        let sent =
            socket::sendmsg::<UnixAddr>(stream.as_raw_fd(), &iov, &cmsgs, MsgFlags::empty(), None)
                .map_err(std::io::Error::from)?;
        debug_assert_eq!(sent, 1);

        Ok(session)
    }

    fn recv_logon(&mut self, stream: &mut UnixStream) -> Result<ClientLogon, AgaveHandshakeError> {
        // Read the logon message.
        let handshake_start = Instant::now();
        let mut buffer_len = 0;
        while buffer_len < self.buffer.len() {
            let read = stream.read(&mut self.buffer[buffer_len..])?;
            if read == 0 {
                return Err(AgaveHandshakeError::EofDuringHandshake);
            }

            // SAFETY: We cannot read a value greater than buffer.len() which itself is a usize.
            buffer_len = buffer_len.checked_add(read).unwrap();

            if handshake_start.elapsed() > HANDSHAKE_TIMEOUT {
                return Err(AgaveHandshakeError::Timeout);
            }
        }

        // Ensure exact version match, version will be bumped any time a backwards incompatible
        // change is made to handshake/shared memory objects.
        let version = u64::from_le_bytes(self.buffer[..8].try_into().unwrap());
        if version != VERSION {
            return Err(AgaveHandshakeError::Version {
                server: VERSION,
                client: version,
            });
        }

        // Read the logon message, cannot panic as we ensure the correct buf size at compile time
        // (hence the const just below).
        const LOGON_END: usize = 8 + core::mem::size_of::<ClientLogon>();
        let logon = ClientLogon::try_from_bytes(&self.buffer[8..LOGON_END]).unwrap();

        // Put a hard limit of 64 worker threads for now.
        if !(1..=MAX_WORKERS).contains(&logon.worker_count) {
            return Err(AgaveHandshakeError::WorkerCount(logon.worker_count));
        }

        // Hard limit allocator handles to 128.
        if !(1..=MAX_ALLOCATOR_HANDLES).contains(&logon.allocator_handles) {
            return Err(AgaveHandshakeError::AllocatorHandles(
                logon.allocator_handles,
            ));
        }

        Ok(logon)
    }

    pub fn setup_session(
        logon: ClientLogon,
    ) -> Result<(AgaveSession, Vec<File>), AgaveHandshakeError> {
        // Setup the allocator in shared memory (`worker_count` & `allocator_handles` have been
        // validated so this won't panic).
        let (allocator_file, tpu_to_pack_allocator) = Self::create_allocator(&logon)?;

        // Setup the global queues.
        let (tpu_to_pack_file, tpu_to_pack_queue) =
            shared_memory::create_producer(SHMEM_NAME, logon.tpu_to_pack_capacity, true)?;
        let (progress_tracker_file, progress_tracker) =
            shared_memory::create_producer(SHMEM_NAME, logon.progress_tracker_capacity, false)?;

        // Setup the worker sessions.
        let (worker_files, workers) = (0..logon.worker_count).try_fold(
            (Vec::default(), Vec::default()),
            |(mut fds, mut workers), _| {
                let allocator = Allocator::join(&allocator_file)?;

                let (pack_to_worker_file, pack_to_worker) =
                    shared_memory::create_consumer::<PackToWorkerMessage>(
                        SHMEM_NAME,
                        logon.pack_to_worker_capacity,
                        true,
                    )?;
                let (worker_to_pack_file, worker_to_pack) = shared_memory::create_producer(
                    SHMEM_NAME,
                    logon.worker_to_pack_capacity,
                    true,
                )?;

                fds.extend([pack_to_worker_file, worker_to_pack_file]);
                workers.push(AgaveWorkerSession {
                    allocator,
                    pack_to_worker,
                    worker_to_pack,
                });

                Ok::<_, AgaveHandshakeError>((fds, workers))
            },
        )?;

        Ok((
            AgaveSession {
                flags: logon.flags,
                tpu_to_pack: AgaveTpuToPackSession {
                    allocator: tpu_to_pack_allocator,
                    producer: tpu_to_pack_queue,
                },
                progress_tracker,
                workers,
            },
            [allocator_file, tpu_to_pack_file, progress_tracker_file]
                .into_iter()
                .chain(worker_files)
                .collect(),
        ))
    }

    fn create_allocator(logon: &ClientLogon) -> Result<(File, Allocator), AgaveHandshakeError> {
        let allocator_count = GLOBAL_ALLOCATORS
            .checked_add(logon.worker_count)
            .unwrap()
            .checked_add(logon.allocator_handles)
            .unwrap();

        shared_memory::create_allocator(
            SHMEM_NAME,
            logon.allocator_size,
            u32::try_from(allocator_count).unwrap(),
            ALLOCATOR_SLAB_SIZE,
        )
        .map_err(Into::into)
    }
}

impl From<SharedMemoryError> for AgaveHandshakeError {
    fn from(value: SharedMemoryError) -> Self {
        match value {
            SharedMemoryError::Io(err) => Self::Io(err),
            SharedMemoryError::RtsAlloc(err) => Self::RtsAlloc(err),
            SharedMemoryError::Shaq(err) => Self::Shaq(err),
        }
    }
}
