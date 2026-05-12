use {
    crossbeam_channel::{Receiver, Sender, TrySendError, bounded},
    solana_entry::entry::EntryVerificationData,
    solana_hash::Hash,
    std::{num::NonZeroUsize, thread},
};

const CHANNEL_CAPACITY: usize = 16_384;

pub(crate) struct EntryHashVerifier {
    job_sender: Option<Sender<EntryHashVerificationTask>>,
    result_receiver: Receiver<EntryHashVerificationResult>,
    worker_handles: Vec<thread::JoinHandle<()>>,
}

pub(crate) struct EntryHashVerificationTask {
    slot: u64,
    start_hash: Hash,
    verification_data: EntryVerificationData,
}

pub(crate) struct EntryHashVerificationResult {
    pub(crate) slot: u64,
    pub(crate) is_valid: bool,
}

pub(crate) enum EntryHashVerificationSubmitError {
    Full(EntryHashVerificationTask),
    Disconnected(EntryHashVerificationTask),
}

impl EntryHashVerificationTask {
    pub(crate) fn new(
        slot: u64,
        start_hash: Hash,
        verification_data: EntryVerificationData,
    ) -> Self {
        Self {
            slot,
            start_hash,
            verification_data,
        }
    }

    pub(crate) fn verify(self) -> EntryHashVerificationResult {
        let is_valid = self.verification_data.verify(&self.start_hash);
        EntryHashVerificationResult {
            slot: self.slot,
            is_valid,
        }
    }
}

impl EntryHashVerificationSubmitError {
    pub(crate) fn verify_inline(self) -> EntryHashVerificationResult {
        match self {
            Self::Full(task) | Self::Disconnected(task) => task.verify(),
        }
    }
}

fn send_result(
    result_sender: &Sender<EntryHashVerificationResult>,
    mut result: EntryHashVerificationResult,
) {
    loop {
        match result_sender.try_send(result) {
            Ok(()) => break,
            Err(TrySendError::Full(returned_result)) => {
                result = returned_result;
                thread::yield_now();
            }
            Err(TrySendError::Disconnected(_)) => break,
        }
    }
}

impl EntryHashVerifier {
    pub(crate) fn new(num_threads: NonZeroUsize) -> Self {
        let (job_sender, job_receiver) = bounded::<EntryHashVerificationTask>(CHANNEL_CAPACITY);
        let (result_sender, result_receiver) = bounded(CHANNEL_CAPACITY);
        let mut worker_handles = Vec::with_capacity(num_threads.get());

        for index in 0..num_threads.get() {
            let job_receiver = job_receiver.clone();
            let result_sender = result_sender.clone();
            let worker_handle = thread::Builder::new()
                .name(format!("solEntryHash{index:02}"))
                .spawn(move || {
                    while let Ok(task) = job_receiver.recv() {
                        send_result(&result_sender, task.verify());
                    }
                })
                .expect("failed to spawn replay entry verification thread");
            worker_handles.push(worker_handle);
        }

        Self {
            job_sender: Some(job_sender),
            result_receiver,
            worker_handles,
        }
    }

    pub(crate) fn try_submit(
        &self,
        task: EntryHashVerificationTask,
    ) -> Result<(), EntryHashVerificationSubmitError> {
        let Some(job_sender) = self.job_sender.as_ref() else {
            return Err(EntryHashVerificationSubmitError::Disconnected(task));
        };

        match job_sender.try_send(task) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(task)) => Err(EntryHashVerificationSubmitError::Full(task)),
            Err(TrySendError::Disconnected(task)) => {
                Err(EntryHashVerificationSubmitError::Disconnected(task))
            }
        }
    }

    pub(crate) fn try_recv_result(&self) -> Option<EntryHashVerificationResult> {
        self.result_receiver.try_recv().ok()
    }
}

impl Drop for EntryHashVerifier {
    fn drop(&mut self) {
        drop(self.job_sender.take());
        for worker_handle in self.worker_handles.drain(..) {
            let _ = worker_handle.join();
        }
    }
}
