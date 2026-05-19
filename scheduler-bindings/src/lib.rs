#![cfg(feature = "agave-unstable-api")]
#![no_std]

//! Messages passed between agave and an external pack process.
//! Messages are passed via `shaq::spsc::Consumer/Producer`.
//!
//! Memory freeing is responsibility of the external pack process,
//! and is done via `rts-alloc` crate. It is also possible the external
//! pack process allocates memory to pass to agave, BUT it will still be
//! the responsibility of the external pack process to free that memory.
//!
//! Setting up the shared memory allocator and queues is done outside of
//! agave - it can be done by the external pack process or another
//! process. agave will just `join` shared memory regions, but not
//! create them.
//! Similarly, agave will not delete files used for shared memory regions.
//! See `shaq` and `rts-alloc` crates for details.
//!
//! The basic architecture is as follows:
//!        ┌───────────────┐       ┌─────────────────┐
//!        │  tpu_to_pack  │       │ progress_tracker│
//!        └───────┬───────┘       └───────┬─────────┘
//!                │                       │
//!                │                       │
//!                │                       │
//!             ┌──▼───────────────────────▼───┐
//!             │  external scheduler          │
//!             └─▲─────── ▲────────────────▲──┘
//!               │        │                │
//!               │        │                │
//!           ┌───▼───┐ ┌──▼─────┐ ...  ┌───▼───┐
//!           │worker1│ │worker2 │      │workerN│
//!           └───────┘ └────────┘      └───────┘
//!
//! - [`TpuToPackMessage`] are sent from `tpu_to_pack` queue to the
//!   external scheduler process. This passes in tpu transactions to be scheduled,
//!   and optionally vote transactions.
//! - [`ReplayToPackMessage`] are sent from replay to the external scheduler
//!   process. This passes bank lifecycle events, entry headers, and transactions.
//! - [`ProgressMessage`] are sent from `progress_tracker` queue to the
//!   external scheduler process. This passes information about leader status
//!   and slot progress to the external scheduler process.
//! - [`ReplayBlockStatusMessage`] are sent from the external scheduler process
//!   back to replay.
//! - [`PackToWorkerMessage`] are sent from the external scheduler process
//!   to worker threads within agave. This passes a batch of transactions
//!   to be processed by the worker threads. This processing can also involve
//!   resolving the transactions' addresses, or similar operations beyond
//!   execution. Replay ingress messages are consumed by the scheduler and must
//!   not be sent directly to workers.
//! - [`WorkerToPackMessage`] are sent from worker threads within agave
//!   back to the external scheduler process. This passes back the results
//!   of processing the transactions.
//!
//! Ownership and pointer lifetime rule:
//! - Sending a message that contains offsets or pointers to memory transfers
//!   ownership of that memory to the receiver for the lifetime defined by the
//!   protocol.
//! - The sender must treat that memory as not owned until ownership is
//!   explicitly returned by a later message, if any. In particular, the sender
//!   must not free or modify that memory while it is not owned.
//! - If the protocol does not return the memory to the sender, the receiver is
//!   responsible for freeing it.
//!

extern crate std;

/// Reference to a transaction that can shared safely across processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SharableTransactionRegion {
    /// Offset within the shared memory allocator.
    pub offset: usize,
    /// Length of the transaction in bytes.
    pub length: u32,
}

/// Reference to an array of Pubkeys that can be shared safely across processes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SharablePubkeys {
    /// Offset within the shared memory allocator.
    pub offset: usize,
    /// Number of pubkeys in the array.
    /// IF 0, indicates no pubkeys and no allocation needing to be freed.
    pub num_pubkeys: u32,
}

/// Reference to an array of [`SharableTransactionRegion`] that can be shared safely
/// across processes.
/// General flow:
/// 1. External pack process allocates memory for
///    `num_transactions` [`SharableTransactionRegion`].
/// 2. External pack sends a [`PackToWorkerMessage`] with `batch`.
/// 3. agave processes the transactions and sends back a [`WorkerToPackMessage`]
///    with the same `batch`.
/// 4. External pack process frees all transaction memory pointed to by the
///    [`SharableTransactionRegion`] in the batch, then frees the memory for
///    the array of [`SharableTransactionRegion`].
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(C)]
pub struct SharableTransactionBatchRegion {
    /// Number of transactions in the batch.
    pub num_transactions: u8,
    /// Offset within the shared memory allocator for the batch of transactions.
    /// The transactions are laid out back-to-back in memory as a
    /// [`SharableTransactionRegion`] with size `num_transactions`.
    pub transactions_offset: usize,
}
/// Reference to an array of response messages.
/// General flow:
/// 1. agave allocates memory for `num_transaction_responses` inner messages.
/// 2. agave sends a [`WorkerToPackMessage`] with `responses`.
/// 3. External pack process processes the inner messages. Potentially freeing
///    any memory within each inner message (see [`worker_message_types`] for details).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct TransactionResponseRegion {
    /// Tag indicating the type of message.
    /// See [`worker_message_types`] for details.
    /// All inner messages/responses per transaction will be of the same type.
    pub tag: u8,
    /// The number of transactions in the original message.
    /// This corresponds to the number of inner response
    /// messages that will be pointed to by `response_offset`.
    /// This MUST be the same as `batch.num_transactions`.
    pub num_transaction_responses: u8,
    /// Offset within the shared memory allocator for the array of
    /// inner messages.
    /// The inner messages are laid out back-to-back in memory starting at
    /// this offset. The type of each inner message is indicated by `tag`.
    /// There are `num_transaction_responses` inner messages.
    /// See [`worker_message_types`] for details on the inner message types.
    pub transaction_responses_offset: usize,
}

/// Message: [TPU -> Pack]
/// TPU passes transactions to the external pack process.
/// This is also a transfer of ownership of the transaction:
///   the external pack process is responsible for freeing the memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct TpuToPackMessage {
    pub transaction: SharableTransactionRegion,
    /// See [`tpu_message_flags`] for details.
    pub flags: u8,
    /// The source address of the transaction.
    /// IPv6-mapped IPv4 addresses: `::ffff:a.b.c.d`
    /// where a.b.c.d is the IPv4 address.
    /// See <https://datatracker.ietf.org/doc/html/rfc4291#section-2.5.5.2>.
    pub src_addr: [u8; 16],
}

pub mod tpu_message_flags {
    /// No special flags.
    pub const NONE: u8 = 0;

    /// The transaction is a simple vote transaction.
    pub const IS_SIMPLE_VOTE: u8 = 1 << 0;
    /// The transaction was forwarded by a validator node.
    pub const FORWARDED: u8 = 1 << 1;
    /// The transaction was sent from a staked node.
    pub const FROM_STAKED_NODE: u8 = 1 << 2;
}

/// Header for an entry sent from replay to the external scheduler.
///
/// This header is sent as a [`ReplayToPackMessage`] with
/// [`replay_to_pack_message_types::ENTRY_HEADER`], followed by exactly
/// [`Self::num_transactions`] [`replay_to_pack_message_types::TRANSACTION`]
/// messages. If `num_transactions` is zero, the header represents a tick entry
/// and is not followed by any transaction messages. The header carries the
/// bank slot for the entry; the immediately following transaction messages
/// belong to the same slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct EntryHeader {
    /// Bank slot for this entry.
    pub slot: u64,
    /// Time when replay sent this entry header.
    pub replay_send_time: std::time::Instant,
    /// The number of hashes since the previous entry ID.
    pub num_hashes: u64,
    /// The SHA-256 hash `num_hashes` after the previous entry ID.
    pub hash: [u8; 32],
    /// Number of transaction messages that follow this header.
    pub num_transactions: u32,
}

/// Bank lifecycle payload for [`ReplayToPackMessage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ReplayBankMessage {
    /// See [`replay_bank_message_kinds`] for details.
    pub kind: u8,
    /// Bank slot.
    pub slot: u64,
    /// Bank id.
    ///
    /// This is required for replay vote verification messages. It is only
    /// meaningful for [`replay_bank_message_kinds::BEGIN`].
    pub bank_id: u64,
    /// Starting hash for verifying the slot's entry chain when `kind` is
    /// [`replay_bank_message_kinds::BEGIN`].
    ///
    /// This is typically the bank's initial last blockhash. It is ignored for
    /// other message kinds.
    pub last_entry_hash: [u8; 32],
}

pub mod replay_bank_message_kinds {
    /// Begin replay scheduling for this bank.
    pub const BEGIN: u8 = 0;
    /// Replay-stage has sent all entry and transaction messages for this bank.
    pub const COMPLETE: u8 = 1;
    /// Abort replay scheduling for this bank.
    pub const ABORT: u8 = 2;
}

/// Payload for [`ReplayToPackMessage`].
///
/// The active field is selected by [`ReplayToPackMessage::tag`].
#[derive(Clone, Copy)]
#[repr(C)]
pub union ReplayToPackMessagePayload {
    /// Active when [`ReplayToPackMessage::tag`] is
    /// [`replay_to_pack_message_types::BANK`].
    pub bank: ReplayBankMessage,
    /// Active when [`ReplayToPackMessage::tag`] is
    /// [`replay_to_pack_message_types::ENTRY_HEADER`].
    pub entry_header: EntryHeader,
    /// Active when [`ReplayToPackMessage::tag`] is
    /// [`replay_to_pack_message_types::TRANSACTION`].
    pub transaction: SharableTransactionRegion,
}

/// Message: [Replay -> Pack]
/// Replay passes bank lifecycle events, entry headers, and transactions to the
/// external scheduler.
///
/// This is a tagged union. Transactions do not carry their entry header.
/// Instead, the protocol is one [`EntryHeader`] message followed by exactly
/// `EntryHeader::num_transactions` transaction messages. Bank lifecycle
/// messages frame which bank subsequent replay ingress belongs to.
///
/// Transaction messages transfer ownership of the transaction memory to the
/// external scheduler, as with [`TpuToPackMessage`].
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ReplayToPackMessage {
    /// Tag indicating the active union field.
    /// See [`replay_to_pack_message_types`] for details.
    pub tag: u8,
    /// The tagged payload.
    pub payload: ReplayToPackMessagePayload,
}

pub mod replay_to_pack_message_types {
    /// [`crate::ReplayToPackMessage::payload`] contains a
    /// [`crate::ReplayBankMessage`].
    pub const BANK: u8 = 0;
    /// [`crate::ReplayToPackMessage::payload`] contains an
    /// [`crate::EntryHeader`].
    pub const ENTRY_HEADER: u8 = 1;
    /// [`crate::ReplayToPackMessage::payload`] contains a
    /// [`crate::SharableTransactionRegion`].
    pub const TRANSACTION: u8 = 2;
}

/// Message: [Pack -> Replay]
/// External scheduler reports replay block status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ReplayBlockStatusMessage {
    /// Bank slot for the replayed block.
    pub slot: u64,
    /// See [`replay_block_status_codes`] for details.
    pub status: u8,
    /// See [`replay_block_status_reasons`] for details.
    pub reason: u16,
}

pub mod replay_block_status_codes {
    /// The block was replayed successfully.
    pub const SUCCESS: u8 = 0;
    /// The block failed replay.
    pub const FAILED: u8 = 1;
    /// The scheduler is done with an aborted bank.
    ///
    /// [`crate::ReplayBlockStatusMessage::reason`] should be
    /// [`crate::replay_block_status_reasons::NONE`].
    pub const ABORTED: u8 = 2;
}

pub mod replay_block_status_reasons {
    /// No failure reason. Used with [`super::replay_block_status_codes::SUCCESS`]
    /// and [`super::replay_block_status_codes::ABORTED`].
    pub const NONE: u16 = 0;

    /// Block did not have enough ticks and no shred marked it full.
    pub const INCOMPLETE: u16 = 1;
    /// Block entries hashes were invalid.
    pub const INVALID_ENTRY_HASH: u16 = 2;
    /// Block did not end in a valid last tick.
    pub const INVALID_LAST_TICK: u16 = 3;
    /// Block had too few ticks.
    pub const TOO_FEW_TICKS: u16 = 4;
    /// Block had too many ticks.
    pub const TOO_MANY_TICKS: u16 = 5;
    /// Block had an invalid tick hash count.
    pub const INVALID_TICK_HASH_COUNT: u16 = 6;
    /// Block had trailing transaction entries.
    pub const TRAILING_ENTRY: u16 = 7;
    /// Block was a duplicate block.
    pub const DUPLICATE_BLOCK: u16 = 8;

    /// Failed to load entries from blockstore.
    pub const FAILED_TO_LOAD_ENTRIES: u16 = 9;
    /// Failed to load blockstore metadata.
    pub const FAILED_TO_LOAD_META: u16 = 10;
    /// Failed to replay bank 0.
    pub const FAILED_TO_REPLAY_BANK0: u16 = 11;
    /// Invalid transaction while replaying the block.
    pub const INVALID_TRANSACTION: u16 = 12;
    /// No valid forks were found.
    pub const NO_VALID_FORKS_FOUND: u16 = 13;
    /// Invalid hard fork slot.
    pub const INVALID_HARD_FORK: u16 = 14;
    /// Root bank capitalization mismatched.
    pub const ROOT_BANK_WITH_MISMATCHED_CAPITALIZATION: u16 = 15;
    /// User transactions were found in a vote-only bank.
    pub const USER_TRANSACTIONS_IN_VOTE_ONLY_BANK: u16 = 16;
    /// Parent-child chained block ID check failed.
    pub const CHAINED_BLOCK_ID_FAILURE: u16 = 17;
    /// Block component processor failed.
    pub const BLOCK_COMPONENT_PROCESSOR: u16 = 18;
    /// Bank hash did not match the expected hash.
    pub const BANK_HASH_MISMATCH: u16 = 19;
}

/// The node is not currently in a leader slot.
pub const NOT_LEADER: u8 = 0;
/// The node is in a leader slot but the working bank is not yet ready.
///
/// Transactions cannot be processed in this state.
pub const LEADER_STARTING: u8 = 1;
/// The node is in a leader slot and has a working bank ready.
///
/// Transactions can be processed in this state.
pub const LEADER_READY: u8 = 2;

/// Message: [Agave -> Pack]
/// Agave passes leader status to the external pack process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ProgressMessage {
    /// Indicates the current leader status of the node.
    ///
    /// - [`NOT_LEADER`]: Node is not in a leader slot.
    /// - [`LEADER_STARTING`]: Node is in a leader slot but bank is not ready.
    /// - [`LEADER_READY`]: Node is in a leader slot with bank ready for transactions.
    ///
    /// # Usage
    ///
    /// - To check if within a leader slot: `leader_state != NOT_LEADER`.
    /// - To check if transactions can be processed: `leader_state == LEADER_READY`.
    pub leader_state: u8,
    /// Progress through the current slot in percentage.
    pub current_slot_progress: u8,
    /// The current epoch of the working bank.
    /// Only valid if `leader_state == LEADER_READY`, otherwise zeroed.
    pub epoch: u64,
    /// The current slot.
    pub current_slot: u64,
    /// Next known leader slot or u64::MAX if unknown.
    /// This will **not** include the current slot if leader.
    /// Node is leader for contiguous slots in the inclusive range
    /// [[`Self::next_leader_slot`], [`Self::leader_range_end`]].
    pub next_leader_slot: u64,
    /// Next known leader slot range end (inclusive) or u64::MAX if unknown.
    /// Node is leader for contiguous slots in the inclusive range
    /// [[`Self::next_leader_slot`], [`Self::leader_range_end`]].
    pub leader_range_end: u64,
    /// The remaining cost units allowed to be packed in the block.
    /// i.e. block_limit - current_cost_units_used.
    /// Only valid if currently leader, otherwise the value is undefined.
    pub remaining_cost_units: u64,
    /// The latest blockhash of the working bank.
    /// Only valid if `leader_state == LEADER_READY`, otherwise zeroed.
    pub latest_blockhash: [u8; 32],
}

/// Maximum number of transactions allowed in a [`PackToWorkerMessage`].
/// If the number of transactions exceeds this value, agave will
/// not process the message.
//
// The reason for this constraint is because rts-alloc currently only
// supports up to 4096 byte allocations. We must ensure that the
// `TransactionResponseRegion` is able to contain responses for all
// transactions sent. This is a conservative bound.
pub const MAX_TRANSACTIONS_PER_MESSAGE: usize = 32;

/// Sentinel used when a worker message is not a replay execution or check message.
pub const NO_REPLAY_BANK_SLOT: u64 = u64::MAX;

/// Message: [Pack -> Worker]
/// External pack processe passes transactions to worker threads within agave.
///
/// These messages do not transfer ownership of the transactions.
/// The external pack process is still responsible for freeing the memory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PackToWorkerMessage {
    /// Flags on how to handle this message.
    /// See [`pack_message_flags`] for details.
    pub flags: u16,
    /// Maximum working bank slot that this message will be processed
    /// for. For leader-side checks and execution, this will check the
    /// leader bank if it exists.
    /// If the working bank is ahead of the slot, the return message will
    /// be set with [`NOT_PROCESSED`].
    ///
    /// For replay check and execution messages, this is the exact bank slot
    /// to process against and must not be [`NO_REPLAY_BANK_SLOT`].
    pub max_working_slot: u64,
    /// Offset and number of transactions in the batch.
    /// See [`SharableTransactionBatchRegion`] for details.
    /// Agave will return this batch in the response message, it is
    /// the responsibility of the external pack process to free the memory
    /// ONLY after receiving the response message.
    pub batch: SharableTransactionBatchRegion,
}

pub mod pack_message_flags {
    //! Flags for [`crate::PackToWorkerMessage::flags`].
    //! Use [`CHECK`] or [`EXECUTE`] to specify how a batch should be processed.
    //! See [`check_flags`] and [`execution_flags`] for details.

    /// Combine with [`check_flags`] for performing checks on transactions.
    /// Worker will respond with [`super::worker_message_types::CheckResponse`] if
    /// the message is processed.
    pub const CHECK: u16 = 0;
    /// Combine with additional [`execution_flags`] for executing a batch of transactions.
    /// Worker will responsd with [`super::worker_message_types::ExecutionResponse`] if
    /// the message is processed.
    pub const EXECUTE: u16 = 1;

    pub mod execution_flags {
        /// Should failing transactions within the batch be dropped (no fee charged & not
        /// committed).
        pub const DROP_ON_FAILURE: u16 = 1 << 1;
        /// If any transaction in the batch is not committed then the entire batch should not be
        /// committed.
        ///
        /// # Note
        ///
        /// Without `drop_on_failure` this flag will still allow processed but failing transactions
        /// to be committed. If both flags are set then any failing transaction will cause all
        /// transactions to be aborted.
        pub const ALL_OR_NOTHING: u16 = 1 << 2;
        /// Execute this batch as replay work.
        ///
        /// Replay uses the existing [`crate::PackToWorkerMessage`] path to reuse
        /// external consume workers. Workers receive normal execution messages
        /// with this flag set; workers must not receive
        /// [`crate::ReplayToPackMessage`] values.
        pub const REPLAY: u16 = 1 << 3;
    }

    pub mod check_flags {
        /// Transactions should check status: if transaction has already been processed
        /// or the nonce is invalid.
        pub const STATUS_CHECKS: u16 = 1 << 1;

        /// Fee-payer balance should be fetched for transactions.
        pub const LOAD_FEE_PAYER_BALANCE: u16 = 1 << 2;

        /// Transactions should have ATL pubkeys resolved and returned.
        pub const LOAD_ADDRESS_LOOKUP_TABLES: u16 = 1 << 3;

        /// Check this batch as replay work.
        ///
        /// Replay checks use the existing [`crate::PackToWorkerMessage`] path
        /// to reuse external consume workers. The message's `max_working_slot`
        /// is the exact replay bank slot to check against.
        pub const REPLAY: u16 = 1 << 4;

        /// Transaction signatures should be verified.
        pub const VERIFY_SIGNATURES: u16 = 1 << 5;

        /// Estimated cost metadata should be returned for transactions that
        /// successfully parse and resolve.
        pub const ESTIMATE_COST: u16 = 1 << 6;
    }
}

pub mod processed_codes {
    /// The message was processed.
    pub const PROCESSED: u8 = 0;
    /// The message was not processed because the message was invalid.
    pub const INVALID: u8 = 1;
    /// The message was not processed because `max_working_slot`
    /// was exceeded.
    pub const MAX_WORKING_SLOT_EXCEEDED: u8 = 2;
    /// The message was not processed because the requested bank was not available.
    pub const BANK_NOT_AVAILABLE: u8 = 3;
}

/// Message: [Worker -> Pack]
/// Message from worker threads in response to a [`PackToWorkerMessage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct WorkerToPackMessage {
    /// Offset and number of transactions in the batch.
    /// See [`SharableTransactionBatchRegion`] for details.
    /// Once the external pack process receives this message,
    /// it is responsible for freeing the memory for this batch,
    /// and is safe to do so - agave will hold no references to this memory
    /// after sending this message.
    pub batch: SharableTransactionBatchRegion,
    /// See [`processed_codes`] for accepted values.
    pub processed_code: u8,
    /// Response per transaction in the batch.
    /// If message was not processed, this field is undefined.
    /// See [`TransactionResponseRegion`] for details.
    pub responses: TransactionResponseRegion,
}

pub mod worker_message_types {
    use crate::SharablePubkeys;

    /// Tag indicating [`ExecutionResponse`] inner message.
    pub const EXECUTION_RESPONSE: u8 = 0;

    /// Response to pack for a transaction that attempted execution.
    /// This response will only be sent if the original message flags
    /// requested execution i.e. not [`super::pack_message_flags::RESOLVE`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(C)]
    pub struct ExecutionResponse {
        /// The slot this transaction was executed.
        ///
        /// # Note
        ///
        /// The current semantics are:
        ///
        /// - If we successfully get a bank and try your request, this slot is the latest
        ///   bank we attempted (during a slot roll we can try 2 banks ).
        /// - If we do not attempt or attemp and fail to get a bank, this field will be
        ///   zero.
        pub execution_slot: u64,
        /// Indicates if the transaction was included in the block or not.
        /// If [`not_included_reasons::NONE`], the transaction was included.
        pub not_included_reason: u8,
        /// If included, cost units used by the transaction.
        pub cost_units: u64,
        /// If included, the fee-payer balance after execution.
        pub fee_payer_balance: u64,
    }

    pub mod not_included_reasons {
        /// The transaction was included in the block.
        pub const NONE: u8 = 0;
        /// The transaction could not attempt processing because the
        /// working bank was unavailable.
        pub const BANK_NOT_AVAILABLE: u8 = 1;

        /// Transaction dropped because the batch was marked as
        /// all_or_nothing and a different transacation failed.
        pub const ALL_OR_NOTHING_BATCH_FAILURE: u8 = 3;

        // Remaining errors are translations from SDK.
        // Moved up to 64 so we have room to add custom reasons in the future.
        // Also allows for easy distinguishing between custom scheduling errors
        // and sdk errors.

        /// An account is already being processed in another transaction in a way
        /// that does not support parallelism
        pub const ACCOUNT_IN_USE: u8 = 64;
        /// A `Pubkey` appears twice in the transaction's `account_keys`.  Instructions can reference
        /// `Pubkey`s more than once but the message must contain a list with no duplicate keys
        pub const ACCOUNT_LOADED_TWICE: u8 = 65;
        /// Attempt to debit an account but found no record of a prior credit.
        pub const ACCOUNT_NOT_FOUND: u8 = 66;
        /// Attempt to load a program that does not exist
        pub const PROGRAM_ACCOUNT_NOT_FOUND: u8 = 67;
        /// The from `Pubkey` does not have sufficient balance to pay the fee to schedule the transaction
        pub const INSUFFICIENT_FUNDS_FOR_FEE: u8 = 68;
        /// This account may not be used to pay transaction fees
        pub const INVALID_ACCOUNT_FOR_FEE: u8 = 69;
        /// The bank has seen this transaction before. This can occur under normal operation
        pub const ALREADY_PROCESSED: u8 = 70;
        /// The bank has not seen the given `recent_blockhash`
        pub const BLOCKHASH_NOT_FOUND: u8 = 71;
        /// An error occurred while processing an instruction.
        pub const INSTRUCTION_ERROR: u8 = 72;
        /// Loader call chain is too deep
        pub const CALL_CHAIN_TOO_DEEP: u8 = 73;
        /// Transaction requires a fee but has no signature present
        pub const MISSING_SIGNATURE_FOR_FEE: u8 = 74;
        /// Transaction contains an invalid account reference
        pub const INVALID_ACCOUNT_INDEX: u8 = 75;
        /// Transaction did not pass signature verification
        pub const SIGNATURE_FAILURE: u8 = 76;
        /// This program may not be used for executing instructions
        pub const INVALID_PROGRAM_FOR_EXECUTION: u8 = 77;
        /// Transaction failed to sanitize accounts offsets correctly
        pub const SANITIZE_FAILURE: u8 = 78;
        pub const CLUSTER_MAINTENANCE: u8 = 79;
        /// Transaction processing left an account with an outstanding borrowed reference
        pub const ACCOUNT_BORROW_OUTSTANDING: u8 = 80;
        /// Transaction would exceed max Block Cost Limit
        pub const WOULD_EXCEED_MAX_BLOCK_COST_LIMIT: u8 = 81;
        /// Transaction version is unsupported
        pub const UNSUPPORTED_VERSION: u8 = 82;
        /// Transaction loads a writable account that cannot be written
        pub const INVALID_WRITABLE_ACCOUNT: u8 = 83;
        /// Transaction would exceed max account limit within the block
        pub const WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT: u8 = 84;
        /// Transaction would exceed account data limit within the block
        pub const WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT: u8 = 85;
        /// Transaction locked too many accounts
        pub const TOO_MANY_ACCOUNT_LOCKS: u8 = 86;
        /// Address lookup table not found
        pub const ADDRESS_LOOKUP_TABLE_NOT_FOUND: u8 = 87;
        /// Attempted to lookup addresses from an account owned by the wrong program
        pub const INVALID_ADDRESS_LOOKUP_TABLE_OWNER: u8 = 88;
        /// Attempted to lookup addresses from an invalid account
        pub const INVALID_ADDRESS_LOOKUP_TABLE_DATA: u8 = 89;
        /// Address table lookup uses an invalid index
        pub const INVALID_ADDRESS_LOOKUP_TABLE_INDEX: u8 = 90;
        /// Transaction leaves an account with a lower balance than rent-exempt minimum
        pub const INVALID_RENT_PAYING_ACCOUNT: u8 = 91;
        /// Transaction would exceed max Vote Cost Limit
        pub const WOULD_EXCEED_MAX_VOTE_COST_LIMIT: u8 = 92;
        /// Transaction would exceed total account data limit
        pub const WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT: u8 = 93;
        /// Transaction contains a duplicate instruction that is not allowed
        pub const DUPLICATE_INSTRUCTION: u8 = 94;
        /// Transaction results in an account with insufficient funds for rent
        pub const INSUFFICIENT_FUNDS_FOR_RENT: u8 = 95;
        /// Transaction exceeded max loaded accounts data size cap
        pub const MAX_LOADED_ACCOUNTS_DATA_SIZE_EXCEEDED: u8 = 96;
        /// LoadedAccountsDataSizeLimit set for transaction must be greater than 0.
        pub const INVALID_LOADED_ACCOUNTS_DATA_SIZE_LIMIT: u8 = 97;
        /// Sanitized transaction differed before/after feature activation. Needs to be resanitized.
        pub const RESANITIZATION_NEEDED: u8 = 98;
        /// Program execution is temporarily restricted on an account.
        pub const PROGRAM_EXECUTION_TEMPORARILY_RESTRICTED: u8 = 99;
        /// The total balance before the transaction does not equal the total balance after the transaction
        pub const UNBALANCED_TRANSACTION: u8 = 100;
        /// Program cache hit max limit.
        pub const PROGRAM_CACHE_HIT_MAX_LIMIT: u8 = 101;

        // This error in agave is only internal, and to avoid updating the sdk
        // it is reused for mapping into `ALL_OR_NOTHING_BATCH_FAILURE`.
        // /// Commit cancelled internally.
        // pub const COMMIT_CANCELLED: u8 = 102;
    }

    /// Tag indicating [`CheckResponse`] inner message.
    pub const CHECK_RESPONSE: u8 = 1;

    pub mod parsing_and_sanitization_flags {
        /// Flag set if parsing and sanitization failed.
        pub const FAILED: u8 = 1 << 0;
    }

    pub mod status_check_flags {
        /// Flag set if status checks were requested.
        pub const REQUESTED: u8 = 1 << 0;
        /// Flag set if status checks were performed. A previous failure
        /// could have caused checks to be skipped.
        pub const PERFORMED: u8 = 1 << 1;
        /// Flag set if status checks failed due to the transaction being
        /// too old.
        pub const TOO_OLD: u8 = 1 << 2;
        /// Flag set if status checks failed due to the transaction already
        /// being processed.
        pub const ALREADY_PROCESSED: u8 = 1 << 3;
        /// Flag set if status checks failed due to an invalid nonce state.
        pub const INVALID_NONCE: u8 = 1 << 4;
        /// Flag set if status checks failed due to unsupported version of
        /// transaction was received.
        pub const UNSUPPORTED_VERSION: u8 = 1 << 5;
    }

    pub mod fee_payer_balance_flags {
        /// Flag set if fee-payer balance was requested.
        pub const REQUESTED: u8 = 1 << 0;
        /// Flag set if fee-payer balance fetching was performed. A previous
        /// failure could have caused balance fetching to be skipped.
        pub const PERFORMED: u8 = 1 << 1;
    }

    pub mod resolve_flags {
        /// Flag set if resolving pubkeys was requested.
        pub const REQUESTED: u8 = 1 << 0;
        /// Flag set if resolving pubkeys was performed.
        pub const PERFORMED: u8 = 1 << 1;
        /// Flag set if resolving failed.
        pub const FAILED: u8 = 1 << 2;
    }

    pub mod signature_verification_flags {
        /// Flag set if signature verification was requested.
        pub const REQUESTED: u8 = 1 << 0;
        /// Flag set if signature verification was performed. A previous
        /// failure could have caused verification to be skipped.
        pub const PERFORMED: u8 = 1 << 1;
        /// Flag set if signature verification failed.
        pub const FAILED: u8 = 1 << 2;
    }

    pub mod cost_model_flags {
        /// Flag set if estimated cost metadata was requested.
        pub const REQUESTED: u8 = 1 << 0;
        /// Flag set if estimated cost calculation was performed.
        pub const PERFORMED: u8 = 1 << 1;
        /// Flag set if the estimated cost should be tracked as a simple vote.
        pub const TRACK_AS_SIMPLE_VOTE: u8 = 1 << 2;
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    #[repr(C)]
    pub struct CheckResponse {
        /// See [`parsing_and_sanitization_flags`] for details.
        pub parsing_and_sanitization_flags: u8,
        /// See [`status_check_flags`] for details.
        pub status_check_flags: u8,
        /// See [`fee_payer_balance_flags`] for details.
        pub fee_payer_balance_flags: u8,
        /// See [`resolve_flags`] for details.
        pub resolve_flags: u8,
        /// See [`signature_verification_flags`] for details.
        pub signature_verification_flags: u8,
        /// See [`cost_model_flags`] for details.
        pub cost_model_flags: u8,

        /// If [`status_check_flags::ALREADY_PROCESSED`] is set,
        /// this is the slot the transaction was previously included in.
        /// Otherwise the value is undefined.
        pub included_slot: u64,

        /// Set only if [`fee_payer_balance_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// The slot of the bank used to fetch fee-payer balance.
        pub balance_slot: u64,
        /// Set only if [`fee_payer_balance_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// The balance of the fee-payer.
        pub fee_payer_balance: u64,

        /// The slot of the bank used for transaction resolution, cost model
        /// metadata, and writable account bitfields.
        pub resolution_slot: u64,
        /// Set only if [`resolve_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// Minimum deactivation slot of any ALT if any.
        /// u64::MAX if no ALTs or deactivation.
        pub min_alt_deactivation_slot: u64,
        /// Set only if [`resolve_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// Resolved pubkeys - writable then readonly.
        /// Freeing this memory is the responsibility of the external
        /// pack process.
        pub resolved_pubkeys: SharablePubkeys,

        /// Set only if [`cost_model_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// Calculated using the bank identified by `resolution_slot`.
        pub estimated_cost_units: u64,
        /// Set only if [`cost_model_flags::PERFORMED`] is set,
        /// otherwise the value is undefined.
        /// Calculated using the bank identified by `resolution_slot`.
        pub allocated_accounts_data_size: u64,
        /// Set only if transaction parsing and resolution succeeded,
        /// otherwise the value is undefined.
        /// Derived from the transaction resolved against the bank identified
        /// by `resolution_slot`.
        /// Bit N in element M corresponds to account key index M * 64 + N.
        pub writable_account_bitfields: [u64; 4],
    }
}

const _: () = assert!(
    MAX_TRANSACTIONS_PER_MESSAGE * core::mem::size_of::<worker_message_types::CheckResponse>()
        <= 4096
);
const _: () = assert!(
    MAX_TRANSACTIONS_PER_MESSAGE * core::mem::size_of::<worker_message_types::ExecutionResponse>()
        <= 4096
);
