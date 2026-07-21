#![cfg(target_os = "linux")]
//! PoH-backed load-test infrastructure for block-production schedulers.

mod harness;
mod scenario;

pub use {
    harness::{
        GreedyHarness, GreedyTpuInjector, GreedyTpuInjectorError, Harness, HarnessConfig,
        HarnessError, TpuInjector, TpuInjectorError,
    },
    scenario::{LoadTestScenario, TransferScenario, run_scenario},
};
use {
    solana_runtime::bank::Bank,
    std::sync::{Arc, atomic::AtomicBool},
};

/// An ingress implementation used by [`run_scenario`].
pub trait TransactionInjector {
    type Error;

    /// Prepare to inject a batch of transactions.
    fn sync(&mut self);

    /// Inject one transaction, returning `false` when the injector is backpressured.
    fn try_push_transaction(&mut self, transaction: &[u8]) -> Result<bool, Self::Error>;

    /// Publish all transactions prepared since the preceding [`Self::sync`].
    fn commit(&mut self) -> Result<(), Self::Error>;
}

/// PoH-backed scheduler environment used by [`run_scenario`].
pub trait LoadTestHarness {
    type Injector: TransactionInjector;

    /// Return the direct post-sigverify transaction injector.
    fn injector(&mut self) -> &mut Self::Injector;

    /// Return the current working bank.
    fn working_bank(&self) -> Arc<Bank>;

    /// Return the shared exit signal used by the harness.
    fn exit_signal(&self) -> Arc<AtomicBool>;
}
