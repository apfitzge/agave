use {
    crate::{
        account_loader::{AccountLoader, TransactionLoadResult},
        rollback_accounts::RollbackAccounts,
        transaction_processing_callback::TransactionProcessingCallback,
        transaction_processing_result::ProcessedTransaction,
    },
    solana_account::AccountSharedData,
    solana_pubkey::Pubkey,
    solana_svm_transaction::svm_transaction::SVMTransaction,
    solana_transaction_error::TransactionError,
};

pub trait LoadedAccountInspector {
    fn inspect_account<CB: TransactionProcessingCallback>(
        &mut self,
        _account_loader: &mut AccountLoader<CB>,
        _pubkey: &Pubkey,
        _account: &AccountSharedData,
        _pre_execution: bool,
    ) {
        // Default implementation does nothing.
    }
}

pub(crate) fn inspect_loaded_accounts<CB: TransactionProcessingCallback>(
    account_inspector: &mut impl LoadedAccountInspector,
    account_loader: &mut AccountLoader<CB>,
    // needed only to get the fee-payer pubkey since it is not returned by the rollback accts.
    transaction: &impl SVMTransaction,
    load_result: &TransactionLoadResult,
) {
    match load_result {
        TransactionLoadResult::Loaded(loaded_transaction) => {
            for (pubkey, account) in loaded_transaction.accounts.iter() {
                account_inspector.inspect_account(account_loader, pubkey, account, true);
            }
        }
        TransactionLoadResult::FeesOnly(fees_only_transaction) => {
            match &fees_only_transaction.rollback_accounts {
                RollbackAccounts::FeePayerOnly { fee_payer_account } => {
                    account_inspector.inspect_account(
                        account_loader,
                        transaction.fee_payer(),
                        fee_payer_account,
                        true,
                    );
                }
                RollbackAccounts::SameNonceAndFeePayer { nonce } => {
                    account_inspector.inspect_account(
                        account_loader,
                        nonce.address(),
                        nonce.account(),
                        true,
                    );
                }
                RollbackAccounts::SeparateNonceAndFeePayer {
                    nonce,
                    fee_payer_account,
                } => {
                    // order here may matter? fee-payer is always first acct.
                    account_inspector.inspect_account(
                        account_loader,
                        transaction.fee_payer(),
                        fee_payer_account,
                        true,
                    );
                    account_inspector.inspect_account(
                        account_loader,
                        nonce.address(),
                        nonce.account(),
                        true,
                    );
                }
            }
        }
        TransactionLoadResult::NotLoaded(_transaction_error) => {
            // Do nothing since transaction was not loaded at all.
        }
    }
}

pub(crate) fn inspect_processed_accounts<CB: TransactionProcessingCallback>(
    account_inspector: &mut impl LoadedAccountInspector,
    account_loader: &mut AccountLoader<CB>,
    processed_transaction: &Result<ProcessedTransaction, TransactionError>,
) {
    match processed_transaction {
        Ok(processed_transaction) => {
            match processed_transaction.executed_transaction() {
                Some(executed_transaction) => {
                    for (pubkey, account) in executed_transaction.loaded_transaction.accounts.iter()
                    {
                        account_inspector.inspect_account(account_loader, pubkey, account, false);
                    }
                }
                None => {
                    // If not executed, no change in balances.
                }
            }
        }
        Err(_) => {
            // If not processes, no change in balances.
        }
    }
}
