pub use generated::agave::ledger::broadcast_events::{
    BankEvent, BankEventKind, FrozenBankEvent, NewBankEvent,
};
use {generated::agave::ledger::broadcast_events as generated_events, solana_runtime::bank::Bank};

mod generated {
    #![allow(clippy::all)]
    #![allow(dead_code)]
    #![allow(missing_docs)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(non_upper_case_globals)]
    #![allow(unsafe_op_in_unsafe_fn)]

    include!("broadcast_events/generated/bank_events_generated.rs");
}

pub fn new_bank_event(bank: &Bank) -> BankEvent {
    let parent_hash = generated_events::Hash::new(&bank.parent_hash().to_bytes());
    let new_bank = NewBankEvent::new(bank.slot(), bank.parent_slot(), &parent_hash);
    BankEvent::new(
        BankEventKind::NewBank,
        &new_bank,
        &FrozenBankEvent::default(),
    )
}

pub fn frozen_bank_event(bank: &Bank) -> BankEvent {
    let bank_hash = generated_events::Hash::new(&bank.hash().to_bytes());
    let frozen_bank = FrozenBankEvent::new(bank.slot(), &bank_hash);
    BankEvent::new(
        BankEventKind::FrozenBank,
        &NewBankEvent::default(),
        &frozen_bank,
    )
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_runtime::{bank::Bank, genesis_utils::create_genesis_config},
    };

    #[test]
    fn constructs_bank_events() {
        let genesis_config = create_genesis_config(1).genesis_config;
        let bank = Bank::new_for_tests(&genesis_config);

        let new_bank = new_bank_event(&bank);
        assert_eq!(new_bank.kind(), BankEventKind::NewBank);
        assert_eq!(new_bank.new_bank().slot(), bank.slot());
        assert_eq!(new_bank.new_bank().parent_slot(), bank.parent_slot());

        bank.freeze();
        let frozen_bank = frozen_bank_event(&bank);
        assert_eq!(frozen_bank.kind(), BankEventKind::FrozenBank);
        assert_eq!(frozen_bank.frozen_bank().slot(), bank.slot());
    }
}
