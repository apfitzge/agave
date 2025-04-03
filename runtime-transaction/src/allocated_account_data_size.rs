use {
    solana_program::{program_utils::limited_deserialize, system_instruction::SystemInstruction},
    solana_pubkey::Pubkey,
    solana_sdk_ids::system_program,
    solana_svm_transaction::instruction::SVMInstruction,
    solana_system_interface::{
        MAX_PERMITTED_ACCOUNTS_DATA_ALLOCATIONS_PER_TRANSACTION, MAX_PERMITTED_DATA_LENGTH,
    },
    std::num::Saturating,
};

#[derive(Default)]
pub struct AllocatedAccountsDataSizeBuilder {
    tx_attempted_allocation_size: Saturating<u64>,
    failed: bool,
}

#[derive(Debug, PartialEq)]
enum SystemProgramAccountAllocation {
    None,
    Some(u64),
    Failed,
}

impl AllocatedAccountsDataSizeBuilder {
    pub fn process_instruction(&mut self, program_id: &Pubkey, instruction: &SVMInstruction) {
        if self.failed {
            return;
        }
        match Self::calculate_account_data_size_on_instruction(program_id, instruction) {
            SystemProgramAccountAllocation::Failed => {
                // If any system program instructions can be statically
                // determined to fail, no allocations will actually be
                // persisted by the transaction. So return 0 here so that no
                // account allocation budget is used for this failed
                // transaction.
                self.failed = true;
            }
            SystemProgramAccountAllocation::None => {}
            SystemProgramAccountAllocation::Some(ix_attempted_allocation_size) => {
                self.tx_attempted_allocation_size += ix_attempted_allocation_size;
            }
        }
    }

    pub fn build(self) -> u64 {
        if self.failed {
            0
        } else {
            self.tx_attempted_allocation_size
                .0
                .min(MAX_PERMITTED_ACCOUNTS_DATA_ALLOCATIONS_PER_TRANSACTION as u64)
        }
    }

    fn calculate_account_data_size_on_instruction(
        program_id: &Pubkey,
        instruction: &SVMInstruction,
    ) -> SystemProgramAccountAllocation {
        if program_id == &system_program::id() {
            if let Ok(instruction) = limited_deserialize(instruction.data, 1232) {
                Self::calculate_account_data_size_on_deserialized_system_instruction(instruction)
            } else {
                SystemProgramAccountAllocation::Failed
            }
        } else {
            SystemProgramAccountAllocation::None
        }
    }

    fn calculate_account_data_size_on_deserialized_system_instruction(
        instruction: SystemInstruction,
    ) -> SystemProgramAccountAllocation {
        match instruction {
            SystemInstruction::CreateAccount { space, .. }
            | SystemInstruction::CreateAccountWithSeed { space, .. }
            | SystemInstruction::Allocate { space }
            | SystemInstruction::AllocateWithSeed { space, .. } => {
                if space > MAX_PERMITTED_DATA_LENGTH {
                    SystemProgramAccountAllocation::Failed
                } else {
                    SystemProgramAccountAllocation::Some(space)
                }
            }
            _ => SystemProgramAccountAllocation::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_instruction::Instruction,
        solana_message::Message,
        solana_program::system_instruction,
        solana_system_interface::MAX_PERMITTED_ACCOUNTS_DATA_ALLOCATIONS_PER_TRANSACTION,
        solana_transaction::{versioned::sanitized::SanitizedVersionedTransaction, Transaction},
    };

    #[test]
    fn test_calculate_allocated_accounts_data_size_no_allocation() {
        let transaction = Transaction::new_unsigned(Message::new(
            &[system_instruction::transfer(
                &Pubkey::new_unique(),
                &Pubkey::new_unique(),
                1,
            )],
            Some(&Pubkey::new_unique()),
        ));
        let sanitized_tx = SanitizedVersionedTransaction::try_new(transaction.into()).unwrap();

        let mut builder = AllocatedAccountsDataSizeBuilder::default();
        for (program_id, instruction) in sanitized_tx.get_message().program_instructions_iter() {
            builder.process_instruction(program_id, &instruction.into());
        }
        assert_eq!(builder.build(), 0);
    }

    #[test]
    fn test_calculate_allocated_accounts_data_size_multiple_allocations() {
        let space1 = 100;
        let space2 = 200;
        let transaction = Transaction::new_unsigned(Message::new(
            &[
                system_instruction::create_account(
                    &Pubkey::new_unique(),
                    &Pubkey::new_unique(),
                    1,
                    space1,
                    &Pubkey::new_unique(),
                ),
                system_instruction::allocate(&Pubkey::new_unique(), space2),
            ],
            Some(&Pubkey::new_unique()),
        ));
        let sanitized_tx = SanitizedVersionedTransaction::try_new(transaction.into()).unwrap();

        let mut builder = AllocatedAccountsDataSizeBuilder::default();
        for (program_id, instruction) in sanitized_tx.get_message().program_instructions_iter() {
            builder.process_instruction(program_id, &instruction.into());
        }
        assert_eq!(builder.build(), space1 + space2,);
    }

    #[test]
    fn test_calculate_allocated_accounts_data_size_max_limit() {
        let spaces = [MAX_PERMITTED_DATA_LENGTH, MAX_PERMITTED_DATA_LENGTH, 100];
        assert!(
            spaces.iter().copied().sum::<u64>()
                > MAX_PERMITTED_ACCOUNTS_DATA_ALLOCATIONS_PER_TRANSACTION as u64
        );
        let transaction = Transaction::new_unsigned(Message::new(
            &[
                system_instruction::create_account(
                    &Pubkey::new_unique(),
                    &Pubkey::new_unique(),
                    1,
                    spaces[0],
                    &Pubkey::new_unique(),
                ),
                system_instruction::create_account(
                    &Pubkey::new_unique(),
                    &Pubkey::new_unique(),
                    1,
                    spaces[1],
                    &Pubkey::new_unique(),
                ),
                system_instruction::create_account(
                    &Pubkey::new_unique(),
                    &Pubkey::new_unique(),
                    1,
                    spaces[2],
                    &Pubkey::new_unique(),
                ),
            ],
            Some(&Pubkey::new_unique()),
        ));
        let sanitized_tx = SanitizedVersionedTransaction::try_new(transaction.into()).unwrap();

        let mut builder = AllocatedAccountsDataSizeBuilder::default();
        for (program_id, instruction) in sanitized_tx.get_message().program_instructions_iter() {
            builder.process_instruction(program_id, &instruction.into());
        }

        assert_eq!(
            builder.build(),
            MAX_PERMITTED_ACCOUNTS_DATA_ALLOCATIONS_PER_TRANSACTION as u64,
        );
    }

    #[test]
    fn test_calculate_allocated_accounts_data_size_overflow() {
        let transaction = Transaction::new_unsigned(Message::new(
            &[
                system_instruction::create_account(
                    &Pubkey::new_unique(),
                    &Pubkey::new_unique(),
                    1,
                    100,
                    &Pubkey::new_unique(),
                ),
                system_instruction::allocate(&Pubkey::new_unique(), u64::MAX),
            ],
            Some(&Pubkey::new_unique()),
        ));
        let sanitized_tx = SanitizedVersionedTransaction::try_new(transaction.into()).unwrap();

        let mut builder = AllocatedAccountsDataSizeBuilder::default();
        for (program_id, instruction) in sanitized_tx.get_message().program_instructions_iter() {
            builder.process_instruction(program_id, &instruction.into());
        }
        assert_eq!(
            0, // SystemProgramAccountAllocation::Failed,
            builder.build(),
        );
    }

    #[test]
    fn test_calculate_allocated_accounts_data_size_invalid_ix() {
        let transaction = Transaction::new_unsigned(Message::new(
            &[
                system_instruction::allocate(&Pubkey::new_unique(), 100),
                Instruction::new_with_bincode(system_program::id(), &(), vec![]),
            ],
            Some(&Pubkey::new_unique()),
        ));
        let sanitized_tx = SanitizedVersionedTransaction::try_new(transaction.into()).unwrap();

        let mut builder = AllocatedAccountsDataSizeBuilder::default();
        for (program_id, instruction) in sanitized_tx.get_message().program_instructions_iter() {
            builder.process_instruction(program_id, &instruction.into());
        }
        assert_eq!(
            0, // SystemProgramAccountAllocation::Failed,
            builder.build(),
        );
    }

    #[test]
    fn test_cost_model_data_len_cost() {
        let lamports = 0;
        let owner = Pubkey::default();
        let seed = String::default();
        let space = 100;
        let base = Pubkey::default();
        for instruction in [
            SystemInstruction::CreateAccount {
                lamports,
                space,
                owner,
            },
            SystemInstruction::CreateAccountWithSeed {
                base,
                seed: seed.clone(),
                lamports,
                space,
                owner,
            },
            SystemInstruction::Allocate { space },
            SystemInstruction::AllocateWithSeed {
                base,
                seed,
                space,
                owner,
            },
        ] {
            assert_eq!(
                SystemProgramAccountAllocation::Some(space),
                AllocatedAccountsDataSizeBuilder::calculate_account_data_size_on_deserialized_system_instruction(
                    instruction
                )
            );
        }
        assert_eq!(
            SystemProgramAccountAllocation::None,
            AllocatedAccountsDataSizeBuilder::calculate_account_data_size_on_deserialized_system_instruction(
                SystemInstruction::TransferWithSeed {
                    lamports,
                    from_seed: String::default(),
                    from_owner: Pubkey::default(),
                }
            )
        );
    }
}
