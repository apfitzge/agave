#![allow(clippy::arithmetic_side_effects)]
use {
    criterion::{Criterion, Throughput, criterion_group, criterion_main},
    solana_ledger::{
        broadcast_events::{frozen_bank_event, new_bank_event},
        genesis_utils::create_genesis_config,
    },
    solana_runtime::bank::{Bank, SlotLeader},
    std::hint::black_box,
};

fn bench_bank_events(c: &mut Criterion) {
    let genesis_config = create_genesis_config(1).genesis_config;
    let (root_bank, bank_forks) =
        Bank::new_for_benches(&genesis_config).wrap_with_bank_forks_for_tests();
    let parent_bank =
        Bank::new_from_parent_with_bank_forks(&bank_forks, root_bank, SlotLeader::default(), 1);
    let bank =
        Bank::new_from_parent_with_bank_forks(&bank_forks, parent_bank, SlotLeader::default(), 2);

    let mut group = c.benchmark_group("bank_events");
    group.throughput(Throughput::Elements(1));

    group.bench_function("new_bank_event", |b| {
        b.iter(|| black_box(new_bank_event(black_box(bank.as_ref()))));
    });

    group.bench_function("frozen_bank_event", |b| {
        b.iter(|| black_box(frozen_bank_event(black_box(bank.as_ref()))));
    });

    group.finish();
}

criterion_group!(benches, bench_bank_events);
criterion_main!(benches);
