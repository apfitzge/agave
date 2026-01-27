use {
    crate::banking_stage::LikeClusterInfo,
    solana_clock::{FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET, NUM_CONSECUTIVE_LEADER_SLOTS},
    solana_gossip::contact_info::ContactInfoQuery,
    solana_poh::poh_recorder::PohRecorder,
    std::{net::SocketAddr, sync::RwLock},
};

pub(crate) fn next_leaders(
    cluster_info: &impl LikeClusterInfo,
    poh_recorder: &RwLock<PohRecorder>,
    max_count: u64,
    port_selector: impl ContactInfoQuery<Option<SocketAddr>>,
) -> Vec<SocketAddr> {
    let recorder = poh_recorder.read().unwrap();
    let leader_pubkeys: Vec<_> = (0..max_count)
        .filter_map(|i| {
            recorder.leader_after_n_slots(
                FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET + i * NUM_CONSECUTIVE_LEADER_SLOTS,
            )
        })
        .collect();
    drop(recorder);

    leader_pubkeys
        .iter()
        .filter_map(|leader_pubkey| {
            cluster_info.lookup_contact_info(leader_pubkey, &port_selector)?
        })
        .collect()
}
