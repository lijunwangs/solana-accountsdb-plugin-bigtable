use {
    super::account::{DbAccountInfo, ReadableAccountInfo},
    crate::parallel_bigtable_client::BufferedBigtableClient,
    log::*,
    prost::Message,
    solana_bigtable_geyser_models::models::accounts,
    solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError,
    solana_sdk::pubkey::Pubkey,
    std::mem,
};

#[derive(Default)]
struct SlotGraph {
    parent_map: std::collections::BTreeMap<u64, u64>,
}

impl SlotGraph {
    fn update_parent(&mut self, slot: u64, parent: u64) {
        self.parent_map
            .insert(slot, parent)
            .map(|prev| debug_assert_eq!(prev, parent));
    }

    fn extract_chain_of(&mut self, mut slot: u64) -> std::collections::HashSet<u64> {
        // Here we assume that all slots < `slot` will be removed, either being part
        // of `slot`'s chain or being on abandoned chains.
        let mut removed = self.parent_map.split_off(&(slot + 1));
        std::mem::swap(&mut removed, &mut self.parent_map);

        let mut result = std::collections::HashSet::from([slot]);
        for (current_slot, parent_slot) in removed.iter().rev() {
            if slot == *current_slot {
                result.insert(*parent_slot);
                slot = *parent_slot;
            }
        }
        result
    }
}

#[derive(Default)]
pub struct AccountsHistoryBatcher {
    updates: Vec<DbAccountInfo>,
    slot_graph: SlotGraph,
}

impl AccountsHistoryBatcher {
    pub fn add(&mut self, value: DbAccountInfo) {
        self.updates.push(value);
    }

    pub fn update_slot_parent(&mut self, slot: u64, parent: u64) {
        self.slot_graph.update_parent(slot, parent);
    }

    pub fn flush<F, E>(&mut self, slot: u64, mut batch_cb: F) -> Result<(), E>
    where
        F: FnMut(Vec<DbAccountInfo>) -> Result<(), E>,
    {
        self.updates
            .sort_unstable_by(|a, b| Self::order_key(a).cmp(&Self::order_key(b)));

        let drain_end = self
            .updates
            .iter()
            .position(|u| u.slot > slot)
            .unwrap_or(self.updates.len());

        let mut send_nonempty = |batch: &mut Vec<DbAccountInfo>| {
            if !batch.is_empty() {
                batch_cb(mem::take(batch))?
            }
            Ok(())
        };

        let mut batch = vec![];
        let mut key = (0, vec![]);
        let notified_slots = self.slot_graph.extract_chain_of(slot);
        for item in self.updates.drain(..drain_end) {
            if !notified_slots.contains(&item.slot) {
                continue;
            }

            let item_key = Self::batch_key(&item);
            if !Self::is_same_batch(&key, &item_key) {
                send_nonempty(&mut batch)?;
                key = (item_key.0, item_key.1.clone());
            }
            batch.push(item);
        }
        send_nonempty(&mut batch)
    }

    fn order_key<'a>(acc: &'a DbAccountInfo) -> (u64, &'a Vec<u8>, u64) {
        (acc.slot, &acc.pubkey, acc.write_version)
    }

    fn batch_key<'a>(acc: &'a DbAccountInfo) -> (u64, &'a Vec<u8>) {
        (acc.slot, &acc.pubkey)
    }

    fn is_same_batch(current_key: &(u64, Vec<u8>), key: &(u64, &Vec<u8>)) -> bool {
        current_key.0 == key.0 && current_key.1 == *key.1
    }
}

fn as_account_batch_item(prev: &DbAccountInfo, next: &DbAccountInfo) -> accounts::Account {
    accounts::Account {
        data: next.data().to_vec(),
        lamports: next.lamports,
        // Immutable fields are skipped, monotonically increasing ones are stored as diffs
        rent_epoch: prev.rent_epoch - next.rent_epoch,
        slot: next.slot - prev.slot,
        write_version: next.write_version - prev.write_version,
        updated_on: Some(accounts::UnixTimestamp {
            timestamp: (next.updated_since_epoch - prev.updated_since_epoch).as_millis() as i64,
        }),
        ..accounts::Account::default()
    }
}

impl BufferedBigtableClient {
    pub async fn update_accounts_batch(
        &mut self,
        accounts: Vec<DbAccountInfo>,
    ) -> Result<(usize, usize), GeyserPluginError> {
        let (key, batch) = {
            let mut batch = accounts::AccountsBatch::default();
            let mut prev = accounts.first().unwrap();
            let key = format!(
                "{}/{:016X}/{:016X}",
                Pubkey::new(prev.pubkey()),
                !prev.slot,
                !prev.write_version
            );
            batch.accounts.push(prev.into());
            for next in accounts.iter().skip(1) {
                batch.accounts.push(as_account_batch_item(&prev, &next));
                prev = next;
            }
            (key, batch)
        };
        let raw_size = batch.encoded_len();
        let cells = vec![(key, batch)];

        let client = self.client.lock().unwrap();
        let result = client
            .client
            .put_protobuf_cells_with_retry("account_history", &cells, false)
            .await;
        match result {
            Ok(written_size) => Ok((written_size, raw_size)),
            Err(err) => {
                error!("Error persisting into the database: {}", err);
                let (key, batch) = cells.first().unwrap();
                error!(
                    "Error persisting into the database: pubkey: {}, len: {} ",
                    key,
                    batch.accounts.len()
                );
                Err(GeyserPluginError::Custom(Box::new(err)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{fmt::Error, time::Duration};

    use solana_sdk::pubkey::Pubkey;

    use crate::parallel_bigtable_client::account::DbAccountInfo;

    use super::AccountsHistoryBatcher;

    #[test]
    fn batcher() -> Result<(), Error> {
        let mut batcher = AccountsHistoryBatcher::default();
        batcher.add(example_acc(1, 10));
        batcher.add(example_acc(2, 10));
        batcher.add(example_acc(2, 11));
        batcher.add(example_acc(1, 11));
        batcher.add(example_acc(3, 11));
        batcher.add(example_acc(1, 11));

        let mut bs = vec![];
        batcher.flush::<_, Error>(9, |b| Ok(bs.push(b)))?;
        assert_eq!(0, bs.len());

        batcher.flush::<_, Error>(11, |b| Ok(bs.push(b)))?;
        assert_eq!(3, bs.len());
        assert_eq!(2, bs[0].len());
        bs[0].iter().for_each(|item| {
            assert_eq!(11, item.slot);
            assert_eq!(1, item.pubkey[0]);
        });

        assert_eq!(1, bs[1].len());
        assert_eq!(11, bs[1][0].slot);
        assert_eq!(2, bs[1][0].pubkey[0]);

        assert_eq!(1, bs[2].len());
        assert_eq!(11, bs[2][0].slot);
        assert_eq!(3, bs[2][0].pubkey[0]);

        Ok(())
    }

    #[test]
    fn skipped_slot() {
        let mut batcher = AccountsHistoryBatcher::default();
        batcher.add(example_acc(1, 10));
        batcher.add(example_acc(2, 10));
        batcher.update_slot_parent(11, 10);
        batcher.add(example_acc(2, 11));
        batcher.add(example_acc(1, 11));
        // New fork, slot 11 will be abandoned
        batcher.update_slot_parent(12, 10);
        batcher.add(example_acc(3, 12));
        batcher.add(example_acc(1, 12));

        let mut bs = vec![];
        batcher
            .flush::<_, Error>(12, |b| Ok(bs.push(b)))
            .expect("flush");
        assert_eq!(4, bs.len());

        assert_eq!(1, bs[0].len());
        assert_eq!(10, bs[0][0].slot);
        assert_eq!(1, bs[0][0].pubkey[0]);

        assert_eq!(1, bs[1].len());
        assert_eq!(10, bs[1][0].slot);
        assert_eq!(2, bs[1][0].pubkey[0]);

        assert_eq!(1, bs[2].len());
        assert_eq!(12, bs[2][0].slot);
        assert_eq!(1, bs[2][0].pubkey[0]);

        assert_eq!(1, bs[3].len());
        assert_eq!(12, bs[3][0].slot);
        assert_eq!(3, bs[3][0].pubkey[0]);
    }

    fn example_acc(addr: u8, slot: u64) -> DbAccountInfo {
        DbAccountInfo {
            pubkey: Pubkey::new_from_array([addr; 32]).to_bytes().to_vec(),
            lamports: 123,
            owner: Pubkey::new_from_array([!addr; 32]).to_bytes().to_vec(),
            executable: true,
            rent_epoch: 10,
            data: vec![1, 2, 3, 4, 5, 6],
            slot: slot,
            write_version: 20000,
            updated_since_epoch: Duration::from_secs(12345),
        }
    }
}
