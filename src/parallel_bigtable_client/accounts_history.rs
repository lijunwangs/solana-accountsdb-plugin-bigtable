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
pub struct AccountsHistoryBatcher {
    updates: Vec<DbAccountInfo>,
}

impl AccountsHistoryBatcher {
    pub fn add(&mut self, value: DbAccountInfo) {
        self.updates.push(value);
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
        for item in self.updates.drain(..drain_end) {
            if item.slot < slot {
                // Items from slots lower than flushed are considered abandoned.
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
        let first_batch = bs.get(0).unwrap();
        assert_eq!(2, first_batch.len());
        assert_eq!(11, first_batch.get(0).unwrap().slot);
        assert_eq!(1, *first_batch.get(0).unwrap().pubkey.get(0).unwrap());

        let second_batch = bs.get(1).unwrap();
        assert_eq!(1, second_batch.len());
        assert_eq!(11, second_batch.get(0).unwrap().slot);
        assert_eq!(2, *second_batch.get(0).unwrap().pubkey.get(0).unwrap());
        Ok(())
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
