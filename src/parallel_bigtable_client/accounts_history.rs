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

    pub fn flush<F, E>(&mut self, slot: u64, batch_cb: F) -> Result<(), E>
    where
        F: Fn(Vec<DbAccountInfo>) -> Result<(), E>,
    {
        if slot < self.updates.first().map_or(u64::MAX, |acc| acc.slot) {
            return Ok(());
        }

        self.updates
            .sort_unstable_by(|a, b| Self::order_key(a).cmp(&Self::order_key(b)));

        let mut batch = vec![];
        let mut key = (0, vec![]);
        for item in mem::take(&mut self.updates).into_iter() {
            let item_key = Self::batch_key(&item);
            if Self::is_same_batch(&key, &item_key) {
                batch.push(item);
            } else {
                if !batch.is_empty() {
                    batch_cb(mem::take(&mut batch))?;
                }
                key = (item_key.0, item_key.1.clone());
                batch.push(item);
            }
        }
        batch_cb(batch)
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
            if let Some(mut prev) = accounts.first() {
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
            } else {
                return Err(GeyserPluginError::AccountsUpdateError {
                    msg: "internal error, empty batch".into(),
                });
            }
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
