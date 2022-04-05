use {
    crate::{
        bigtable_client::bigtable_client_account::{DbAccountInfo, ReadableAccountInfo},
        convert::accounts,
        parallel_bigtable_client::BufferedBigtableClient,
    },
    log::*,
    solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError,
    solana_sdk::pubkey::Pubkey,
};

impl BufferedBigtableClient {
    /// Update or insert a single account
    pub async fn update_account(
        &mut self,
        account: DbAccountInfo,
        is_startup: bool,
    ) -> Result<(), GeyserPluginError> {
        let account_cells = if is_startup {
            self.slots_at_startup.insert(account.slot);
            self.pending_account_updates.push(account);

            if self.pending_account_updates.len() == self.batch_size {
                self.pending_account_updates
                    .drain(..)
                    .map(|account| {
                        (
                            Pubkey::new(account.pubkey()).to_string(),
                            accounts::Account::from(&account),
                        )
                    })
                    .collect()
            } else {
                return Ok(());
            }
        } else {
            vec![(
                Pubkey::new(account.pubkey()).to_string(),
                accounts::Account::from(&account),
            )]
        };

        let client = self.client.lock().unwrap();
        let result = client
            .client
            .put_protobuf_cells_with_retry::<accounts::Account>("account", &account_cells, true)
            .await;
        match result {
            Ok(_size) => Ok(()),
            Err(err) => {
                error!("Error persisting into the database: {}", err);
                for (key, account) in account_cells.iter() {
                    error!("Error persisting into the database: pubkey: {}, len: {} ", key, account.data.len());
                }
                Err(GeyserPluginError::Custom(Box::new(err)))
            }
        }
    }
}
