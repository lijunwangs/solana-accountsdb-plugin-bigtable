use {
    crate::{
        bigtable_client::bigtable_client_account::{DbAccountInfo, ReadableAccountInfo},
        convert::accounts,
        parallel_bigtable_client::BufferedBigtableClient,
    },
    chrono::Utc,
    log::*,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, ReplicaAccountInfo,
    },
    solana_measure::measure::Measure,
    solana_metrics::*,
    solana_sdk::pubkey::Pubkey,
};

impl BufferedBigtableClient {
    /// Update or insert a single account
    pub async fn update_account(
        &self,
        account: &DbAccountInfo,
        is_startup: bool,
    ) -> Result<(), GeyserPluginError> {
        let client = self.client.lock().unwrap();
        let account_cells = [(
            Pubkey::new(account.pubkey()).to_string(),
            accounts::Account::from(account),
        )];
        let result = client
            .client
            .put_protobuf_cells_with_retry::<accounts::Account>("account", &account_cells)
            .await;
        match result {
            Ok(_size) => Ok(()),
            Err(err) => {
                error!("Error persisting into the database: {}", err);
                Err(GeyserPluginError::Custom(Box::new(err)))
            }
        }
    }
}
