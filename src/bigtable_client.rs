mod bigtable_client_account;
mod bigtable_client_account_index;
mod bigtable_client_block_metadata;
mod bigtable_client_transaction;


use {
    crate::{
        accountsdb_plugin_bigtable::{AccountsDbPluginBigtableError, AccountsDbPluginBigtableConfig},
        bigtable::BigTableConnection as Client,
    },
    chrono::Utc,
    bigtable_client_account::{DbAccountInfo, ReadableAccountInfo},
    bigtable_client_account_index::TokenSecondaryIndexEntry,
    bigtable_client_block_metadata::UpdateBlockMetadataRequest,
    bigtable_client_transaction::LogTransactionRequest,
    log::*,
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        AccountsDbPluginError, SlotStatus,
    },
    solana_measure::measure::Measure,
    solana_metrics::*,
    std::{
        sync::{
            Arc, Mutex,
        },
        thread
    },
    tokio::runtime::Runtime,
};

/// The maximum asynchronous requests allowed in the channel to avoid excessive
/// memory usage. The downside -- calls after this threshold is reached can get blocked.
const MAX_ASYNC_REQUESTS: usize = 40960;
const DEFAULT_POSTGRES_PORT: u16 = 5432;
const DEFAULT_THREADS_COUNT: usize = 100;
const DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE: usize = 10;
const ACCOUNT_COLUMN_COUNT: usize = 9;
const DEFAULT_PANIC_ON_DB_ERROR: bool = false;
const DEFAULT_STORE_ACCOUNT_HISTORICAL_DATA: bool = false;

pub(crate) fn abort() -> ! {
    #[cfg(not(test))]
    {
        // standard error is usually redirected to a log file, cry for help on standard output as
        // well
        eprintln!("Validator process aborted. The validator log may contain further details");
        std::process::exit(1);
    }

    #[cfg(test)]
    panic!("process::exit(1) is intercepted for friendly test failure...");
}


pub trait BigtableClient {
    fn join(&mut self) -> thread::Result<()> {
        Ok(())
    }

    fn update_account(
        &mut self,
        account: DbAccountInfo,
        is_startup: bool,
    ) -> Result<(), AccountsDbPluginError>;

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<(), AccountsDbPluginError>;

    fn notify_end_of_startup(&mut self) -> Result<(), AccountsDbPluginError>;

    fn log_transaction(
        &mut self,
        transaction_log_info: LogTransactionRequest,
    ) -> Result<(), AccountsDbPluginError>;

    fn update_block_metadata(
        &mut self,
        block_info: UpdateBlockMetadataRequest,
    ) -> Result<(), AccountsDbPluginError>;
}

impl BigtableClient for SimpleBigtableClient {
    fn update_account(
        &mut self,
        account: DbAccountInfo,
        is_startup: bool,
    ) -> Result<(), AccountsDbPluginError> {
        trace!(
            "Updating account {} with owner {} at slot {}",
            bs58::encode(account.pubkey()).into_string(),
            bs58::encode(account.owner()).into_string(),
            account.slot,
        );
        if !is_startup {
            return self.upsert_account(&account);
        }
        self.insert_accounts_in_batch(account)
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<(), AccountsDbPluginError> {
        info!("Updating slot {:?} at with status {:?}", slot, status);

        let slot = slot as i64; // bigtable only supports i64
        let parent = parent.map(|parent| parent as i64);
        let updated_on = Utc::now().naive_utc();
        let status_str = status.as_str();
        let client = self.client.get_mut().unwrap();

        Ok(())
    }

    fn notify_end_of_startup(&mut self) -> Result<(), AccountsDbPluginError> {
        self.flush_buffered_writes()
    }

    fn log_transaction(
        &mut self,
        transaction_log_info: LogTransactionRequest,
    ) -> Result<(), AccountsDbPluginError> {
        self.log_transaction_impl(transaction_log_info)
    }

    fn update_block_metadata(
        &mut self,
        block_info: UpdateBlockMetadataRequest,
    ) -> Result<(), AccountsDbPluginError> {
        self.update_block_metadata_impl(block_info)
    }
}

struct BigtableClientWrapper {
    client: Client,
}

pub struct SimpleBigtableClient {
    batch_size: usize,
    pending_account_updates: Vec<DbAccountInfo>,
    index_token_owner: bool,
    index_token_mint: bool,
    pending_token_owner_index: Vec<TokenSecondaryIndexEntry>,
    pending_token_mint_index: Vec<TokenSecondaryIndexEntry>,
    client: Mutex<BigtableClientWrapper>,
}

impl SimpleBigtableClient {
    pub async fn connect_to_db(
        config: &AccountsDbPluginBigtableConfig,
    ) -> Result<Client, AccountsDbPluginError> {

        let result = Client::new("solana-accountsdb-plugin-bigtable", false, config.timeout, config.credential_path.clone()).await;

        match result {
            Ok(client) => {
                Ok(client)
            },
            Err(err) => {
                let msg = format!(
                    "Error in connecting to Bigtable \"credential_path\": {:?}, : {}",
                    config.credential_path, err
                );
                Err(AccountsDbPluginError::Custom(
                    Box::new(
                        AccountsDbPluginBigtableError::DataStoreConnectionError {
                            msg
                        },
                )))
            }
        }
    }

    /// Update or insert a single account
    fn upsert_account(&mut self, account: &DbAccountInfo) -> Result<(), AccountsDbPluginError> {
        Ok(())
    }

    /// Insert accounts in batch to reduce network overhead
    fn insert_accounts_in_batch(
        &mut self,
        account: DbAccountInfo,
    ) -> Result<(), AccountsDbPluginError> {
        Ok(())
    }

    fn bulk_insert_accounts(&mut self) -> Result<(), AccountsDbPluginError> {
        if self.pending_account_updates.len() == self.batch_size {
            let mut measure = Measure::start("accountsdb-plugin-bigtable-prepare-values");

            let mut values: Vec<&(dyn Sync)> =
                Vec::with_capacity(self.batch_size * ACCOUNT_COLUMN_COUNT);
            let updated_on = Utc::now().naive_utc();
            for j in 0..self.batch_size {
                let account = &self.pending_account_updates[j];

                values.push(&account.pubkey);
                values.push(&account.slot);
                values.push(&account.owner);
                values.push(&account.lamports);
                values.push(&account.executable);
                values.push(&account.rent_epoch);
                values.push(&account.data);
                values.push(&account.write_version);
                values.push(&updated_on);
            }
            measure.stop();
            inc_new_counter_debug!(
                "accountsdb-plugin-bigtable-prepare-values-us",
                measure.as_us() as usize,
                10000,
                10000
            );

            let mut measure = Measure::start("accountsdb-plugin-bigtable-update-account");
            let client = self.client.get_mut().unwrap();

            self.pending_account_updates.clear();

            measure.stop();
            inc_new_counter_debug!(
                "accountsdb-plugin-bigtable-update-account-us",
                measure.as_us() as usize,
                10000,
                10000
            );
            inc_new_counter_debug!(
                "accountsdb-plugin-bigtable-update-account-count",
                self.batch_size,
                10000,
                10000
            );
        }
        Ok(())
    }

    /// Flush any left over accounts in batch which are not processed in the last batch
    fn flush_buffered_writes(&mut self) -> Result<(), AccountsDbPluginError> {
        if self.pending_account_updates.is_empty() {
            return Ok(());
        }

        let client = self.client.get_mut().unwrap();

        Ok(())
    }

    pub async fn new(config: &AccountsDbPluginBigtableConfig) -> Result<Self, AccountsDbPluginError> {
        info!("Creating SimpleBigtableClient...");
        let client = Self::connect_to_db(config).await?;

        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE);

        let store_account_historical_data = config
            .store_account_historical_data
            .unwrap_or(DEFAULT_STORE_ACCOUNT_HISTORICAL_DATA);

        info!("Created SimpleBigtableClient.");
        Ok(Self {
            batch_size,
            pending_account_updates: Vec::with_capacity(batch_size),
            client: Mutex::new(BigtableClientWrapper {
                client,
            }),
            index_token_owner: config.index_token_owner.unwrap_or_default(),
            index_token_mint: config.index_token_mint.unwrap_or(false),
            pending_token_owner_index: Vec::with_capacity(batch_size),
            pending_token_mint_index: Vec::with_capacity(batch_size),
        })
    }
}

pub struct AsyncBigtableClient {
    client: SimpleBigtableClient,
    runtime: Arc<Runtime>,
}

const DEFAULT_THREADS: usize = 10;

impl AsyncBigtableClient {
    pub fn new(config: &AccountsDbPluginBigtableConfig) -> Result<Self, AccountsDbPluginError> {

        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(config.threads.unwrap_or(DEFAULT_THREADS))
                .thread_name("sol-acountsdb-plugin-bigtable")
                .enable_all()
                .build()
                .expect("Runtime"),
        );

        let client = runtime
            .block_on(SimpleBigtableClient::new(config));

        match client {
            Ok(client) => {
                Ok(Self{client, runtime})
            },
            Err(err) => {
                Err(err)
            }
        }
    }

    pub fn join(&self) {

    }
}
