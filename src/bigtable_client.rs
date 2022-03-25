mod bigtable_client_account;
mod bigtable_client_account_index;
mod bigtable_client_block_metadata;
mod bigtable_client_transaction;

use {
    crate::{
        geyser_plugin_bigtable::{
            GeyserPluginBigtableConfig, GeyserPluginBigtableError,
        },
        bigtable::BigTableConnection as Client,
    },
    log::*,
    solana_geyser_plugin_interface::geyser_plugin_interface::GeyserPluginError,
    std::sync::{Arc, Mutex},
    tokio::runtime::Runtime,
};

/// The maximum asynchronous requests allowed in the channel to avoid excessive
/// memory usage. The downside -- calls after this threshold is reached can get blocked.
const DEFAULT_THREADS_COUNT: usize = 100;
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

struct BigtableClientWrapper {
    client: Client,
}

pub struct SimpleBigtableClient {
    index_token_owner: bool,
    index_token_mint: bool,
    store_account_historical_data: bool,
    client: Mutex<BigtableClientWrapper>,
}

const DEFAULT_BIGTABLE_INSTANCE: &str = "solana-geyser-plugin-bigtable";

impl SimpleBigtableClient {
    pub async fn connect_to_db(
        config: &GeyserPluginBigtableConfig,
    ) -> Result<Client, GeyserPluginError> {
        let result = Client::new(
            config.instance.as_ref().unwrap_or(&DEFAULT_BIGTABLE_INSTANCE.to_string()),
            false,
            config.timeout,
            config.credential_path.clone(),
        )
        .await;

        match result {
            Ok(client) => Ok(client),
            Err(err) => {
                let msg = format!(
                    "Error in connecting to Bigtable \"credential_path\": {:?}, : {}",
                    config.credential_path, err
                );
                Err(GeyserPluginError::Custom(Box::new(
                    GeyserPluginBigtableError::DataStoreConnectionError { msg },
                )))
            }
        }
    }

    pub async fn new(
        config: &GeyserPluginBigtableConfig,
    ) -> Result<Self, GeyserPluginError> {
        info!("Creating SimpleBigtableClient...");
        let client = Self::connect_to_db(config).await?;

        let store_account_historical_data = config
            .store_account_historical_data
            .unwrap_or(DEFAULT_STORE_ACCOUNT_HISTORICAL_DATA);

        info!("Created SimpleBigtableClient.");
        Ok(Self {
            client: Mutex::new(BigtableClientWrapper { client }),
            index_token_owner: config.index_token_owner.unwrap_or_default(),
            index_token_mint: config.index_token_mint.unwrap_or(false),
            store_account_historical_data,
        })
    }
}

pub struct AsyncBigtableClient {
    client: SimpleBigtableClient,
    runtime: Arc<Runtime>,
}

impl AsyncBigtableClient {
    pub fn new(config: &GeyserPluginBigtableConfig) -> Result<Self, GeyserPluginError> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(config.threads.unwrap_or(DEFAULT_THREADS_COUNT))
                .thread_name("sol-acountsdb-plugin-bigtable")
                .enable_all()
                .build()
                .expect("Runtime"),
        );

        let client = runtime.block_on(SimpleBigtableClient::new(config));

        match client {
            Ok(client) => Ok(Self { client, runtime }),
            Err(err) => Err(err),
        }
    }

    pub fn join(&self) {}
}
