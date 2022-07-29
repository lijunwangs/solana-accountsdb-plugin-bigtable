pub mod account;
pub mod account_index;
pub mod block_metadata;
pub mod slot;
pub mod transaction;

use {
    crate::{
        geyser_plugin_bigtable::{GeyserPluginBigtableConfig, GeyserPluginBigtableError},
        parallel_bigtable_client::{
            account::{
                DbAccountInfo, ReadableAccountInfo, UpdateAccountRequest,
            },
            account_index::TokenSecondaryIndexEntry,
            block_metadata::{DbBlockInfo, UpdateBlockMetadataRequest},
            transaction::{build_db_transaction, LogTransactionRequest}
        },
    },
    crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender},
    log::*,
    solana_bigtable_connection::{bigtable::BigTableConnection as Client, CredentialType},
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, ReplicaAccountInfo, ReplicaBlockInfo, ReplicaTransactionInfo, SlotStatus,
    },    
    solana_measure::measure::Measure,
    solana_metrics::*,
    solana_sdk::timing::AtomicInterval,
    std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        thread::{self, sleep, Builder, JoinHandle},
        time::Duration,
    },
    tokio::runtime::Runtime,
};

pub fn abort() -> ! {
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

#[allow(unused_variables)]

/// The maximum asynchronous requests allowed in the channel to avoid excessive
/// memory usage. The downside -- calls after this threshold is reached can get blocked.
const MAX_ASYNC_REQUESTS: usize = 40960;
const DEFAULT_THREADS_COUNT: usize = 100;
const DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE: usize = 10;
const DEFAULT_PANIC_ON_DB_ERROR: bool = false;

/// The default bigtable instance name
pub const DEFAULT_BIGTABLE_INSTANCE: &str = "solana-geyser-plugin-bigtable";
pub const DEFAULT_APP_PROFILE_ID: &str = "";
pub const DEFAULT_STORE_ACCOUNT_HISTORICAL_DATA: bool = false;

struct UpdateSlotRequest {
    slot: u64,
    parent: Option<u64>,
    slot_status: SlotStatus,
}

#[warn(clippy::large_enum_variant)]
enum DbWorkItem {
    UpdateAccount(Box<UpdateAccountRequest>),
    UpdateSlot(Box<UpdateSlotRequest>),
    LogTransaction(Box<LogTransactionRequest>),
    UpdateBlockMetadata(Box<UpdateBlockMetadataRequest>),
}

struct BigtableClientWrapper {
    client: Client,
}
#[allow(dead_code)]
pub struct BufferedBigtableClient {
    client: Mutex<BigtableClientWrapper>,
    store_account_historical_data: bool,
    batch_size: usize,
    slots_at_startup: HashSet<u64>,
    pending_account_updates: Vec<DbAccountInfo>,
    index_token_owner: bool,
    index_token_mint: bool,
    pending_token_owner_index: Vec<TokenSecondaryIndexEntry>,
    pending_token_mint_index: Vec<TokenSecondaryIndexEntry>,
}

impl BufferedBigtableClient {
    pub async fn connect_to_db(
        config: &GeyserPluginBigtableConfig,
    ) -> Result<Client, GeyserPluginError> {
        let result = Client::new(
            config
                .instance
                .as_ref()
                .unwrap_or(&DEFAULT_BIGTABLE_INSTANCE.to_string()),
            config
                .app_profile_id
                .as_ref()
                .unwrap_or(&DEFAULT_APP_PROFILE_ID.to_string()),
            false,
            config.timeout,
            CredentialType::Filepath(config.credential_path.clone()),
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

    pub async fn new(config: &GeyserPluginBigtableConfig) -> Result<Self, GeyserPluginError> {
        info!("Creating SimpleBigtableClient...");
        let client = Self::connect_to_db(config).await?;

        let store_account_historical_data = config
            .store_account_historical_data
            .unwrap_or(DEFAULT_STORE_ACCOUNT_HISTORICAL_DATA);

        let batch_size = config
            .batch_size
            .unwrap_or(DEFAULT_ACCOUNTS_INSERT_BATCH_SIZE);

        info!("Created SimpleBigtableClient.");
        Ok(Self {
            client: Mutex::new(BigtableClientWrapper { client }),
            batch_size,
            pending_account_updates: Vec::with_capacity(batch_size),
            index_token_owner: config.index_token_owner.unwrap_or_default(),
            index_token_mint: config.index_token_mint.unwrap_or(false),
            store_account_historical_data,
            pending_token_owner_index: Vec::with_capacity(batch_size),
            pending_token_mint_index: Vec::with_capacity(batch_size),
            slots_at_startup: HashSet::default(),
        })
    }
}

struct BigtableClientWorker {
    client: BufferedBigtableClient,
    /// Indicating if accounts notification during startup is done.
    is_startup_done: bool,
    runtime: Arc<Runtime>,
}

impl BigtableClientWorker {
    fn new(
        config: GeyserPluginBigtableConfig,
        runtime: Arc<Runtime>,
    ) -> Result<Self, GeyserPluginError> {
        let result = runtime.block_on(BufferedBigtableClient::new(&config));
        match result {
            Ok(client) => Ok(BigtableClientWorker {
                client,
                is_startup_done: false,
                runtime,
            }),
            Err(err) => {
                error!("Error in creating SimpleBigtableClient: {}", err);
                Err(err)
            }
        }
    }

    fn update_account(
        &mut self,
        account: DbAccountInfo,
        is_startup: bool,
    ) -> Result<(), GeyserPluginError> {
        self.runtime
            .block_on(self.client.update_account(account, is_startup))
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<(), GeyserPluginError> {
        info!("Updating slot {:?} at with status {:?}", slot, status);
        self.runtime
            .block_on(self.client.update_slot(slot, parent, status.as_str()))
    }

    fn notify_end_of_startup(&mut self) -> Result<(), GeyserPluginError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn log_transaction(
        &mut self,
        transaction_log_info: LogTransactionRequest,
    ) -> Result<(), GeyserPluginError> {
        Ok(())
    }

    #[allow(unused_variables)]
    fn update_block_metadata(
        &mut self,
        block_info: UpdateBlockMetadataRequest,
    ) -> Result<(), GeyserPluginError> {
        Ok(())
    }

    fn do_work(
        &mut self,
        receiver: Receiver<DbWorkItem>,
        exit_worker: Arc<AtomicBool>,
        is_startup_done: Arc<AtomicBool>,
        startup_done_count: Arc<AtomicUsize>,
        panic_on_db_errors: bool,
    ) -> Result<(), GeyserPluginError> {
        while !exit_worker.load(Ordering::Relaxed) {
            let mut measure = Measure::start("geyser-plugin-bigtable-worker-recv");
            let work = receiver.recv_timeout(Duration::from_millis(500));
            measure.stop();
            inc_new_counter_debug!(
                "geyser-plugin-bigtable-worker-recv-us",
                measure.as_us() as usize,
                100000,
                100000
            );
            match work {
                Ok(work) => match work {
                    DbWorkItem::UpdateAccount(request) => {
                        if let Err(err) = self.update_account(request.account, request.is_startup) {
                            error!("Failed to update account: ({})", err);
                            if panic_on_db_errors {
                                abort();
                            }
                        }
                    }
                    DbWorkItem::UpdateSlot(request) => {
                        if let Err(err) = self.update_slot_status(
                            request.slot,
                            request.parent,
                            request.slot_status,
                        ) {
                            error!("Failed to update slot: ({})", err);
                            if panic_on_db_errors {
                                abort();
                            }
                        }
                    }
                    DbWorkItem::LogTransaction(transaction_log_info) => {
                        if let Err(err) = self.log_transaction(*transaction_log_info) {
                            error!("Failed to update transaction: ({})", err);
                            if panic_on_db_errors {
                                abort();
                            }
                        }
                    }
                    DbWorkItem::UpdateBlockMetadata(block_info) => {
                        if let Err(err) = self.update_block_metadata(*block_info) {
                            error!("Failed to update block metadata: ({})", err);
                            if panic_on_db_errors {
                                abort();
                            }
                        }
                    }
                },
                Err(err) => match err {
                    RecvTimeoutError::Timeout => {
                        if !self.is_startup_done && is_startup_done.load(Ordering::Relaxed) {
                            if let Err(err) = self.notify_end_of_startup() {
                                error!("Error in notifying end of startup: ({})", err);
                                if panic_on_db_errors {
                                    abort();
                                }
                            }
                            self.is_startup_done = true;
                            startup_done_count.fetch_add(1, Ordering::Relaxed);
                        }

                        continue;
                    }
                    _ => {
                        error!("Error in receiving the item {:?}", err);
                        if panic_on_db_errors {
                            abort();
                        }
                        break;
                    }
                },
            }
        }
        Ok(())
    }
}

pub struct ParallelBigtableClient {
    workers: Vec<JoinHandle<Result<(), GeyserPluginError>>>,
    exit_worker: Arc<AtomicBool>,
    is_startup_done: Arc<AtomicBool>,
    startup_done_count: Arc<AtomicUsize>,
    initialized_worker_count: Arc<AtomicUsize>,
    sender: Sender<DbWorkItem>,
    last_report: AtomicInterval,
    do_work_on_startup: bool,
}

impl ParallelBigtableClient {
    pub fn new(config: &GeyserPluginBigtableConfig) -> Result<Self, GeyserPluginError> {
        info!("Creating ParallelBigtableClient...");
        let (sender, receiver) = bounded(MAX_ASYNC_REQUESTS);
        let exit_worker = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::default();
        let is_startup_done = Arc::new(AtomicBool::new(false));
        let startup_done_count = Arc::new(AtomicUsize::new(0));
        let worker_count = config.threads.unwrap_or(DEFAULT_THREADS_COUNT);
        let initialized_worker_count = Arc::new(AtomicUsize::new(0));
        let thread_per_runtime = 2;
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(config.threads.unwrap_or(thread_per_runtime))
                .thread_name("sol-acountsdb-plugin-bigtable")
                .enable_all()
                .build()
                .expect("Runtime"),
        );

        for i in 0..worker_count {
            let cloned_receiver = receiver.clone();
            let exit_clone = exit_worker.clone();
            let is_startup_done_clone = is_startup_done.clone();
            let startup_done_count_clone = startup_done_count.clone();
            let initialized_worker_count_clone = initialized_worker_count.clone();
            let config = config.clone();
            let runtime = runtime.clone();
            let worker = Builder::new()
                .name(format!("worker-{}", i))
                .spawn(move || -> Result<(), GeyserPluginError> {
                    let panic_on_db_errors = *config
                        .panic_on_db_errors
                        .as_ref()
                        .unwrap_or(&DEFAULT_PANIC_ON_DB_ERROR);
                    let result = BigtableClientWorker::new(config, runtime);

                    match result {
                        Ok(mut worker) => {
                            initialized_worker_count_clone.fetch_add(1, Ordering::Relaxed);
                            worker.do_work(
                                cloned_receiver,
                                exit_clone,
                                is_startup_done_clone,
                                startup_done_count_clone,
                                panic_on_db_errors,
                            )?;
                            Ok(())
                        }
                        Err(err) => {
                            error!("Error when making connection to database: ({})", err);
                            if panic_on_db_errors {
                                abort();
                            }
                            Err(err)
                        }
                    }
                })
                .unwrap();

            workers.push(worker);
        }

        info!("Created ParallelBigtableClient.");
        Ok(Self {
            last_report: AtomicInterval::default(),
            workers,
            exit_worker,
            is_startup_done,
            startup_done_count,
            initialized_worker_count,
            sender,
            do_work_on_startup: config.write_during_startup.unwrap_or(true)
        })
    }

    pub fn join(&mut self) -> thread::Result<()> {
        self.exit_worker.store(true, Ordering::Relaxed);
        while !self.workers.is_empty() {
            let worker = self.workers.pop();
            if worker.is_none() {
                break;
            }
            let worker = worker.unwrap();
            let result = worker.join().unwrap();
            if result.is_err() {
                error!("The worker thread has failed: {:?}", result);
            }
        }

        Ok(())
    }

    pub fn update_account(
        &mut self,
        account: &ReplicaAccountInfo,
        slot: u64,
        is_startup: bool,
    ) -> Result<(), GeyserPluginError> {
        if self.should_skip_work() {
            return Ok(())
        }
        if self.last_report.should_update(30000) {
            datapoint_debug!(
                "bigtable-plugin-stats",
                ("message-queue-length", self.sender.len() as i64, i64),
            );
        }
        let mut measure = Measure::start("geyser-plugin-bigtable-create-work-item");
        let wrk_item = DbWorkItem::UpdateAccount(Box::new(UpdateAccountRequest {
            account: DbAccountInfo::new(account, slot),
            is_startup,
        }));

        measure.stop();

        inc_new_counter_debug!(
            "geyser-plugin-bigtable-create-work-item-us",
            measure.as_us() as usize,
            100000,
            100000
        );

        let mut measure = Measure::start("geyser-plugin-bigtable-send-msg");

        if let Err(err) = self.sender.send(wrk_item) {
            return Err(GeyserPluginError::AccountsUpdateError {
                msg: format!(
                    "Failed to update the account {:?}, error: {:?}",
                    bs58::encode(account.pubkey()).into_string(),
                    err
                ),
            });
        }

        measure.stop();
        inc_new_counter_debug!(
            "geyser-plugin-bigtable-send-msg-us",
            measure.as_us() as usize,
            100000,
            100000
        );

        Ok(())
    }

    pub fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> Result<(), GeyserPluginError> {
        if self.should_skip_work() {
            return Ok(())
        }
        if let Err(err) = self
            .sender
            .send(DbWorkItem::UpdateSlot(Box::new(UpdateSlotRequest {
                slot,
                parent,
                slot_status: status,
            })))
        {
            return Err(GeyserPluginError::SlotStatusUpdateError {
                msg: format!("Failed to update the slot {:?}, error: {:?}", slot, err),
            });
        }
        Ok(())
    }

    pub fn update_block_metadata(
        &mut self,
        block_info: &ReplicaBlockInfo,
    ) -> Result<(), GeyserPluginError> {
        if self.should_skip_work() {
            return Ok(())
        }
        if let Err(err) = self.sender.send(DbWorkItem::UpdateBlockMetadata(Box::new(
            UpdateBlockMetadataRequest {
                block_info: DbBlockInfo::from(block_info),
            },
        ))) {
            return Err(GeyserPluginError::SlotStatusUpdateError {
                msg: format!(
                    "Failed to update the block metadata at slot {:?}, error: {:?}",
                    block_info.slot, err
                ),
            });
        }
        Ok(())
    }

    pub fn notify_end_of_startup(&mut self) -> Result<(), GeyserPluginError> {
        info!("Notifying the end of startup");
        // Ensure all items in the queue has been received by the workers
        while !self.sender.is_empty() {
            sleep(Duration::from_millis(100));
        }
        self.is_startup_done.store(true, Ordering::Relaxed);

        // Wait for all worker threads to be done with flushing
        while self.startup_done_count.load(Ordering::Relaxed)
            != self.initialized_worker_count.load(Ordering::Relaxed)
        {
            info!(
                "Startup done count: {}, good worker thread count: {}",
                self.startup_done_count.load(Ordering::Relaxed),
                self.initialized_worker_count.load(Ordering::Relaxed)
            );
            sleep(Duration::from_millis(100));
        }

        info!("Done with notifying the end of startup");
        Ok(())
    }

    fn build_transaction_request(
        slot: u64,
        transaction_info: &ReplicaTransactionInfo,
    ) -> LogTransactionRequest {
        LogTransactionRequest {
            transaction_info: build_db_transaction(slot, transaction_info),
        }
    }

    pub fn log_transaction_info(
        &mut self,
        transaction_info: &ReplicaTransactionInfo,
        slot: u64,
    ) -> Result<(), GeyserPluginError> {
        if self.should_skip_work() {
            return Ok(())
        }
        let wrk_item = DbWorkItem::LogTransaction(Box::new(Self::build_transaction_request(
            slot,
            transaction_info,
        )));

        if let Err(err) = self.sender.send(wrk_item) {
            return Err(GeyserPluginError::SlotStatusUpdateError {
                msg: format!("Failed to update the transaction, error: {:?}", err),
            });
        }
        Ok(())
    }

    fn should_skip_work(&self) -> bool {
        !self.do_work_on_startup && !self.is_startup_done.load(Ordering::Relaxed)
    }
}
