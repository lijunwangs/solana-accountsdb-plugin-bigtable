mod bigtable_client_account_index;
mod bigtable_client_block_metadata;
mod bigtable_client_transaction;


use {
    crate::{
        accountsdb_plugin_bigtable::{AccountsDbPluginBigtableError, AccountsDbPluginBigtableConfig},
        bigtable::BigTableConnection as Client,
    },
    chrono::Utc,
    bigtable_client_transaction::LogTransactionRequest,
    bigtable_client_block_metadata::UpdateBlockMetadataRequest,
    bigtable_client_block_metadata::DbBlockInfo,
    bigtable_client_account_index::TokenSecondaryIndexEntry,
    crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender},
    log::*,
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        AccountsDbPluginError, ReplicaAccountInfo, ReplicaBlockInfo, SlotStatus,
    },
    solana_measure::measure::Measure,
    solana_metrics::*,
    solana_sdk::timing::AtomicInterval,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc, Mutex,
        },
        thread::{self, sleep, Builder, JoinHandle},
        time::Duration,
    },
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


impl Eq for DbAccountInfo {}

#[derive(Clone, PartialEq, Debug)]
pub struct DbAccountInfo {
    pub pubkey: Vec<u8>,
    pub lamports: i64,
    pub owner: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: i64,
    pub data: Vec<u8>,
    pub slot: i64,
    pub write_version: i64,
}

impl DbAccountInfo {
    fn new<T: ReadableAccountInfo>(account: &T, slot: u64) -> DbAccountInfo {
        let data = account.data().to_vec();
        Self {
            pubkey: account.pubkey().to_vec(),
            lamports: account.lamports() as i64,
            owner: account.owner().to_vec(),
            executable: account.executable(),
            rent_epoch: account.rent_epoch() as i64,
            data,
            slot: slot as i64,
            write_version: account.write_version(),
        }
    }
}

impl ReadableAccountInfo for DbAccountInfo {
    fn pubkey(&self) -> &[u8] {
        &self.pubkey
    }

    fn owner(&self) -> &[u8] {
        &self.owner
    }

    fn lamports(&self) -> i64 {
        self.lamports
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> i64 {
        self.rent_epoch
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn write_version(&self) -> i64 {
        self.write_version
    }
}

impl<'a> ReadableAccountInfo for ReplicaAccountInfo<'a> {
    fn pubkey(&self) -> &[u8] {
        self.pubkey
    }

    fn owner(&self) -> &[u8] {
        self.owner
    }

    fn lamports(&self) -> i64 {
        self.lamports as i64
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> i64 {
        self.rent_epoch as i64
    }

    fn data(&self) -> &[u8] {
        self.data
    }

    fn write_version(&self) -> i64 {
        self.write_version as i64
    }
}

pub trait ReadableAccountInfo: Sized {
    fn pubkey(&self) -> &[u8];
    fn owner(&self) -> &[u8];
    fn lamports(&self) -> i64;
    fn executable(&self) -> bool;
    fn rent_epoch(&self) -> i64;
    fn data(&self) -> &[u8];
    fn write_version(&self) -> i64;
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


struct UpdateAccountRequest {
    account: DbAccountInfo,
    is_startup: bool,
}

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
    // update_account_stmt: Statement,
    // bulk_account_insert_stmt: Statement,
    // update_slot_with_parent_stmt: Statement,
    // update_slot_without_parent_stmt: Statement,
    // update_transaction_log_stmt: Statement,
    // update_block_metadata_stmt: Statement,
    // insert_account_audit_stmt: Option<Statement>,
    // insert_token_owner_index_stmt: Option<Statement>,
    // insert_token_mint_index_stmt: Option<Statement>,
    // bulk_insert_token_owner_index_stmt: Option<Statement>,
    // bulk_insert_token_mint_index_stmt: Option<Statement>,
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


struct BigtableClientWorker {
    client: SimpleBigtableClient,
    /// Indicating if accounts notification during startup is done.
    is_startup_done: bool,
}

impl BigtableClientWorker {
    fn new(config: AccountsDbPluginBigtableConfig) -> Result<Self, AccountsDbPluginError> {
        let result = SimpleBigtableClient::new(&config);
        match result {
            Ok(client) => Ok(BigtableClientWorker {
                client,
                is_startup_done: false,
            }),
            Err(err) => {
                error!("Error in creating SimpleBigtableClient: {}", err);
                Err(err)
            }
        }
    }

    fn do_work(
        &mut self,
        receiver: Receiver<DbWorkItem>,
        exit_worker: Arc<AtomicBool>,
        is_startup_done: Arc<AtomicBool>,
        startup_done_count: Arc<AtomicUsize>,
        panic_on_db_errors: bool,
    ) -> Result<(), AccountsDbPluginError> {
        while !exit_worker.load(Ordering::Relaxed) {
            let mut measure = Measure::start("accountsdb-plugin-bigtable-worker-recv");
            let work = receiver.recv_timeout(Duration::from_millis(500));
            measure.stop();
            inc_new_counter_debug!(
                "accountsdb-plugin-bigtable-worker-recv-us",
                measure.as_us() as usize,
                100000,
                100000
            );
            match work {
                Ok(work) => match work {
                    DbWorkItem::UpdateAccount(request) => {
                        if let Err(err) = self
                            .client
                            .update_account(request.account, request.is_startup)
                        {
                            error!("Failed to update account: ({})", err);
                            if panic_on_db_errors {
                                abort();
                            }
                        }
                    }
                    DbWorkItem::UpdateSlot(request) => {
                        if let Err(err) = self.client.update_slot_status(
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
                        if let Err(err) = self.client.log_transaction(*transaction_log_info) {
                            error!("Failed to update transaction: ({})", err);
                            if panic_on_db_errors {
                                abort();
                            }
                        }
                    }
                    DbWorkItem::UpdateBlockMetadata(block_info) => {
                        if let Err(err) = self.client.update_block_metadata(*block_info) {
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
                            if let Err(err) = self.client.notify_end_of_startup() {
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


impl SimpleBigtableClient {
    pub fn connect_to_db(
        config: &AccountsDbPluginBigtableConfig,
    ) -> Result<Client, AccountsDbPluginError> {
        Err(AccountsDbPluginError::Custom(
            Box::new(
                AccountsDbPluginBigtableError::DataStoreConnectionError {
                    msg: "There is no connection to the Bigtable database.".to_string(),
                },
        )))
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

    pub fn new(config: &AccountsDbPluginBigtableConfig) -> Result<Self, AccountsDbPluginError> {
        info!("Creating SimpleBigtableClient...");
        let mut client = Self::connect_to_db(config)?;

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
pub struct ParallelBigtableClient {
    workers: Vec<JoinHandle<Result<(), AccountsDbPluginError>>>,
    exit_worker: Arc<AtomicBool>,
    is_startup_done: Arc<AtomicBool>,
    startup_done_count: Arc<AtomicUsize>,
    initialized_worker_count: Arc<AtomicUsize>,
    sender: Sender<DbWorkItem>,
    last_report: AtomicInterval,
}

impl ParallelBigtableClient {
    pub fn new(config: &AccountsDbPluginBigtableConfig) -> Result<Self, AccountsDbPluginError> {
        info!("Creating ParallelBigtableClient...");
        let (sender, receiver) = bounded(MAX_ASYNC_REQUESTS);
        let exit_worker = Arc::new(AtomicBool::new(false));
        let mut workers = Vec::default();
        let is_startup_done = Arc::new(AtomicBool::new(false));
        let startup_done_count = Arc::new(AtomicUsize::new(0));
        let worker_count = config.threads.unwrap_or(DEFAULT_THREADS_COUNT);
        let initialized_worker_count = Arc::new(AtomicUsize::new(0));
        for i in 0..worker_count {
            let cloned_receiver = receiver.clone();
            let exit_clone = exit_worker.clone();
            let is_startup_done_clone = is_startup_done.clone();
            let startup_done_count_clone = startup_done_count.clone();
            let initialized_worker_count_clone = initialized_worker_count.clone();
            let config = config.clone();
            let worker = Builder::new()
                .name(format!("worker-{}", i))
                .spawn(move || -> Result<(), AccountsDbPluginError> {
                    let panic_on_db_errors = *config
                        .panic_on_db_errors
                        .as_ref()
                        .unwrap_or(&DEFAULT_PANIC_ON_DB_ERROR);
                    let result = BigtableClientWorker::new(config);

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
    ) -> Result<(), AccountsDbPluginError> {
        if self.last_report.should_update(30000) {
            datapoint_debug!(
                "bigtable-plugin-stats",
                ("message-queue-length", self.sender.len() as i64, i64),
            );
        }
        let mut measure = Measure::start("accountsdb-plugin-posgres-create-work-item");
        let wrk_item = DbWorkItem::UpdateAccount(Box::new(UpdateAccountRequest {
            account: DbAccountInfo::new(account, slot),
            is_startup,
        }));

        measure.stop();

        inc_new_counter_debug!(
            "accountsdb-plugin-posgres-create-work-item-us",
            measure.as_us() as usize,
            100000,
            100000
        );

        let mut measure = Measure::start("accountsdb-plugin-posgres-send-msg");

        if let Err(err) = self.sender.send(wrk_item) {
            return Err(AccountsDbPluginError::AccountsUpdateError {
                msg: format!(
                    "Failed to update the account {:?}, error: {:?}",
                    bs58::encode(account.pubkey()).into_string(),
                    err
                ),
            });
        }

        measure.stop();
        inc_new_counter_debug!(
            "accountsdb-plugin-posgres-send-msg-us",
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
    ) -> Result<(), AccountsDbPluginError> {
        if let Err(err) = self
            .sender
            .send(DbWorkItem::UpdateSlot(Box::new(UpdateSlotRequest {
                slot,
                parent,
                slot_status: status,
            })))
        {
            return Err(AccountsDbPluginError::SlotStatusUpdateError {
                msg: format!("Failed to update the slot {:?}, error: {:?}", slot, err),
            });
        }
        Ok(())
    }

    pub fn update_block_metadata(
        &mut self,
        block_info: &ReplicaBlockInfo,
    ) -> Result<(), AccountsDbPluginError> {
        if let Err(err) = self.sender.send(DbWorkItem::UpdateBlockMetadata(Box::new(
            UpdateBlockMetadataRequest {
                block_info: DbBlockInfo::from(block_info),
            },
        ))) {
            return Err(AccountsDbPluginError::SlotStatusUpdateError {
                msg: format!(
                    "Failed to update the block metadata at slot {:?}, error: {:?}",
                    block_info.slot, err
                ),
            });
        }
        Ok(())
    }

    pub fn notify_end_of_startup(&mut self) -> Result<(), AccountsDbPluginError> {
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
}
