use {
    crate::{
        bigtable_client::{AsyncBigtableClient, SimpleBigtableClient},
        convert::accounts,
    },
    log::*,
    solana_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, ReplicaAccountInfo,
    },
    solana_sdk::pubkey::Pubkey,
    std::time::SystemTime,
};

impl Eq for DbAccountInfo {}

#[derive(Clone, PartialEq, Debug)]
pub struct DbAccountInfo {
    pub pubkey: Vec<u8>,
    pub lamports: u64,
    pub owner: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: u64,
    pub data: Vec<u8>,
    pub slot: u64,
    pub write_version: u64,
}

pub struct UpdateAccountRequest {
    pub account: DbAccountInfo,
    pub is_startup: bool,
}

impl DbAccountInfo {
    pub fn new<T: ReadableAccountInfo>(account: &T, slot: u64) -> DbAccountInfo {
        let data = account.data().to_vec();
        Self {
            pubkey: account.pubkey().to_vec(),
            lamports: account.lamports(),
            owner: account.owner().to_vec(),
            executable: account.executable(),
            rent_epoch: account.rent_epoch(),
            data,
            slot,
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

    fn lamports(&self) -> u64 {
        self.lamports
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> u64 {
        self.rent_epoch
    }

    fn data(&self) -> &[u8] {
        &self.data
    }

    fn write_version(&self) -> u64 {
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

    fn lamports(&self) -> u64 {
        self.lamports
    }

    fn executable(&self) -> bool {
        self.executable
    }

    fn rent_epoch(&self) -> u64 {
        self.rent_epoch
    }

    fn data(&self) -> &[u8] {
        self.data
    }

    fn write_version(&self) -> u64 {
        self.write_version
    }
}

pub trait ReadableAccountInfo: Sized {
    fn pubkey(&self) -> &[u8];
    fn owner(&self) -> &[u8];
    fn lamports(&self) -> u64;
    fn executable(&self) -> bool;
    fn rent_epoch(&self) -> u64;
    fn data(&self) -> &[u8];
    fn write_version(&self) -> u64;
}

impl From<&DbAccountInfo> for accounts::Account {
    fn from(account: &DbAccountInfo) -> Self {
        accounts::Account {
            pubkey: account.pubkey().to_vec(),
            owner: account.owner().to_vec(),
            lamports: account.lamports() as u64,
            slot: account.slot as u64,
            executable: account.executable(),
            rent_epoch: account.rent_epoch() as u64,
            data: account.data().to_vec(),
            write_version: account.write_version as u64,
            updated_on: Some(accounts::UnixTimestamp {
                timestamp: SystemTime::now().elapsed().unwrap().as_secs() as i64,
            }),
        }
    }
}

impl SimpleBigtableClient {
    /// Update or insert a single account
    pub async fn upsert_account(
        &mut self,
        account: &DbAccountInfo,
    ) -> Result<(), GeyserPluginError> {
        let client = self.client.get_mut().unwrap();
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

impl AsyncBigtableClient {
    pub fn update_account(
        &mut self,
        account: &ReplicaAccountInfo,
        slot: u64,
        is_startup: bool,
    ) -> Result<(), GeyserPluginError> {
        let account = DbAccountInfo::new(account, slot);

        let client = &mut self.client;
        self.runtime.block_on(client.upsert_account(&account))
    }

    pub fn notify_end_of_startup(&mut self) -> Result<(), GeyserPluginError> {
        info!("Notifying the end of startup");
        info!("Done with notifying the end of startup");
        Ok(())
    }
}
