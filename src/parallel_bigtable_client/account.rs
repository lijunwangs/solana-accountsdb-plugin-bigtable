use {
    crate::{parallel_bigtable_client::BufferedBigtableClient},
    log::*,
    prost::Message,
    solana_bigtable_geyser_models::models::{accounts},
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

impl BufferedBigtableClient {
    /// Update or insert a single account
    pub async fn update_account(
        &mut self,
        account: DbAccountInfo,
        _is_startup: bool,
    ) -> Result<(usize, usize), GeyserPluginError> {
        let account_cells = {
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
                    .collect::<Vec<(String, accounts::Account)>>()
            } else {
                return Ok((0, 0));
            }
        };
        let raw_size = account_cells.iter().map(|(_, m)| m.encoded_len()).sum();

        let client = self.client.lock().unwrap();
        let result = client
            .client
            .put_protobuf_cells_with_retry::<accounts::Account>("account", &account_cells, true)
            .await;
        match result {
            Ok(written_size) => Ok((written_size, raw_size)),
            Err(err) => {
                error!("Error persisting into the database: {}", err);
                for (key, account) in account_cells.iter() {
                    error!(
                        "Error persisting into the database: pubkey: {}, len: {} ",
                        key,
                        account.data.len()
                    );
                }
                Err(GeyserPluginError::Custom(Box::new(err)))
            }
        }
    }
}
