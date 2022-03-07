use {
    crate::{
            bigtable_client::{bigtable_client_transaction::DbReward, SimpleBigtableClient},
    },
    chrono::Utc,
    log::*,
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        AccountsDbPluginError, ReplicaBlockInfo,
    },    
};

#[derive(Clone, Debug)]
pub struct DbBlockInfo {
    pub slot: i64,
    pub blockhash: String,
    pub rewards: Vec<DbReward>,
    pub block_time: Option<i64>,
    pub block_height: Option<i64>,
}

impl<'a> From<&ReplicaBlockInfo<'a>> for DbBlockInfo {
    fn from(block_info: &ReplicaBlockInfo) -> Self {
        Self {
            slot: block_info.slot as i64,
            blockhash: block_info.blockhash.to_string(),
            rewards: block_info.rewards.iter().map(DbReward::from).collect(),
            block_time: block_info.block_time,
            block_height: block_info
                .block_height
                .map(|block_height| block_height as i64),
        }
    }
}

pub struct UpdateBlockMetadataRequest {
    pub block_info: DbBlockInfo,
}


impl SimpleBigtableClient {

    pub(crate) fn update_block_metadata_impl(
        &mut self,
        block_info: UpdateBlockMetadataRequest,
    ) -> Result<(), AccountsDbPluginError> {

        Ok(())
    }
}
