use {
    crate::bigtable_client::{AsyncBigtableClient, SimpleBigtableClient},
    chrono::Utc,
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        AccountsDbPluginError, ReplicaTransactionInfo,
    },
    solana_runtime::bank::RewardType,
    solana_sdk::{
        instruction::CompiledInstruction,
        message::{
            v0::{self, LoadedAddresses, MessageAddressTableLookup},
            Message, MessageHeader, SanitizedMessage,
        },
        transaction::TransactionError,
    },
    solana_transaction_status::{
        InnerInstructions, Reward, TransactionStatusMeta, TransactionTokenBalance,
    },
};

const MAX_TRANSACTION_STATUS_LEN: usize = 256;

#[derive(Clone, Debug)]
pub struct DbCompiledInstruction {
    pub program_id_index: i16,
    pub accounts: Vec<i16>,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct DbInnerInstructions {
    pub index: i16,
    pub instructions: Vec<DbCompiledInstruction>,
}

#[derive(Clone, Debug)]
pub struct DbTransactionTokenBalance {
    pub account_index: i16,
    pub mint: String,
    pub ui_token_amount: Option<f64>,
    pub owner: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DbRewardType {
    Fee,
    Rent,
    Staking,
    Voting,
}

#[derive(Clone, Debug)]
pub struct DbReward {
    pub pubkey: String,
    pub lamports: i64,
    pub post_balance: i64,
    pub reward_type: Option<DbRewardType>,
    pub commission: Option<i16>,
}

#[derive(Clone, Debug)]
pub struct DbTransactionStatusMeta {
    pub error: Option<DbTransactionError>,
    pub fee: i64,
    pub pre_balances: Vec<i64>,
    pub post_balances: Vec<i64>,
    pub inner_instructions: Option<Vec<DbInnerInstructions>>,
    pub log_messages: Option<Vec<String>>,
    pub pre_token_balances: Option<Vec<DbTransactionTokenBalance>>,
    pub post_token_balances: Option<Vec<DbTransactionTokenBalance>>,
    pub rewards: Option<Vec<DbReward>>,
}

#[derive(Clone, Debug)]
pub struct DbTransactionMessageHeader {
    pub num_required_signatures: i16,
    pub num_readonly_signed_accounts: i16,
    pub num_readonly_unsigned_accounts: i16,
}

#[derive(Clone, Debug)]
pub struct DbTransactionMessage {
    pub header: DbTransactionMessageHeader,
    pub account_keys: Vec<Vec<u8>>,
    pub recent_blockhash: Vec<u8>,
    pub instructions: Vec<DbCompiledInstruction>,
}

#[derive(Clone, Debug)]
pub struct DbTransactionMessageAddressTableLookup {
    pub account_key: Vec<u8>,
    pub writable_indexes: Vec<i16>,
    pub readonly_indexes: Vec<i16>,
}

#[derive(Clone, Debug)]
pub struct DbTransactionMessageV0 {
    pub header: DbTransactionMessageHeader,
    pub account_keys: Vec<Vec<u8>>,
    pub recent_blockhash: Vec<u8>,
    pub instructions: Vec<DbCompiledInstruction>,
    pub address_table_lookups: Vec<DbTransactionMessageAddressTableLookup>,
}

#[derive(Clone, Debug)]
pub struct DbLoadedAddresses {
    pub writable: Vec<Vec<u8>>,
    pub readonly: Vec<Vec<u8>>,
}

#[derive(Clone, Debug)]
pub struct DbLoadedMessageV0 {
    pub message: DbTransactionMessageV0,
    pub loaded_addresses: DbLoadedAddresses,
}

pub struct DbTransaction {
    pub signature: Vec<u8>,
    pub is_vote: bool,
    pub slot: i64,
    pub message_type: i16,
    pub legacy_message: Option<DbTransactionMessage>,
    pub v0_loaded_message: Option<DbLoadedMessageV0>,
    pub message_hash: Vec<u8>,
    pub meta: DbTransactionStatusMeta,
    pub signatures: Vec<Vec<u8>>,
}

pub struct LogTransactionRequest {
    pub transaction_info: DbTransaction,
}

impl From<&MessageAddressTableLookup> for DbTransactionMessageAddressTableLookup {
    fn from(address_table_lookup: &MessageAddressTableLookup) -> Self {
        Self {
            account_key: address_table_lookup.account_key.as_ref().to_vec(),
            writable_indexes: address_table_lookup
                .writable_indexes
                .iter()
                .map(|idx| *idx as i16)
                .collect(),
            readonly_indexes: address_table_lookup
                .readonly_indexes
                .iter()
                .map(|idx| *idx as i16)
                .collect(),
        }
    }
}

impl From<&LoadedAddresses> for DbLoadedAddresses {
    fn from(loaded_addresses: &LoadedAddresses) -> Self {
        Self {
            writable: loaded_addresses
                .writable
                .iter()
                .map(|pubkey| pubkey.as_ref().to_vec())
                .collect(),
            readonly: loaded_addresses
                .readonly
                .iter()
                .map(|pubkey| pubkey.as_ref().to_vec())
                .collect(),
        }
    }
}

impl From<&MessageHeader> for DbTransactionMessageHeader {
    fn from(header: &MessageHeader) -> Self {
        Self {
            num_required_signatures: header.num_required_signatures as i16,
            num_readonly_signed_accounts: header.num_readonly_signed_accounts as i16,
            num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as i16,
        }
    }
}

impl From<&CompiledInstruction> for DbCompiledInstruction {
    fn from(instruction: &CompiledInstruction) -> Self {
        Self {
            program_id_index: instruction.program_id_index as i16,
            accounts: instruction
                .accounts
                .iter()
                .map(|account_idx| *account_idx as i16)
                .collect(),
            data: instruction.data.clone(),
        }
    }
}

impl From<&Message> for DbTransactionMessage {
    fn from(message: &Message) -> Self {
        Self {
            header: DbTransactionMessageHeader::from(&message.header),
            account_keys: message
                .account_keys
                .iter()
                .map(|key| key.as_ref().to_vec())
                .collect(),
            recent_blockhash: message.recent_blockhash.as_ref().to_vec(),
            instructions: message
                .instructions
                .iter()
                .map(DbCompiledInstruction::from)
                .collect(),
        }
    }
}

impl From<&v0::Message> for DbTransactionMessageV0 {
    fn from(message: &v0::Message) -> Self {
        Self {
            header: DbTransactionMessageHeader::from(&message.header),
            account_keys: message
                .account_keys
                .iter()
                .map(|key| key.as_ref().to_vec())
                .collect(),
            recent_blockhash: message.recent_blockhash.as_ref().to_vec(),
            instructions: message
                .instructions
                .iter()
                .map(DbCompiledInstruction::from)
                .collect(),
            address_table_lookups: message
                .address_table_lookups
                .iter()
                .map(DbTransactionMessageAddressTableLookup::from)
                .collect(),
        }
    }
}

impl From<&v0::LoadedMessage> for DbLoadedMessageV0 {
    fn from(message: &v0::LoadedMessage) -> Self {
        Self {
            message: DbTransactionMessageV0::from(&message.message),
            loaded_addresses: DbLoadedAddresses::from(&message.loaded_addresses),
        }
    }
}

impl From<&InnerInstructions> for DbInnerInstructions {
    fn from(instructions: &InnerInstructions) -> Self {
        Self {
            index: instructions.index as i16,
            instructions: instructions
                .instructions
                .iter()
                .map(DbCompiledInstruction::from)
                .collect(),
        }
    }
}

impl From<&RewardType> for DbRewardType {
    fn from(reward_type: &RewardType) -> Self {
        match reward_type {
            RewardType::Fee => Self::Fee,
            RewardType::Rent => Self::Rent,
            RewardType::Staking => Self::Staking,
            RewardType::Voting => Self::Voting,
        }
    }
}

fn get_reward_type(reward: &Option<RewardType>) -> Option<DbRewardType> {
    reward.as_ref().map(DbRewardType::from)
}

impl From<&Reward> for DbReward {
    fn from(reward: &Reward) -> Self {
        Self {
            pubkey: reward.pubkey.clone(),
            lamports: reward.lamports as i64,
            post_balance: reward.post_balance as i64,
            reward_type: get_reward_type(&reward.reward_type),
            commission: reward
                .commission
                .as_ref()
                .map(|commission| *commission as i16),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct DbTransactionError {
    error_code: DbTransactionErrorCode,
    error_detail: Option<String>,
}

fn get_transaction_error(result: &Result<(), TransactionError>) -> Option<DbTransactionError> {
    if result.is_ok() {
        return None;
    }

    let error = result.as_ref().err().unwrap();
    Some(DbTransactionError {
        error_code: DbTransactionErrorCode::from(error),
        error_detail: {
            if let TransactionError::InstructionError(idx, instruction_error) = error {
                let mut error_detail = format!(
                    "InstructionError: idx ({}), error: ({})",
                    idx, instruction_error
                );
                if error_detail.len() > MAX_TRANSACTION_STATUS_LEN {
                    error_detail = error_detail
                        .to_string()
                        .split_off(MAX_TRANSACTION_STATUS_LEN);
                }
                Some(error_detail)
            } else {
                None
            }
        },
    })
}

impl From<&TransactionTokenBalance> for DbTransactionTokenBalance {
    fn from(token_balance: &TransactionTokenBalance) -> Self {
        Self {
            account_index: token_balance.account_index as i16,
            mint: token_balance.mint.clone(),
            ui_token_amount: token_balance.ui_token_amount.ui_amount,
            owner: token_balance.owner.clone(),
        }
    }
}

impl From<&TransactionStatusMeta> for DbTransactionStatusMeta {
    fn from(meta: &TransactionStatusMeta) -> Self {
        Self {
            error: get_transaction_error(&meta.status),
            fee: meta.fee as i64,
            pre_balances: meta
                .pre_balances
                .iter()
                .map(|balance| *balance as i64)
                .collect(),
            post_balances: meta
                .post_balances
                .iter()
                .map(|balance| *balance as i64)
                .collect(),
            inner_instructions: meta
                .inner_instructions
                .as_ref()
                .map(|instructions| instructions.iter().map(DbInnerInstructions::from).collect()),
            log_messages: meta.log_messages.clone(),
            pre_token_balances: meta.pre_token_balances.as_ref().map(|balances| {
                balances
                    .iter()
                    .map(DbTransactionTokenBalance::from)
                    .collect()
            }),
            post_token_balances: meta.post_token_balances.as_ref().map(|balances| {
                balances
                    .iter()
                    .map(DbTransactionTokenBalance::from)
                    .collect()
            }),
            rewards: meta
                .rewards
                .as_ref()
                .map(|rewards| rewards.iter().map(DbReward::from).collect()),
        }
    }
}

fn build_db_transaction(slot: u64, transaction_info: &ReplicaTransactionInfo) -> DbTransaction {
    DbTransaction {
        signature: transaction_info.signature.as_ref().to_vec(),
        is_vote: transaction_info.is_vote,
        slot: slot as i64,
        message_type: match transaction_info.transaction.message() {
            SanitizedMessage::Legacy(_) => 0,
            SanitizedMessage::V0(_) => 1,
        },
        legacy_message: match transaction_info.transaction.message() {
            SanitizedMessage::Legacy(legacy_message) => {
                Some(DbTransactionMessage::from(legacy_message))
            }
            _ => None,
        },
        v0_loaded_message: match transaction_info.transaction.message() {
            SanitizedMessage::V0(loaded_message) => Some(DbLoadedMessageV0::from(loaded_message)),
            _ => None,
        },
        signatures: transaction_info
            .transaction
            .signatures()
            .iter()
            .map(|signature| signature.as_ref().to_vec())
            .collect(),
        message_hash: transaction_info
            .transaction
            .message_hash()
            .as_ref()
            .to_vec(),
        meta: DbTransactionStatusMeta::from(transaction_info.transaction_status_meta),
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum DbTransactionErrorCode {
    AccountInUse,
    AccountLoadedTwice,
    AccountNotFound,
    ProgramAccountNotFound,
    InsufficientFundsForFee,
    InvalidAccountForFee,
    AlreadyProcessed,
    BlockhashNotFound,
    InstructionError,
    CallChainTooDeep,
    MissingSignatureForFee,
    InvalidAccountIndex,
    SignatureFailure,
    InvalidProgramForExecution,
    SanitizeFailure,
    ClusterMaintenance,
    AccountBorrowOutstanding,
    WouldExceedMaxAccountCostLimit,
    WouldExceedMaxBlockCostLimit,
    UnsupportedVersion,
    InvalidWritableAccount,
    WouldExceedMaxAccountDataCostLimit,
    TooManyAccountLocks,
    AddressLookupTableNotFound,
    InvalidAddressLookupTableOwner,
    InvalidAddressLookupTableData,
    InvalidAddressLookupTableIndex,
    InvalidRentPayingAccount,
    WouldExceedMaxVoteCostLimit,
}

impl From<&TransactionError> for DbTransactionErrorCode {
    fn from(err: &TransactionError) -> Self {
        match err {
            TransactionError::AccountInUse => Self::AccountInUse,
            TransactionError::AccountLoadedTwice => Self::AccountLoadedTwice,
            TransactionError::AccountNotFound => Self::AccountNotFound,
            TransactionError::ProgramAccountNotFound => Self::ProgramAccountNotFound,
            TransactionError::InsufficientFundsForFee => Self::InsufficientFundsForFee,
            TransactionError::InvalidAccountForFee => Self::InvalidAccountForFee,
            TransactionError::AlreadyProcessed => Self::AlreadyProcessed,
            TransactionError::BlockhashNotFound => Self::BlockhashNotFound,
            TransactionError::InstructionError(_idx, _error) => Self::InstructionError,
            TransactionError::CallChainTooDeep => Self::CallChainTooDeep,
            TransactionError::MissingSignatureForFee => Self::MissingSignatureForFee,
            TransactionError::InvalidAccountIndex => Self::InvalidAccountIndex,
            TransactionError::SignatureFailure => Self::SignatureFailure,
            TransactionError::InvalidProgramForExecution => Self::InvalidProgramForExecution,
            TransactionError::SanitizeFailure => Self::SanitizeFailure,
            TransactionError::ClusterMaintenance => Self::ClusterMaintenance,
            TransactionError::AccountBorrowOutstanding => Self::AccountBorrowOutstanding,
            TransactionError::WouldExceedMaxAccountCostLimit => {
                Self::WouldExceedMaxAccountCostLimit
            }
            TransactionError::WouldExceedMaxBlockCostLimit => Self::WouldExceedMaxBlockCostLimit,
            TransactionError::UnsupportedVersion => Self::UnsupportedVersion,
            TransactionError::InvalidWritableAccount => Self::InvalidWritableAccount,
            TransactionError::WouldExceedMaxAccountDataCostLimit => {
                Self::WouldExceedMaxAccountDataCostLimit
            }
            TransactionError::TooManyAccountLocks => Self::TooManyAccountLocks,
            TransactionError::AddressLookupTableNotFound => Self::AddressLookupTableNotFound,
            TransactionError::InvalidAddressLookupTableOwner => {
                Self::InvalidAddressLookupTableOwner
            }
            TransactionError::InvalidAddressLookupTableData => Self::InvalidAddressLookupTableData,
            TransactionError::InvalidAddressLookupTableIndex => {
                Self::InvalidAddressLookupTableIndex
            }
            TransactionError::InvalidRentPayingAccount => Self::InvalidRentPayingAccount,
            TransactionError::WouldExceedMaxVoteCostLimit => Self::WouldExceedMaxVoteCostLimit,
        }
    }
}

impl SimpleBigtableClient {
    pub(crate) fn log_transaction_impl(
        &mut self,
        transaction_log_info: LogTransactionRequest,
    ) -> Result<(), AccountsDbPluginError> {
        let client = self.client.get_mut().unwrap();
        let client = &mut client.client;
        let updated_on = Utc::now().naive_utc();

        let transaction_info = transaction_log_info.transaction_info;

        Ok(())
    }
}

impl AsyncBigtableClient {
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
    ) -> Result<(), AccountsDbPluginError> {
        Ok(())
    }
}
