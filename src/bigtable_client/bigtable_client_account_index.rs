const TOKEN_INDEX_COLUMN_COUNT: usize = 3;
/// Struct for the secondary index for both token account's owner and mint index,
pub struct TokenSecondaryIndexEntry {
    /// In case of token owner, the secondary key is the Pubkey of the owner and in case of
    /// token index the secondary_key is the Pubkey of mint.
    secondary_key: Vec<u8>,

    /// The Pubkey of the account
    account_key: Vec<u8>,

    /// Record the slot at which the index entry is created.
    slot: i64,
}
