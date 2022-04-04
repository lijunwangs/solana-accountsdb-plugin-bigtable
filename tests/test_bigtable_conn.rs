#![allow(clippy::integer_arithmetic)]

/// For integration tests locally, use the Google Bigtable Emulator.
/// See this project's README.md

use {
    log::*,
    solana_geyser_plugin_bigtable::{
        bigtable::BigTableConnection,
        convert::accounts,
    },
    solana_sdk::pubkey::Pubkey,
    std::{
        time::{SystemTime},
    },
};

const RUST_LOG_FILTER: &str =
    "info,solana_core::replay_stage=warn,solana_local_cluster=info,local_cluster=info";

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_bigtable_connection() {
    solana_logger::setup_with_default(RUST_LOG_FILTER);

    let result = BigTableConnection::new("geyser-bigtable", false, None, None).await;

    if result.is_err() {
        error!("Failed to connecto the Bigtable database. Please setup the database to run the integration tests. {:?}", result.err());
        return;
    }

    let conn = result.unwrap();
    info!("Connected to Bigtable!");
    
    let mut data = vec![0; 1024];

    for i in 0..data.len() {
        data[i] = rand::random();
    }

    let pubkey = Pubkey::new_unique();
    let account = accounts::Account {
        pubkey: pubkey.to_bytes().to_vec(),
        lamports: 1234,
        owner: Pubkey::new_unique().to_bytes().to_vec(),
        data,
        slot: 12345,
        executable: false,
        rent_epoch: 0,
        write_version: 1,
        updated_on: Some(accounts::UnixTimestamp {
            timestamp: SystemTime::now().elapsed().unwrap().as_secs() as i64,
        })
    };
    let account_cells = [(pubkey.to_string(), account)];
    let result = conn
        .put_protobuf_cells_with_retry::<accounts::Account>("account", &account_cells)
        .await;
    assert!(result.is_ok());
    info!("Written length {}", result.unwrap());
}
