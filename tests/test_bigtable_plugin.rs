#![allow(clippy::integer_arithmetic)]

/// For integration tests locally, use the Google Bigtable Emulator.
/// See this project's README.md
use serde_json::json;

/// The test will cover transmitting accounts, transaction and slot and
/// block metadata.
use {
    libloading::Library,
    log::*,
    solana_core::validator::ValidatorConfig,
    solana_geyser_plugin_bigtable::{
        parallel_bigtable_client::BufferedBigtableClient, geyser_plugin_bigtable::GeyserPluginBigtableConfig,
    },
    solana_local_cluster::{
        cluster::Cluster,
        local_cluster::{ClusterConfig, LocalCluster},
        validator_configs::*,
    },
    solana_runtime::{
        snapshot_archive_info::SnapshotArchiveInfoGetter, snapshot_config::SnapshotConfig,
        snapshot_utils,
    },
    solana_sdk::{
        client::SyncClient, clock::Slot, commitment_config::CommitmentConfig,
        epoch_schedule::MINIMUM_SLOTS_PER_EPOCH, hash::Hash,
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        fs::{self, File},
        io::Read,
        io::Write,
        path::{Path, PathBuf},
        thread::sleep,
        time::Duration,
    },
    tempfile::TempDir,
};

const RUST_LOG_FILTER: &str =
    "info,solana_core::replay_stage=warn,solana_local_cluster=info,local_cluster=info";

fn wait_for_next_snapshot(
    cluster: &LocalCluster,
    snapshot_archives_dir: &Path,
) -> (PathBuf, (Slot, Hash)) {
    // Get slot after which this was generated
    let client = cluster
        .get_validator_client(&cluster.entry_point_info.id)
        .unwrap();
    let last_slot = client
        .get_slot_with_commitment(CommitmentConfig::processed())
        .expect("Couldn't get slot");

    // Wait for a snapshot for a bank >= last_slot to be made so we know that the snapshot
    // must include the transactions just pushed
    trace!(
        "Waiting for snapshot archive to be generated with slot > {}",
        last_slot
    );
    loop {
        if let Some(full_snapshot_archive_info) =
            snapshot_utils::get_highest_full_snapshot_archive_info(snapshot_archives_dir)
        {
            trace!(
                "full snapshot for slot {} exists",
                full_snapshot_archive_info.slot()
            );
            if full_snapshot_archive_info.slot() >= last_slot {
                return (
                    full_snapshot_archive_info.path().clone(),
                    (
                        full_snapshot_archive_info.slot(),
                        *full_snapshot_archive_info.hash(),
                    ),
                );
            }
            trace!(
                "full snapshot slot {} < last_slot {}",
                full_snapshot_archive_info.slot(),
                last_slot
            );
        }
        sleep(Duration::from_millis(1000));
    }
}

fn farf_dir() -> PathBuf {
    let dir: String = std::env::var("FARF_DIR").unwrap_or_else(|_| "farf".to_string());
    fs::create_dir_all(dir.clone()).unwrap();
    PathBuf::from(dir)
}

fn generate_account_paths(num_account_paths: usize) -> (Vec<TempDir>, Vec<PathBuf>) {
    let account_storage_dirs: Vec<TempDir> = (0..num_account_paths)
        .map(|_| tempfile::tempdir_in(farf_dir()).unwrap())
        .collect();
    let account_storage_paths: Vec<_> = account_storage_dirs
        .iter()
        .map(|a| a.path().to_path_buf())
        .collect();
    (account_storage_dirs, account_storage_paths)
}

fn generate_geyser_plugin_config() -> (TempDir, PathBuf) {
    let tmp_dir = tempfile::tempdir_in(farf_dir()).unwrap();
    let mut path = tmp_dir.path().to_path_buf();
    path.push("accounts_db_plugin.json");
    let mut config_file = File::create(path.clone()).unwrap();

    // Need to specify the absolute path of the dynamic library
    // as the framework is looking for the library relative to the
    // config file otherwise.
    let lib_name = if std::env::consts::OS == "macos" {
        "libsolana_geyser_plugin_bigtable.dylib"
    } else {
        "libsolana_geyser_plugin_bigtable.so"
    };

    let mut lib_path = path.clone();

    lib_path.pop();
    lib_path.pop();
    lib_path.pop();
    lib_path.push("target");
    lib_path.push("debug");
    lib_path.push(lib_name);

    let lib_path = lib_path.as_os_str().to_str().unwrap();

    let config_content = json!({
        "libpath": lib_path,
        "threads": 20,
        "batch_size": 20,
        "panic_on_db_errors": true,
        "accounts_selector" : {
            "accounts" : ["*"]
        },
        "transaction_selector" : {
            "mentions" : ["*"]
        }
    });

    write!(config_file, "{}", config_content.to_string()).unwrap();
    (tmp_dir, path)
}

#[allow(dead_code)]
struct SnapshotValidatorConfig {
    snapshot_dir: TempDir,
    snapshot_archives_dir: TempDir,
    account_storage_dirs: Vec<TempDir>,
    validator_config: ValidatorConfig,
    plugin_config_dir: TempDir,
}

fn setup_snapshot_validator_config(
    snapshot_interval_slots: u64,
    num_account_paths: usize,
) -> SnapshotValidatorConfig {
    // Create the snapshot config
    let bank_snapshots_dir = tempfile::tempdir_in(farf_dir()).unwrap();
    let snapshot_archives_dir = tempfile::tempdir_in(farf_dir()).unwrap();
    let snapshot_config = SnapshotConfig {
        full_snapshot_archive_interval_slots: snapshot_interval_slots,
        incremental_snapshot_archive_interval_slots: Slot::MAX,
        snapshot_archives_dir: snapshot_archives_dir.path().to_path_buf(),
        bank_snapshots_dir: bank_snapshots_dir.path().to_path_buf(),
        ..SnapshotConfig::default()
    };

    // Create the account paths
    let (account_storage_dirs, account_storage_paths) = generate_account_paths(num_account_paths);

    let (plugin_config_dir, path) = generate_geyser_plugin_config();

    let geyser_plugin_config_files = Some(vec![path]);

    // Create the validator config
    let validator_config = ValidatorConfig {
        snapshot_config: Some(snapshot_config),
        account_paths: account_storage_paths,
        accounts_db_caching_enabled: true,
        accounts_hash_interval_slots: snapshot_interval_slots,
        geyser_plugin_config_files,
        enforce_ulimit_nofile: false,
        ..ValidatorConfig::default()
    };

    SnapshotValidatorConfig {
        snapshot_dir: bank_snapshots_dir,
        snapshot_archives_dir,
        account_storage_dirs,
        validator_config,
        plugin_config_dir,
    }
}

fn test_local_cluster_start_and_exit_with_config(socket_addr_space: SocketAddrSpace) {
    const NUM_NODES: usize = 1;
    let config = ValidatorConfig {
        enforce_ulimit_nofile: false,
        ..ValidatorConfig::default()
    };
    let mut config = ClusterConfig {
        validator_configs: make_identical_validator_configs(&config, NUM_NODES),
        node_stakes: vec![3; NUM_NODES],
        cluster_lamports: 100,
        ticks_per_slot: 8,
        slots_per_epoch: MINIMUM_SLOTS_PER_EPOCH as u64,
        stakers_slot_offset: MINIMUM_SLOTS_PER_EPOCH as u64,
        ..ClusterConfig::default()
    };
    let cluster = LocalCluster::new(&mut config, socket_addr_space);
    assert_eq!(cluster.validators.len(), NUM_NODES);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_bigtable_plugin() {
    solana_logger::setup_with_default(RUST_LOG_FILTER);

    unsafe {
        let filename = match std::env::consts::OS {
            "macos" => "libsolana_geyser_plugin_bigtable.dylib",
            _ => "libsolana_geyser_plugin_bigtable.so",
        };

        let lib = Library::new(filename);
        if lib.is_err() {
            info!("Failed to load the dynamic library {} {:?}", filename, lib);
            return;
        }
    }

    let socket_addr_space = SocketAddrSpace::new(true);
    test_local_cluster_start_and_exit_with_config(socket_addr_space);

    // First set up the cluster with 1 node
    let snapshot_interval_slots = 50;
    let num_account_paths = 3;

    let leader_snapshot_test_config =
        setup_snapshot_validator_config(snapshot_interval_slots, num_account_paths);

    let mut file = File::open(
        &leader_snapshot_test_config
            .validator_config
            .geyser_plugin_config_files
            .as_ref()
            .unwrap()[0],
    )
    .unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    let plugin_config: GeyserPluginBigtableConfig = serde_json::from_str(&contents).unwrap();

    let result = BufferedBigtableClient::connect_to_db(&plugin_config).await;
    if result.is_err() {
        error!("Failed to connecto the Bigtable database. Please setup the database to run the integration tests. {:?}", result.err());
        return;
    }

    info!("Connected to Bigtable!");

    let stake = 10_000;
    let mut config = ClusterConfig {
        node_stakes: vec![stake],
        cluster_lamports: 1_000_000,
        validator_configs: make_identical_validator_configs(
            &leader_snapshot_test_config.validator_config,
            1,
        ),
        ..ClusterConfig::default()
    };

    let cluster = LocalCluster::new(&mut config, socket_addr_space);

    assert_eq!(cluster.validators.len(), 1);
    let contact_info = &cluster.entry_point_info;

    info!("Contact info: {:?}", contact_info);

    // Get slot after which this was generated
    let snapshot_archives_dir = &leader_snapshot_test_config
        .validator_config
        .snapshot_config
        .as_ref()
        .unwrap()
        .snapshot_archives_dir;
    info!("Waiting for snapshot");
    let (archive_filename, archive_snapshot_hash) =
        wait_for_next_snapshot(&cluster, snapshot_archives_dir);
    info!("Found: {:?} {:?}", archive_filename, archive_snapshot_hash);
}
