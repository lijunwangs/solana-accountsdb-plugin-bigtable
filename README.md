# solana-geyser-plugin-bigtable
The `solana-geyser-plugin-bigtable` crate implements a plugin storing
account, block, transaction data to a Bigtable database using the 
[Geyser Plugin Framework](https://docs.solana.com/developing/plugins/geyser_plugin).


### Configuration File Format

The plugin is configured using the input configuration file. An example
configuration file looks like the following:

```
{
    "libpath": "/solana/target/release/libsolana_geyser_plugin_bigtable.so",
	"credential_path": "/home/solana/geyser-big-table-creds.json",
	"instance": "geyser-bigtable",
	"threads": 80,
	"batch_size": 20,
	"panic_on_db_errors": true,
	"accounts_selector" : {
           "accounts" : ["*"]
    },
}
```

`credential_path` specifies the path of the Bigtable credential JSON file.
Please refer to https://cloud.google.com/docs/authentication/getting-started
on creating a service account key.
It must be in the format specified by https://github.com/durch/rust-goauth. For example:

```
{
   "type": "service_account",
   "project_id": "dummy",
   "private_key_id": "dummy",
   "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
   "client_email": "dummy@developer.gserviceaccount.com",
   "client_id": "dummy",
   "auth_uri": "https://accounts.google.com/o/oauth2/auth",
   "token_uri": "https://accounts.google.com/o/oauth2/token",
   "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
   "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/457015483506-compute%40developer.gserviceaccount.com"
}

```

The `instance` specifies the Bigtable instance name.

To improve the throughput to the database, the plugin supports connection pooling
using multiple threads, each maintaining a connection to the PostgreSQL database.
The count of the threads is controlled by the `threads` field. A higher thread
count usually offers better performance.

To further improve performance when saving large numbers of accounts at
startup, the plugin uses bulk inserts. The batch size is controlled by the
`batch_size` parameter. This can help reduce the round trips to the database.

The `panic_on_db_errors` can be used to panic the validator in case of database
errors to ensure data consistency.


### Account Selection

The `accounts_selector` can be used to filter the accounts that should be persisted.

For example, one can use the following to persist only the accounts with particular
Base58-encoded Pubkeys,

```
    "accounts_selector" : {
         "accounts" : ["pubkey-1", "pubkey-2", ..., "pubkey-n"],
    }
```

Or use the following to select accounts with certain program owners:

```
    "accounts_selector" : {
         "owners" : ["pubkey-owner-1", "pubkey-owner-2", ..., "pubkey-owner-m"],
    }
```

To select all accounts, use the wildcard character (*):

```
    "accounts_selector" : {
         "accounts" : ["*"],
    }
```

### BigTable Setup

#### Development Environment
The Cloud BigTable emulator can be used during development/test.  See
https://cloud.google.com/bigtable/docs/emulator for general setup information.

Process:
1. Make sure install GCP CLI, see https://cloud.google.com/sdk/docs/install-sdk.
2. Install the Bigtable CLI CBT https://cloud.google.com/bigtable/docs/cbt-overview.
3. Run `gcloud beta emulators bigtable start` in the background
4. Run `$(gcloud beta emulators bigtable env-init)` to establish the `BIGTABLE_EMULATOR_HOST` environment variable
5. Run `./scripts/init-bigtable.sh` to configure the emulator
6. Develop/test

#### Production Environment
Export a standard `GOOGLE_APPLICATION_CREDENTIALS` environment variable to your
service account credentials.  The project should contain a BigTable instance
configured in the config file must have been initialized by running the `./init-bigtable.sh` script.

Depending on what operation mode is required, either the
`https://www.googleapis.com/auth/bigtable.data` or
`https://www.googleapis.com/auth/bigtable.data.readonly` OAuth scope will be
requested using the provided credentials.

### Object Models

Account and slot metadata are supported with plan to support transaction data, block metadata and account secondary indexes.

The storage-proto contains the gRPC models for the objects. For example for accounts:

```
message account {
    bytes pubkey = 1;
    bytes owner = 2;
    uint64 lamports = 3;
    uint64 slot = 4;
    bool executable = 5;
    uint64 rent_epoch = 6;
    bytes data = 7;
    uint64 write_version = 8;
    UnixTimestamp updated_on = 9;
}
```

The following are the tables in the Postgres database

| Table         | Description             |
|:--------------|:------------------------|
| account       | Account data            |
| slot          | Slot metadata           |


The model data is encoded into binary format and then compressed using `compress_best`
src/compression.rs.