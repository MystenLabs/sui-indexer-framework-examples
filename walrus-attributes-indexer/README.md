# Walrus Attributes Indexer

Index `"path": {value}"` attribute pairs from `Metadata` dynamic fields on `Blob` objects to build a db instance that can emulate S3-like functionality. This indexer assumes that the `Metadata` dynamic field can only be created or deleted, and otherwise remains immutable. The resulting tables enable the user to:
1. upload files to a path,
   - Additional uploads to the same file path are valid, and are considered newer versions of the same file
2. retrieve or delete a file at a path,
   - If a version is provided, that version is retrieved or deleted, otherwise defaults to the latest version.
3. paginate their files, optionally filtered by path prefix.


To run the indexer:

```sh
$ RUST_LOG=info cargo run --release -- \
    --remote-store-url https://checkpoints.mainnet.sui.io
```

Other useful commands:
```sh
# Get the status of a blob, such as its expiry epoch, when it was initially certified, etc.
walrus blob-status --blob-id {BLOB_ID}
# List all blobs for the current address, including expired ones.
walrus list-blobs --include-expired
# Set a path: value attribute pair on the Metadata dynamic field of a Blob object on Sui.
walrus set-blob-attribute {Sui blob object id} --attr "path" {path} --attr "key" {value}
```

```sh
# Creates a database and sets up the __diesel_schema_migrations table. Does not run any migrations.
diesel setup                                                                \
    --database-url=... \
    --migration-dir migrations
# Applies all pending migrations and updates the __diesel_schema_migrations table.
diesel migration run                                                        \
    --database-url=... \
    --migration-dir migrations
# Drops the entire database and recreates it from scratch by running all migrations from the beginning. Deletes all existing data.
diesel database reset --database-url=... --migration-dir migrations
```
