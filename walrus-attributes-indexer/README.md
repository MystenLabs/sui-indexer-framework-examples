# Walrus Attributes Indexer

## Quickstart

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

## Walrus Blob Pipeline

The Walrus Blob Pipeline is a concurrent pipeline that writes the latest state of the `Metadata` dynamic fields to the `walrus_blob` table. It operates on a checkpoint granularity, so any modifications made to the same dynamic field within a checkpoint are not reflected on the table, and only the final update is persisted. This is fine since the indexer assumes that `Metadata` dynamic fields can only be created or deleted.

On commit, we handle out-of-order writes by using the `address_owner` and `file_path` columns as the primary key, and filtering on the `cp_sequence_number` column to ensure that on constraint violation, we persist the update only if it is newer than the existing row.

## Walrus Blob Historical Pipeline

This pipeline is also a concurrent pipeline, but unlike the Walrus Blob Pipeline, it writes all `Metadata` modifications to the `walrus_blob_historical` table. For some object that was created, mutated, or unwrapped, we can construct the relevant data directly from the output contents of its parent and itself. On the other hand, when an object is deleted or wrapped, it will not have an output version or contents, which complicates the indexing process. To avoid indexing more objects than necessary, we check the object's input state, and if it is a relevant `Metadata` dynamic field, then we write a tombstone record to the table.

## Chain-agnostic Indexer

For the purpose of this guide, the StructTag of the `Metadata` dynamic field is hardcoded in
`main.rs`. Ideally, in a production deployment, this should be a value that is passed to the
service.
