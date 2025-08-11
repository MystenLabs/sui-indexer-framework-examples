# Walrus Attributes Indexer

## Quickstart

Index `view_count` and `title` attributes from `Metadata` dynamic fields on `Blob` objects to build a db instance that can emulate a blog post platform. Users can:
- Upload blog posts with titles
- View their own posts and metrics
- Delete posts they created
- Edit post titles
- Browse posts by other publishers


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
walrus set-blob-attribute {Sui blob object id} --attr "title" {title} --attr "view_count" {view_count}
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

## Blog Post Pipeline

The Blog Post pipeline is a sequential pipeline that writes the latest state of the `Metadata` dynamic fields to the `blog_post` table. It operates on a checkpoint granularity, and upserts records such that only the final update to an object in a checkpoint is persisted.

## Chain-agnostic Indexer

For the purpose of this guide, the StructTag of the `Metadata` dynamic field is hardcoded in `main.rs`. Ideally, in a production deployment, this should be a value that is passed to the service.

## Defaults

As of writing, the SequentialConfig is defined [here](https://github.com/MystenLabs/sui/blob/main/crates/sui-indexer-alt-framework/src/pipeline/sequential/mod.rs#L68) consisting of a committer config and a checkpoint lag. The default values set `checkpoint_lag` to 0, and the committer config as follows:
```
/// Configuration for a sequential pipeline
#[derive(Serialize, Deserialize, Clone, Default)]
pub struct SequentialConfig {
    /// Configuration for the writer, that makes forward progress.
    pub committer: CommitterConfig,

    /// How many checkpoints to hold back writes for.
    pub checkpoint_lag: u64,
}

// Defaults
impl Default for CommitterConfig {
    fn default() -> Self {
        Self {
            write_concurrency: 5,
            collect_interval_ms: 500,
            watermark_interval_ms: 500,
        }
    }
}
```

The ingestion config is defined [here](https://github.com/MystenLabs/sui/blob/main/crates/sui-indexer-alt-framework/src/ingestion/mod.rs#L59) with defaults configured to:
```
impl Default for IngestionConfig {
    fn default() -> Self {
        Self {
            checkpoint_buffer_size: 5000,
            ingest_concurrency: 200,
            retry_interval_ms: 200,
        }
    }
}
```

This means that by default, the blog post pipeline will have a write concurrency of 5, and the regulator will buffer at most 5000 checkpoints from the latest checkpoint committed by the blog post pipeline.
