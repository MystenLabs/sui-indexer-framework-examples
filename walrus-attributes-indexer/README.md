# Walrus Attributes Indexer

Index `"path": {value}"` attribute pairs from `Metadata` dynamic fields on `Blob` objects to build a db instance that can emulate S3-like functionality. This indexer assumes that the `Metadata` dynamic field can only be created or deleted, and otherwise remains immutable. The resulting tables enable the following features:
1. A user can upload files to a file path
    1. Additional uploads to the same file path are valid, and are considered newer versions of the same file
2. A user can retrieve a file by providing the exact file path
    1. This will reference the latest version of the file
3. A user can filter their files by path prefix
4. A user can see a paginated list of all their files
5. A user can delete a file
    1. By default, this deletes the latest version of the file
6. A user can view or delete a file by specifying the file path and version


To run the indexer:

```sh
$ RUST_LOG=info cargo run --release -- \
    --remote-store-url https://checkpoints.mainnet.sui.io
```

Other useful commands:
```sh
walrus blob-status
walrus list-blobs --include-expired
walrus set-blob-attribute {Sui blob object id} --attr "path" {path} --attr "key" {value}
```

```sh
$ diesel setup                                                                \
    --database-url=... \
    --migration-dir migrations
$ diesel migration run                                                        \
    --database-url=... \
    --migration-dir migrations
diesel database reset --database-url=... --migration-dir migrations
```
