-- This table maps file paths to their latest dynamic field metadata. The dynamic_field_id
-- references a Metadata dynamic field on a Sui Blob object, which gives us access to both the
-- blob_id pointing to the actual file contents and the address_owner of the Blob object.
CREATE TABLE IF NOT EXISTS walrus_blob (
    -- Address that owns the Blob object on Sui
    address_owner               BYTEA         NOT NULL,
    -- File path associated with the Walrus Blob
    file_path                   TEXT          NOT NULL,
    -- The blob ID deterministically derived from the content of a blob and the Walrus configuration
    blob_id                     BYTEA         NOT NULL,
    -- ObjectID of the Blob object on Sui that owns the Metadata dynamic field
    owner_id                    BYTEA         NOT NULL,
    -- ID of the Metadata dynamic field
    dynamic_field_id            BYTEA         NOT NULL,
    -- Checkpoint sequence number that produced the state of the dynamic field
    cp_sequence_number          BIGINT        NOT NULL,
    -- Version of the Metadata dynamic field
    df_version                  BIGINT        NOT NULL,
    -- Sentinel value to indicate whether the record is a tombstone
    deleted                     BOOLEAN       DEFAULT FALSE,
    PRIMARY KEY (address_owner, file_path)
);

-- This index supports querying on a path prefix for the blob_id. The partial index ignores records
-- marked for deletion.
CREATE INDEX IF NOT EXISTS walrus_blob_covering_idx ON walrus_blob
(address_owner, file_path TEXT_PATTERN_OPS)
INCLUDE (blob_id)
WHERE deleted = FALSE;

-- This index supports the pruner, which selects deletable records within a checkpoint range.
CREATE INDEX IF NOT EXISTS walrus_blob_can_delete_idx ON walrus_blob
(cp_sequence_number, deleted)
WHERE deleted = TRUE;


-- This table tracks historical changes to relevant Metadata dynamic fields. Unlike the main table,
-- the historical table is keyed on dynamic_field_id and df_version to capture the full history of
-- object changes.
CREATE TABLE IF NOT EXISTS walrus_blob_historical (
    -- ID of the Metadata dynamic field of key-value attributes on the Blob object
    dynamic_field_id            BYTEA         NOT NULL,
    -- Version of the Metadata dynamic field
    df_version                  BIGINT        NOT NULL,
    cp_sequence_number          BIGINT        NOT NULL,
    -- ObjectID of the Blob object on Sui that owns the Metadata dynamic field
    owner_id                    BYTEA,
    -- Address that owns the Blob object on Sui
    address_owner               BYTEA,
    file_path                   TEXT,
    -- The blob ID deterministically derived from the content of a blob and the Walrus configuration
    blob_id                     BYTEA,
    PRIMARY KEY (dynamic_field_id, df_version)
);

-- This index supports querying previous versions of a file on the exact file_path.
CREATE INDEX IF NOT EXISTS walrus_blob_historical_version_idx ON walrus_blob_historical
(address_owner, file_path, df_version DESC)
WHERE file_path IS NOT NULL;

-- Pruning on the historical table can be done by simply dropping records within the pruning range.
CREATE INDEX IF NOT EXISTS walrus_blob_historical_cp_sequence_number_idx ON walrus_blob_historical
(cp_sequence_number);
