use diesel::prelude::*;
use sui_indexer_alt_framework::FieldCount;

use crate::schema::{walrus_blob, walrus_blob_historical};

// ============================================================================
// DATABASE STORAGE TYPES
// ============================================================================
// These types represent the structure of data as it's stored in the database.
// They map directly to database tables and include Diesel annotations.

/// Representation of a row from the `walrus_blob` table, which maps file paths to their latest
/// dynamic field metadata.
#[derive(Insertable, Debug, FieldCount, Clone)]
#[diesel(table_name = walrus_blob)]
pub struct StoredWalrusBlob {
    /// The ID of the address that owns the Blob object.
    pub address_owner: Vec<u8>,
    /// The file path of the Blob object that owns the Metadata dynamic field.
    pub file_path: String,
    /// The Blob ID to be used to fetch the Walrus blob. This can be selected in postgres with:
    ///
    /// SELECT replace(replace(rtrim(encode(blob_id, 'base64'), '='), '+', '-'), '/', '_') as
    /// blob_id FROM walrus_blob;
    pub blob_id: Vec<u8>,
    /// The ID of the owner of the Blob object that owns the Metadata dynamic field.
    pub owner_id: Vec<u8>,
    /// The ID of the Metadata dynamic field.
    pub dynamic_field_id: Vec<u8>,
    /// The version of the Metadata dynamic field.
    /// The checkpoint sequence number this update occurred in.
    pub cp_sequence_number: i64,
    /// Sentinel value to indicate whether the record is a tombstone.
    pub deleted: bool,
}

/// Representation of a row from the `walrus_blob_historical` table, which tracks historical changes
/// to relevant Metadata dynamic fields. This is almost identical to the StoredWalrusBlob struct,
/// except that this struct does not use a `deleted` sentinel value, but rather records deletions
/// with optional columns set to NULL.
#[derive(Insertable, Debug, FieldCount, Clone)]
#[diesel(table_name = walrus_blob_historical)]
#[diesel(treat_none_as_null = true)]
pub struct StoredWalrusBlobHistorical {
    /// The ID of the Metadata dynamic field.
    pub dynamic_field_id: Vec<u8>,
    /// The version of the Metadata dynamic field.
    pub df_version: i64,
    /// The checkpoint sequence number this update occurred in.
    pub cp_sequence_number: i64,
    /// The ID of the address that owns the Blob object.
    pub address_owner: Option<Vec<u8>>,
    /// The file path of the Blob object that owns the Metadata dynamic field.
    pub file_path: Option<String>,
    /// The Blob ID to be used to fetch the Walrus blob. This can be selected in postgres with:
    ///
    /// SELECT replace(replace(rtrim(encode(blob_id, 'base64'), '='), '+', '-'), '/', '_') as
    /// blob_id FROM walrus_blob;
    pub blob_id: Option<Vec<u8>>,
    /// The ID of the owner of the Blob object that owns the Metadata dynamic field.
    pub owner_id: Option<Vec<u8>>,
}
