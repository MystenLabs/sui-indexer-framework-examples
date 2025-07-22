use serde::{Deserialize, Serialize};
use sui_indexer_alt_framework::types::collection_types::VecMap;
use sui_indexer_alt_framework::types::id::UID;

// ============================================================================
// WALRUS BLOB DESERIALIZATION TYPES
// ============================================================================
// These types represent the structure of Walrus blob data as it exists on-chain.
// They are used for deserializing Move objects into Rust structs.

#[derive(Debug, Serialize, Deserialize)]
pub struct Blob {
    pub id: UID,
    pub registered_epoch: u32,
    pub blob_id: BlobId,
    pub size: u64,
    pub encoding_type: u8,
    // Stores the epoch first certified.
    pub certified_epoch: Option<u32>,
    pub storage: StorageResource,
    // Marks if this blob can be deleted.
    pub deletable: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StorageResource {
    pub id: UID,
    pub start_epoch: u32,
    pub end_epoch: u32,
    pub storage_size: u64,
}

/// The ID of a blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[repr(transparent)]
pub struct BlobId(pub [u8; 32]);

#[derive(Debug, Serialize, Deserialize)]
pub struct DynamicFieldName(pub Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
pub struct BlobAttribute {
    pub metadata: VecMap<String, String>,
}
