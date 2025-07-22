use anyhow::{self, Context};
use move_core_types::language_storage::StructTag;
use serde::{Deserialize, Serialize};
use sui_indexer_alt_framework::types::base_types::ObjectID;
use sui_indexer_alt_framework::types::collection_types::VecMap;
use sui_indexer_alt_framework::types::dynamic_field::Field;
use sui_indexer_alt_framework::types::id::UID;
use sui_indexer_alt_framework::types::object::{Object, Owner};

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

/// Try to deserialize the object as a Walrus Metadata dynamic field, and return the
/// deserialized data and the parent object ID, or return None if it is not.
pub fn get_metadata(
    tag: &StructTag,
    object: &Object,
) -> anyhow::Result<Option<(BlobAttribute, ObjectID)>> {
    // Must be a MoveObject
    let Some(type_) = object.type_() else {
        return Ok(None);
    };

    // Dynamic fields must have an ObjectOwner
    let Owner::ObjectOwner(parent_id) = object.owner() else {
        return Ok(None);
    };

    // The expected type of the dynamic field is a `Field<DynamicFieldName, BlobAttribute>`.
    if !type_.is(tag) {
        return Ok(None);
    }

    let move_object = object
        .data
        .try_as_move()
        .ok_or_else(|| anyhow::anyhow!("Not a Move object"))?;

    // This is called during `process`, so the indexing framework can trace the error
    let field: Field<DynamicFieldName, BlobAttribute> =
        bcs::from_bytes(move_object.contents()).context("Failed to deserialize")?;

    Ok(Some((field.value, (*parent_id).into())))
}

/// Extract the file path from the object if it is a walrus metadata dynamic field, otherwise
/// return None.
pub fn extract_file_path_and_parent_id(
    tag: &StructTag,
    object: &Object,
) -> Option<(String, ObjectID)> {
    let (metadata, parent_id) = get_metadata(tag, object).ok()??;
    let file_path = metadata.metadata.get(&"path".to_owned())?.to_string();

    Some((file_path, parent_id))
}
