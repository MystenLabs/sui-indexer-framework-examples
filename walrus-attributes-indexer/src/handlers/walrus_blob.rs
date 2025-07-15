use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use anyhow::{self, Context};
use diesel::prelude::*;
use diesel::query_dsl::methods::FilterDsl;
use diesel::upsert::excluded;
use diesel_async::RunQueryDsl;
use serde::{Deserialize, Serialize};
use sui_indexer_alt_framework::pipeline::{concurrent::Handler, Processor};
use sui_indexer_alt_framework::postgres;
use sui_indexer_alt_framework::types::base_types::{ObjectID, SequenceNumber};
use sui_indexer_alt_framework::types::collection_types::VecMap;
use sui_indexer_alt_framework::types::dynamic_field::Field;
use sui_indexer_alt_framework::types::effects::TransactionEffectsAPI;
use sui_indexer_alt_framework::types::full_checkpoint_content::CheckpointData;
use sui_indexer_alt_framework::types::id::UID;
use sui_indexer_alt_framework::types::object::{Object, Owner};
use sui_indexer_alt_framework::types::parse_sui_struct_tag;
use sui_indexer_alt_framework::FieldCount;
use sui_indexer_alt_framework::Result;

use crate::schema::{walrus_blob, walrus_blob_historical};

// ============================================================================
// WALRUS BLOB DESERIALIZATION TYPES
// ============================================================================
// These types represent the structure of Walrus blob data as it exists on-chain.
// They are used for deserializing Move objects into Rust structs.

#[derive(Debug, Serialize, Deserialize)]
pub struct Blob {
    id: UID,
    registered_epoch: u32,
    blob_id: BlobId,
    size: u64,
    encoding_type: u8,
    // Stores the epoch first certified.
    certified_epoch: Option<u32>,
    storage: StorageResource,
    // Marks if this blob can be deleted.
    deletable: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StorageResource {
    id: UID,
    start_epoch: u32,
    end_epoch: u32,
    storage_size: u64,
}

/// The ID of a blob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[repr(transparent)]
pub struct BlobId(pub [u8; 32]);

#[derive(Debug, Serialize, Deserialize)]
pub struct DynamicFieldName(Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
pub struct BlobAttribute {
    metadata: VecMap<String, String>,
}

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
    address_owner: Vec<u8>,
    /// The file path of the Blob object that owns the Metadata dynamic field.
    file_path: String,
    /// The Blob ID to be used to fetch the Walrus blob. This can be selected in postgres with:
    ///
    /// SELECT replace(replace(rtrim(encode(blob_id, 'base64'), '='), '+', '-'), '/', '_') as
    /// blob_id FROM walrus_blob;
    blob_id: Vec<u8>,
    /// The ID of the owner of the Blob object that owns the Metadata dynamic field.
    owner_id: Vec<u8>,
    /// The ID of the Metadata dynamic field.
    dynamic_field_id: Vec<u8>,
    /// The checkpoint sequence number this update occurred in.
    cp_sequence_number: i64,
    /// Sentinel value to indicate whether the record is a tombstone.
    deleted: bool,
}

/// Representation of a row from the `walrus_blob_historical` table, which tracks historical changes
/// to relevant Metadata dynamic fields. This is almost identical to the StoredWalrusBlob struct,
/// except that this struct does not use a `deleted` sentinel value, but rather records deletions
/// with optional columns set to NULL.
#[derive(Insertable, Debug, FieldCount, Clone)]
#[diesel(table_name = walrus_blob_historical)]
#[diesel(treat_none_as_null = true)]
pub struct StoredWalrusBlobHistorical {
    /// The ID of the address that owns the Blob object.
    address_owner: Option<Vec<u8>>,
    /// The file path of the Blob object that owns the Metadata dynamic field.
    file_path: Option<String>,
    /// The Blob ID to be used to fetch the Walrus blob. This can be selected in postgres with:
    ///
    /// SELECT replace(replace(rtrim(encode(blob_id, 'base64'), '='), '+', '-'), '/', '_') as
    /// blob_id FROM walrus_blob;
    blob_id: Option<Vec<u8>>,
    /// The ID of the owner of the Blob object that owns the Metadata dynamic field.
    owner_id: Option<Vec<u8>>,
    /// The ID of the Metadata dynamic field.
    dynamic_field_id: Vec<u8>,
    /// The checkpoint sequence number this update occurred in.
    cp_sequence_number: i64,
}

// ============================================================================
// PROCESSING TYPES
// ============================================================================
// These types represent intermediate data structures used during processing.
// They bridge between the raw on-chain data and the database storage format.

/// The data of interest from processing a checkpoint, consisting of the Sui Blob object and
pub struct ProcessedWalrusMetadata {
    /// The Blob parent object that owns the Metadata dynamic field. TODO holds important content
    parent_object: Object,
    /// The ID of the Metadata dynamic field. mainly for reference
    dynamic_field_id: ObjectID,
    /// TODO
    file_path: String,
    /// The checkpoint sequence number this update occurred in.
    cp_sequence_number: i64,
    /// Indicates whether the object was wrapped or deleted in this checkpoint.
    deleted: bool,
}

pub struct WalrusBlobPipeline;

impl Processor for WalrusBlobPipeline {
    const NAME: &'static str = "walrus_blob";

    type Value = ProcessedWalrusMetadata;

    fn process(&self, checkpoint: &Arc<CheckpointData>) -> Result<Vec<Self::Value>> {
        let cp_sequence_number = checkpoint.checkpoint_summary.sequence_number;
        let checkpoint_input_objects = checkpoint_input_objects(checkpoint)?;
        let latest_live_output_objects = checkpoint
            .latest_live_output_objects()
            .into_iter()
            .map(|o| (o.id(), o))
            .collect::<BTreeMap<_, _>>();
        let mut values: BTreeMap<ObjectID, Self::Value> = BTreeMap::new();

        for (object_id, object) in checkpoint_input_objects.iter() {
            // If an object appears only in the input objects, it must have been deleted or wrapped.
            if !latest_live_output_objects.contains_key(object_id) {
                // We only care to emit a record for `Metadata` dynamic fields with the key-value
                // attribute of interest.
                let Some((file_path, parent_id)) = extract_file_path_and_parent_id(object) else {
                    continue;
                };

                // The parent object must also exist at least as the input into the checkpoint. We
                // can consult the input state to determine the correct record to update on the main
                // table.
                let Some(parent_object) = checkpoint_input_objects.get(&parent_id) else {
                    tracing::error!("Parent object {} not found among input objects", parent_id);
                    continue;
                };

                // If an input object is not in the latest live output objects, it must have been
                // deleted or wrapped in this checkpoint. An entry is written for this deletion, so
                // that we don't include the deleted object when querying.
                values.insert(
                    *object_id,
                    ProcessedWalrusMetadata {
                        cp_sequence_number: cp_sequence_number as i64,
                        parent_object: (*parent_object).clone(),
                        dynamic_field_id: *object_id,
                        file_path,
                        deleted: true,
                    },
                );
            }
        }

        for (object_id, object) in latest_live_output_objects.iter() {
            if let Some((file_path, parent_id)) = extract_file_path_and_parent_id(object) {
                // Ignore mutations to the dynamic field.
                if let Some(_) = checkpoint_input_objects.get(object_id) {
                    continue;
                }

                // The parent object must also exist: retrieve it for address_owner, blob_id, and
                // other fields.
                let Some(parent_object) = latest_live_output_objects.get(&parent_id) else {
                    tracing::error!("Parent object {} not found among output objects", parent_id);
                    continue;
                };

                values.insert(
                    *object_id,
                    ProcessedWalrusMetadata {
                        cp_sequence_number: cp_sequence_number as i64,
                        parent_object: (*parent_object).clone(),
                        dynamic_field_id: *object_id,
                        file_path,
                        deleted: false,
                    },
                );
            }
        }

        Ok(values.into_values().collect())
    }
}

#[async_trait::async_trait]
impl Handler for WalrusBlobPipeline {
    type Store = postgres::Db;

    async fn commit<'a>(
        values: &[Self::Value],
        conn: &mut postgres::Connection<'a>,
    ) -> Result<usize> {
        let (upserts, deletes): (Vec<StoredWalrusBlob>, Vec<StoredWalrusBlob>) = values
            .into_iter()
            .map(|v| v.try_into())
            // Even though we end up having to iterate twice, we early return on any conversion
            // error.
            .collect::<Result<Vec<StoredWalrusBlob>>>()?
            .into_iter()
            .partition(|commit| !commit.deleted);

        let mut total_affected = 0;

        if !upserts.is_empty() {
            total_affected += diesel::insert_into(walrus_blob::table)
                .values(&upserts)
                .on_conflict((walrus_blob::address_owner, walrus_blob::file_path))
                .do_update()
                .set((
                    walrus_blob::dynamic_field_id.eq(excluded(walrus_blob::dynamic_field_id)),
                    walrus_blob::cp_sequence_number.eq(excluded(walrus_blob::cp_sequence_number)),
                    walrus_blob::owner_id.eq(excluded(walrus_blob::owner_id)),
                    walrus_blob::address_owner.eq(excluded(walrus_blob::address_owner)),
                    walrus_blob::file_path.eq(excluded(walrus_blob::file_path)),
                    walrus_blob::blob_id.eq(excluded(walrus_blob::blob_id)),
                    walrus_blob::deleted.eq(false),
                ))
                .filter(
                    walrus_blob::cp_sequence_number.lt(excluded(walrus_blob::cp_sequence_number)),
                )
                .execute(conn)
                .await?;

            let historical_upserts: Vec<StoredWalrusBlobHistorical> =
                upserts.iter().map(|v| v.into()).collect();

            // All updates are recorded in the historical table.
            total_affected += diesel::insert_into(walrus_blob_historical::table)
                .values(historical_upserts)
                .on_conflict_do_nothing()
                .execute(conn)
                .await?;
        }

        if deletes.is_empty() {
            tracing::info!("No deletes found");
        }

        if !deletes.is_empty() {
            total_affected += diesel::insert_into(walrus_blob::table)
                .values(&deletes)
                .on_conflict((walrus_blob::address_owner, walrus_blob::file_path))
                .do_update()
                .set((
                    walrus_blob::deleted.eq(true),
                    walrus_blob::cp_sequence_number.eq(excluded(walrus_blob::cp_sequence_number)),
                ))
                .filter(
                    walrus_blob::cp_sequence_number.lt(excluded(walrus_blob::cp_sequence_number)),
                )
                .execute(conn)
                .await?;

            let historical_deletes: Vec<StoredWalrusBlobHistorical> =
                deletes.iter().map(|v| v.into()).collect();

            total_affected += diesel::insert_into(walrus_blob_historical::table)
                .values(historical_deletes)
                .on_conflict_do_nothing()
                .execute(conn)
                .await?;
        }

        Ok(total_affected)
    }
}

impl FieldCount for ProcessedWalrusMetadata {
    const FIELD_COUNT: usize = StoredWalrusBlob::FIELD_COUNT;
}

impl TryInto<StoredWalrusBlob> for &ProcessedWalrusMetadata {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<StoredWalrusBlob> {
        let blob_object: Blob = bcs::from_bytes(
            self.parent_object
                .data
                .try_as_move()
                .ok_or_else(|| anyhow::anyhow!("Parent object is not a Move object"))?
                .contents(),
        )?;

        let address_owner = match self.parent_object.owner() {
            Owner::AddressOwner(id) => id.to_vec(),
            _ => {
                return Err(anyhow::anyhow!(
                    "Parent object's owner is not an address owner"
                ))
            }
        };

        Ok(StoredWalrusBlob {
            dynamic_field_id: self.dynamic_field_id.to_vec(),
            cp_sequence_number: self.cp_sequence_number as i64,
            file_path: self.file_path.clone(),
            owner_id: self.parent_object.id().to_vec(),
            address_owner,
            blob_id: blob_object.blob_id.0.to_vec(),
            deleted: self.deleted,
        })
    }
}

impl From<&StoredWalrusBlob> for StoredWalrusBlobHistorical {
    fn from(value: &StoredWalrusBlob) -> Self {
        if value.deleted {
            return StoredWalrusBlobHistorical {
                dynamic_field_id: value.dynamic_field_id.clone(),
                cp_sequence_number: value.cp_sequence_number,
                owner_id: None,
                address_owner: None,
                file_path: None,
                blob_id: None,
            };
        }
        StoredWalrusBlobHistorical {
            dynamic_field_id: value.dynamic_field_id.clone(),
            cp_sequence_number: value.cp_sequence_number,
            owner_id: Some(value.owner_id.clone()),
            address_owner: Some(value.address_owner.clone()),
            file_path: Some(value.file_path.clone()),
            blob_id: Some(value.blob_id.clone()),
        }
    }
}

/// Returns the first appearance of all objects that were used as inputs to the transactions in the
/// checkpoint. These are objects that existed prior to the checkpoint, and excludes objects that
/// were created or unwrapped within the checkpoint.
pub fn checkpoint_input_objects(
    checkpoint: &CheckpointData,
) -> anyhow::Result<BTreeMap<ObjectID, &Object>> {
    let mut output_objects_seen = HashSet::new();
    let mut checkpoint_input_objects = BTreeMap::new();
    for tx in checkpoint.transactions.iter() {
        let input_objects_map: BTreeMap<(ObjectID, SequenceNumber), &Object> = tx
            .input_objects
            .iter()
            .map(|obj| ((obj.id(), obj.version()), obj))
            .collect();

        for change in tx.effects.object_changes() {
            let id = change.id;

            let Some(version) = change.input_version else {
                continue;
            };

            // This object was previously modified, created, or unwrapped in the checkpoint, so
            // this version is not a checkpoint input.
            if output_objects_seen.contains(&id) {
                continue;
            }

            // Make sure this object has not already been recorded as an input.
            let Entry::Vacant(entry) = checkpoint_input_objects.entry(id) else {
                continue;
            };

            let input_obj = input_objects_map
                .get(&(id, version))
                .copied()
                .with_context(|| format!(
                    "Object {id} at version {version} referenced in effects not found in input_objects"
                ))?;

            entry.insert(input_obj);
        }

        for change in tx.effects.object_changes() {
            if change.output_version.is_some() {
                output_objects_seen.insert(change.id);
            }
        }
    }
    Ok(checkpoint_input_objects)
}

/// Extract the file path from the object if it is a walrus metadata dynamic field, otherwise return
/// None.
pub fn extract_file_path_and_parent_id(object: &Object) -> Option<(String, ObjectID)> {
    let Some((metadata, parent_id)) = get_metadata(object).ok()? else {
        return None;
    };

    let Some(file_path) = metadata
        .metadata
        .get(&"path".to_string())
        .map(|s| s.to_string())
    else {
        return None;
    };

    Some((file_path, parent_id))
}

/// Try to deserialize the object as a Walrus Metadata dynamic field, and return the deserialized
/// data and the parent object ID, or return None if it is not.
pub fn get_metadata(object: &Object) -> anyhow::Result<Option<(BlobAttribute, ObjectID)>> {
    let Some(type_) = object.type_() else {
        return Ok(None);
    };

    if let Owner::ObjectOwner(parent_id) = object.owner() {
        // The expected type of the dynamic field is a `Field<DynamicFieldName, BlobAttribute>`.
        let expected_obj_type = parse_sui_struct_tag(
            "0x2::dynamic_field::Field<vector<u8>, 0xfdc88f7d7cf30afab2f82e8380d11ee8f70efb90e863d1de8616fae1bb09ea77::metadata::Metadata>"
        )?;

        if type_.is(&expected_obj_type) {
            let move_object = object
                .data
                .try_as_move()
                .ok_or_else(|| anyhow::anyhow!("Not a Move object"))?;

            let field: Field<DynamicFieldName, BlobAttribute> =
                match bcs::from_bytes(move_object.contents()) {
                    Ok(field) => field,
                    Err(e) => {
                        tracing::error!("Failed to deserialize: {:?}", e);
                        return Err(anyhow::anyhow!("Failed to deserialize: {}", e));
                    }
                };

            Ok(Some((field.value, (*parent_id).into())))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}
