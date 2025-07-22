// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use anyhow::{self, bail, Context};
use diesel::prelude::*;
use diesel::query_dsl::methods::FilterDsl;
use diesel::upsert::excluded;
use diesel_async::RunQueryDsl;
use move_core_types::language_storage::StructTag;
use sui_indexer_alt_framework::pipeline::{concurrent::Handler, Processor};
use sui_indexer_alt_framework::postgres;
use sui_indexer_alt_framework::types::base_types::{ObjectID, SequenceNumber};
use sui_indexer_alt_framework::types::effects::TransactionEffectsAPI;
use sui_indexer_alt_framework::types::full_checkpoint_content::CheckpointData;
use sui_indexer_alt_framework::types::object::{Object, Owner};
use sui_indexer_alt_framework::types::parse_sui_struct_tag;
use sui_indexer_alt_framework::FieldCount;
use sui_indexer_alt_framework::Result;

use crate::schema::walrus_blob;
use crate::storage::StoredWalrusBlob;
use crate::types::{extract_file_path_and_parent_id, Blob};

// ============================================================================
// PROCESSING TYPES
// ============================================================================
// These types represent intermediate data structures used during processing.
// They bridge between the raw on-chain data and the database storage format.

/// Struct representing the data of interest transformed from processing the checkpoint to be passed
/// to the committer implementation.
pub struct ProcessedWalrusMetadata {
    /// The Blob parent object that owns the Metadata dynamic field.
    parent_object: Object,
    /// The ID of the Metadata dynamic field.
    dynamic_field_id: ObjectID,
    /// The user-provided file path to be associated with the blob.
    file_path: String,
    /// The checkpoint sequence number this update occurred in.
    cp_sequence_number: i64,
    /// Indicates whether the object was wrapped or deleted in this checkpoint.
    deleted: bool,
}

pub struct WalrusBlobPipeline {
    metadata_type: StructTag,
}

impl Processor for WalrusBlobPipeline {
    const NAME: &'static str = "walrus_blob";

    type Value = ProcessedWalrusMetadata;

    fn process(&self, checkpoint: &Arc<CheckpointData>) -> Result<Vec<Self::Value>> {
        let cp_sequence_number = checkpoint.checkpoint_summary.sequence_number;
        let checkpoint_input_objects = checkpoint_input_objects(checkpoint)?;
        let latest_live_output_objects = checkpoint_output_objects(checkpoint)?;
        // Collect values to be passed to committer.
        let mut values: BTreeMap<ObjectID, Self::Value> = BTreeMap::new();

        // Process relevant objects that were wrapped or deleted in this checkpoint.
        for (object_id, object) in checkpoint_input_objects.iter() {
            if latest_live_output_objects.contains_key(object_id) {
                continue;
            }

            // We only care to emit a record for `Metadata` dynamic fields if they have the "path"
            // key-value attribute.
            let Some((file_path, parent_id)) =
                extract_file_path_and_parent_id(&self.metadata_type, object)
            else {
                continue;
            };

            // The parent object must also exist at least as the input into the checkpoint. We need
            // to consult the input state of the parent object to determine the address_owner and
            // file_path to correctly update the existing entry on the table into a sentinel row.
            // Because we are not tracking object mutations, there should not be any interim
            // mutations to the objects, so it is correct for us to look at the input state into the
            // checkpoint.
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

        for (object_id, object) in latest_live_output_objects.iter() {
            // Ignore mutations to the dynamic field.
            if checkpoint_input_objects.contains_key(object_id) {
                continue;
            }

            // We only care to emit a record for `Metadata` dynamic fields if they have the "path"
            // key-value attribute.
            let Some((file_path, parent_id)) =
                extract_file_path_and_parent_id(&self.metadata_type, object)
            else {
                continue;
            };

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
        let stored_values = values
            .into_iter()
            .map(|v| v.try_into())
            .collect::<Result<Vec<StoredWalrusBlob>>>()?;

        Ok(diesel::insert_into(walrus_blob::table)
            .values(&stored_values)
            .on_conflict((walrus_blob::address_owner, walrus_blob::file_path))
            .do_update()
            .set((
                walrus_blob::dynamic_field_id.eq(excluded(walrus_blob::dynamic_field_id)),
                walrus_blob::cp_sequence_number.eq(excluded(walrus_blob::cp_sequence_number)),
                walrus_blob::owner_id.eq(excluded(walrus_blob::owner_id)),
                walrus_blob::address_owner.eq(excluded(walrus_blob::address_owner)),
                walrus_blob::file_path.eq(excluded(walrus_blob::file_path)),
                walrus_blob::blob_id.eq(excluded(walrus_blob::blob_id)),
                walrus_blob::deleted.eq(excluded(walrus_blob::deleted)),
            ))
            .filter(walrus_blob::cp_sequence_number.lt(excluded(walrus_blob::cp_sequence_number)))
            .execute(conn)
            .await?)
    }

    async fn prune<'a>(
        &self,
        from: u64,
        to_exclusive: u64,
        conn: &mut postgres::Connection<'a>,
    ) -> Result<usize> {
        Ok(diesel::delete(walrus_blob::table)
            .filter(walrus_blob::deleted.eq(true))
            .filter(walrus_blob::cp_sequence_number.ge(from as i64))
            .filter(walrus_blob::cp_sequence_number.lt(to_exclusive as i64))
            .execute(conn)
            .await?)
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

        let Owner::AddressOwner(id) = self.parent_object.owner() else {
            bail!("Parent object's owner is not an address owner");
        };
        let address_owner = id.to_vec();

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

impl WalrusBlobPipeline {
    pub fn new(type_string: &str) -> Result<Self> {
        let metadata_type = parse_sui_struct_tag(type_string)?;
        Ok(WalrusBlobPipeline { metadata_type })
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

/// Returns all versions of objects that were output by transactions in the checkpoint, and are
/// still live at the end of the checkpoint.
pub(crate) fn checkpoint_output_objects(
    checkpoint: &CheckpointData,
) -> anyhow::Result<BTreeMap<ObjectID, &Object>> {
    let mut output_objects = BTreeMap::new();
    for tx in &checkpoint.transactions {
        let output_objects_map: BTreeMap<_, _> = tx
            .output_objects
            .iter()
            .map(|obj| ((obj.id(), obj.version()), obj))
            .collect();

        for change in tx.effects.object_changes() {
            let id = change.id;

            // Clear the previous entry, in case it was created within this checkpoint.
            output_objects.remove(&id);

            let Some(version) = change.output_version else {
                continue;
            };

            let output_object = output_objects_map
                .get(&(id, version))
                .copied()
                .with_context(|| format!("{id} at {version} in effects, not in output_objects"))?;

            output_objects.insert(id, output_object);
        }
    }

    Ok(output_objects)
}
