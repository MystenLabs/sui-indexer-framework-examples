// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{self, bail};
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

use crate::schema::walrus_blob_historical;
use crate::storage::StoredWalrusBlobHistorical;
use crate::types::extract_file_path_and_parent_id;
use crate::types::Blob;

// ============================================================================
// PROCESSING TYPES
// ============================================================================
// These types represent intermediate data structures used during processing.
// They bridge between the raw on-chain data and the database storage format.

/// Struct representing the data of interest transformed from processing the checkpoint to be passed
/// to the committer implementation.
pub struct ProcessedWalrusHistoricalMetadata {
    pub cp_sequence_number: i64,
    pub update: ProcessedWalrusHistoricalUpdate,
}

/// Enum to encapsulate the different types of object changes.
pub enum ProcessedWalrusHistoricalUpdate {
    /// The object was created, mutated, or unwrapped.
    Upsert {
        /// The Blob parent object that owns the Metadata dynamic field.
        parent_object: Object,
        /// The ID of the Metadata dynamic field.
        dynamic_field_id: ObjectID,
        /// The version of the Metadata dynamic field.
        df_version: SequenceNumber,
        /// The user-provided file path to be associated with the blob.
        file_path: String,
    },
    /// The object was deleted or wrapped at the transaction's lamport version.
    Delete((ObjectID, SequenceNumber)),
}

pub struct WalrusBlobHistoricalPipeline {
    metadata_type: StructTag,
}

impl Processor for WalrusBlobHistoricalPipeline {
    const NAME: &'static str = "walrus_blob_historical";

    type Value = ProcessedWalrusHistoricalMetadata;

    fn process(&self, checkpoint: &Arc<CheckpointData>) -> Result<Vec<Self::Value>> {
        let mut values: Vec<Self::Value> = Vec::new();

        for tx in &checkpoint.transactions {
            // All input objects as they were prior to execution.
            let input_objects_map: BTreeMap<_, _> =
                tx.input_objects.iter().map(|obj| (obj.id(), obj)).collect();

            // All output objects created, mutated, or unwrapped by this transaction.
            let output_objects_map: BTreeMap<_, _> = tx
                .output_objects
                .iter()
                .map(|obj| ((obj.id(), obj.version()), obj))
                .collect();

            for c in tx.effects.object_changes() {
                // Object was created, mutated, or unwrapped.
                if let Some(version) = c.output_version {
                    let Some(object) = output_objects_map.get(&(c.id, version)) else {
                        tracing::error!(
                            "Object {} at version {} not found in tx's output objects",
                            c.id,
                            version
                        );
                        continue;
                    };

                    // Only index Metadata dynamic fields that have the "path" key-value attribute.
                    let Some((file_path, parent_id)) =
                        extract_file_path_and_parent_id(&self.metadata_type, object)
                    else {
                        continue;
                    };

                    // The parent object must exist and is needed for the remaining columns on the
                    // table. Look for the parent object change at the same version as the child.
                    let Some(parent_object) = output_objects_map.get(&(parent_id, version)) else {
                        tracing::error!(
                            "Parent object {} at version {} for child {} not found among tx's output objects",
                            parent_id,
                            version,
                            c.id
                        );
                        continue;
                    };

                    values.push(ProcessedWalrusHistoricalMetadata {
                        cp_sequence_number: checkpoint.checkpoint_summary.sequence_number as i64,
                        update: ProcessedWalrusHistoricalUpdate::Upsert {
                            parent_object: (*parent_object).clone(),
                            dynamic_field_id: c.id,
                            df_version: version,
                            file_path,
                        },
                    });
                }
                // Object was wrapped or deleted. At its output state, it will not have a version or
                // any contents, so we need to consult the object's input state to the transaction.
                else {
                    let Some(input_object) = input_objects_map.get(&c.id) else {
                        // We don't raise an error log here, because objects may have been unwrapped
                        // then deleted. If this applies to a Metadata dynamic field, then when it
                        // was wrapped, we would've written a sentinel row to the table.
                        continue;
                    };

                    // Only Metadata dynamic fields that have the "path" key-value attribute are
                    // indexed, and only those entries need a tombstone record.
                    let Some((_, _)) =
                        extract_file_path_and_parent_id(&self.metadata_type, input_object)
                    else {
                        continue;
                    };

                    let lamport = tx.effects.lamport_version();

                    // Unlike the upsert scenario, we do not need to consult the parent object, as
                    // those fields are to remain empty for tombstone records.
                    values.push(ProcessedWalrusHistoricalMetadata {
                        cp_sequence_number: checkpoint.checkpoint_summary.sequence_number as i64,
                        update: ProcessedWalrusHistoricalUpdate::Delete((c.id, lamport)),
                    });
                }
            }
        }

        Ok(values)
    }
}

#[async_trait::async_trait]
impl Handler for WalrusBlobHistoricalPipeline {
    type Store = postgres::Db;

    async fn commit<'a>(
        values: &[Self::Value],
        conn: &mut postgres::Connection<'a>,
    ) -> Result<usize> {
        let stored_values = values
            .into_iter()
            .map(|v| v.try_into())
            .collect::<Result<Vec<StoredWalrusBlobHistorical>>>()?;

        Ok(diesel::insert_into(walrus_blob_historical::table)
            .values(&stored_values)
            .on_conflict_do_nothing()
            .execute(conn)
            .await?)
    }
}

impl FieldCount for ProcessedWalrusHistoricalMetadata {
    const FIELD_COUNT: usize = StoredWalrusBlobHistorical::FIELD_COUNT;
}

impl TryInto<StoredWalrusBlobHistorical> for &ProcessedWalrusHistoricalMetadata {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<StoredWalrusBlobHistorical> {
        match &self.update {
            ProcessedWalrusHistoricalUpdate::Upsert {
                parent_object,
                dynamic_field_id,
                df_version,
                file_path,
            } => {
                let blob_object: Blob = bcs::from_bytes(
                    parent_object
                        .data
                        .try_as_move()
                        .ok_or_else(|| anyhow::anyhow!("Parent object is not a Move object"))?
                        .contents(),
                )?;

                let Owner::AddressOwner(id) = parent_object.owner() else {
                    bail!("Parent object's owner is not an address owner");
                };
                let address_owner = id.to_vec();

                Ok(StoredWalrusBlobHistorical {
                    dynamic_field_id: dynamic_field_id.to_vec(),
                    df_version: df_version.value() as i64,
                    cp_sequence_number: self.cp_sequence_number as i64,
                    file_path: Some(file_path.clone()),
                    owner_id: Some(parent_object.id().to_vec()),
                    address_owner: Some(address_owner),
                    blob_id: Some(blob_object.blob_id.0.to_vec()),
                })
            }
            ProcessedWalrusHistoricalUpdate::Delete((dynamic_field_id, lamport)) => {
                Ok(StoredWalrusBlobHistorical {
                    dynamic_field_id: dynamic_field_id.to_vec(),
                    cp_sequence_number: self.cp_sequence_number as i64,
                    df_version: lamport.value() as i64,
                    address_owner: None,
                    file_path: None,
                    owner_id: None,
                    blob_id: None,
                })
            }
        }
    }
}

impl WalrusBlobHistoricalPipeline {
    pub fn new(type_string: &str) -> Result<Self> {
        let metadata_type = parse_sui_struct_tag(type_string)?;
        Ok(WalrusBlobHistoricalPipeline { metadata_type })
    }
}
