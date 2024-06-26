use anyhow::{Context as AnyhowContext, Result};

use ethers::prelude::*;
use tracing::{debug, info};

use crate::{
    clients::{
        beacon::types::{BlobsResponse, BlockHeader, BlockId},
        blobscan::types::{Blob, Block, Transaction},
    },
    context::Context,
};

use self::error::{SlotProcessingError, SlotsProcessorError};
use self::helpers::{create_tx_hash_versioned_hashes_mapping, create_versioned_hash_blob_mapping};

pub mod error;
mod helpers;
const SLOT_PER_EPOCH: u32 = 6;
pub struct SlotsProcessor {
    context: Context,
}

#[derive(Debug, Clone)]
pub struct BlockData {
    pub root: H256,
    pub slot: u32,
}

impl From<BlockHeader> for BlockData {
    fn from(block_header: BlockHeader) -> Self {
        Self {
            root: block_header.root,
            slot: block_header.header.message.slot,
        }
    }
}

impl SlotsProcessor {
    pub fn new(context: Context) -> SlotsProcessor {
        Self { context }
    }

    pub async fn process_slots(
        &mut self,
        initial_slot: u32,
        final_slot: u32,
    ) -> Result<(), SlotsProcessorError> {
        let is_reverse = initial_slot > final_slot;
        let slots = if is_reverse {
            (final_slot..initial_slot).rev().collect::<Vec<_>>()
        } else {
            (initial_slot..final_slot).collect::<Vec<_>>()
        };

        for current_slot in slots {
            if let Err(error) = self.process_slot(current_slot).await {
                return Err(SlotsProcessorError::FailedSlotsProcessing {
                    initial_slot,
                    final_slot,
                    failed_slot: current_slot,
                    error,
                });
            }
        }

        Ok(())
    }

    pub async fn process_slot(&mut self, slot: u32) -> Result<(), SlotProcessingError> {
        let beacon_client = self.context.beacon_client();
        let blobscan_client = self.context.blobscan_client();
        let provider = self.context.provider();
        if slot == 0 {
            debug!(
                target = "slots_processor",
                slot, "Slot = 0! Skipping getting initial beacon block as it's empty."
            );
            return Ok(());
        }
        let beacon_block = match beacon_client.get_block(&BlockId::Slot(slot)).await? {
            Some(block) => block,
            None => {
                debug!(slot = slot, "Skipping as there is no beacon block");

                return Ok(());
            }
        };

        let execution_payload = match beacon_block.message.body.execution_payload {
            Some(payload) => payload,
            None => {
                debug!(
                    slot,
                    "Skipping as beacon block doesn't contain execution payload"
                );

                return Ok(());
            }
        };

        let has_kzg_blob_commitments = match beacon_block.message.body.blob_kzg_commitments {
            Some(commitments) => !commitments.is_empty(),
            None => false,
        };

        // if !has_kzg_blob_commitments {
        //     debug!(
        //         target = "slots_processor",
        //         slot, "Skipping as beacon block doesn't contain blob kzg commitments"
        //     );

        //     return Ok(());
        // }

        let execution_block_hash = execution_payload.block_hash;

        // Fetch execution block and perform some checks

        let execution_block = provider
            .get_block_with_txs(execution_block_hash)
            .await?
            .with_context(|| format!("Execution block {execution_block_hash} not found"))?;
        //create versioned_hashes for blob transactions
        let tx_hash_to_versioned_hashes =
            create_tx_hash_versioned_hashes_mapping(&execution_block)?;

        // if tx_hash_to_versioned_hashes.is_empty() {
        //     return Err(anyhow!("Blocks mismatch: Beacon block contains blob KZG commitments, but the corresponding execution block does not contain any blob transactions").into());
        // }

        // Fetch blobs and perform some checks

        // let blobs = match beacon_client
        //     .get_blobs(&BlockId::Slot(slot))
        //     .await
        //     .map_err(SlotProcessingError::ClientError)?
        // {
        //     Some(blobs) => {
        //         if blobs.is_empty() {
        //             debug!(
        //                 target = "slots_processor",
        //                 slot, "Skipping as blobs sidecar is empty"
        //             );

        //             return Ok(());
        //         } else {
        //             blobs
        //         }
        //     }
        //     None => {
        //         debug!(
        //             target = "slots_processor",
        //             slot, "Skipping as there is no blobs sidecar"
        //         );

        //         return Ok(());
        //     }
        // };
        


        // Create entities to be indexed

        let transactions_entities = execution_block
            .transactions
            .iter()
            // .filter(|tx| tx_hash_to_versioned_hashes.contains_key(&tx.hash))
            .map(|tx| Transaction::try_from((tx, &execution_block)))
            .collect::<Result<Vec<Transaction>>>()?;

        if transactions_entities.is_empty() {
            debug!(
                target = "slots_processor",
                slot, "Skipping as there are no transactions to index, it is a empty block!"
            );

            return Ok(());
        }

        let validators = match beacon_client.get_validators(&BlockId::Slot(slot/SLOT_PER_EPOCH)).await? {
            Some(validators) => validators,
            None => {
                debug!(
                    target = "slots_processor",
                    slot, "Skipping as there are no validators"
                );

                return Ok(());
            }
        };
        //选出其中slot为当前slot的validator_pubkey
        let validator_pubkey = validators.iter().find(|validator| validator.slot == slot).unwrap().pubkey.clone();
        // println!("validator_pubkeys: {:?}", validator_pubkeys);
        
        let block_entity = Block::try_from((&execution_block, slot, validator_pubkey))?;

        let mut blob_entities: Vec<Blob> = vec![];
        //if there are blobs, create blob entities
        if has_kzg_blob_commitments {
            let columns = match beacon_client
                .get_columns(&BlockId::Slot(slot))
                .await
                .map_err(SlotProcessingError::ClientError)?
            {
                Some(columns) => {
                    if columns.data.is_empty() {
                        debug!(
                            target = "slots_processor",
                            slot, "Skipping as columns sidecar is empty"
                        );

                        return Ok(());
                    } else {
                        columns
                    }
                }
                None => {
                    debug!(
                        target = "slots_processor",
                        slot, "Skipping as there is no columns sidecar"
                    );

                    return Ok(());
                }
            };

            let blobs = BlobsResponse::from(columns).data;
            let versioned_hash_to_blob = create_versioned_hash_blob_mapping(&blobs)?;
            for (tx_hash, versioned_hashes) in tx_hash_to_versioned_hashes.iter() {
                for (i, versioned_hash) in versioned_hashes.iter().enumerate() {
                    let blob = *versioned_hash_to_blob.get(versioned_hash).with_context(|| format!("Sidecar not found for blob {i} with versioned hash {versioned_hash} from tx {tx_hash}"))?;
    
                    blob_entities.push(Blob::from((blob, versioned_hash, i, tx_hash)));
                }
            }
        }
        

        /*
        let tx_hashes = transactions_entities
            .iter()
            .map(|tx| tx.hash.to_string())
            .collect::<Vec<String>>();
        let blob_versioned_hashes = blob_entities
            .iter()
            .map(|blob| blob.versioned_hash.to_string())
            .collect::<Vec<String>>();
         */

        let block_number = block_entity.number.as_u32();

        blobscan_client
            .index(block_entity, transactions_entities, blob_entities)
            .await
            .map_err(SlotProcessingError::ClientError)?;

        info!(slot, block_number, "Block indexed successfully");

        Ok(())
    }
}
