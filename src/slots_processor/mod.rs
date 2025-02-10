use anyhow::{Context as AnyhowContext, Result};

use ethers::prelude::*;
use tracing::{debug, info, warn};
use std::time::Duration;
use std::time::SystemTime;
use std::thread::sleep;
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
const SLOT_PER_EPOCH: u32 = 32;
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
        debug!("process_slot, slot is {}, begin time is {:?}", slot, SystemTime::now()); 
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

        let mut retries = 0;
        let max_retries = 5000;
        let max_delay = Duration::from_secs(600);
        let mut delay = Duration::from_secs(5);

        debug!("process_slot, slot is {}, before get beacon block, time is {:?}", slot, SystemTime::now()); 
        let beacon_block = loop {
            match beacon_client.get_block(&BlockId::Slot(slot)).await {
                Ok(Some(block)) => break block,
                Ok(None) => {
                    debug!(slot = slot, "Skipping as there is no beacon block");
                    return Ok(());
                },
                Err(_e) if retries < max_retries => {
                    retries += 1;
                    warn!(retries, "Error {:?} occurred when get beacon block, retrying... ({}/{}) ", _e, retries, max_retries);
                    sleep(delay);
                    delay *= 2;
                    if delay > max_delay {
                        delay = max_delay;
                    }
                },
                Err(e) => {
                    return Err(e.into());
                }
            }
        };
        debug!("process_slot, slot is {}, after get beacon block, time is {:?}", slot, SystemTime::now()); 

        let proposer_index = beacon_block.message.proposer_index;

        let execution_payload = match beacon_block.message.body.execution_payload {
            Some(payload) => payload,
            None => {
                warn!(
                    slot,
                    "Skipping as beacon block doesn't contain execution payload"
                );

                return Ok(());
            }
        };

        let blob_kzg_commitments = match beacon_block.message.body.blob_kzg_commitments{
            Some(commitments) => commitments.clone(),
            None => Vec::new(),
        };
        // println!("{:?}===============>", blob_kzg_commitments.len());
        // if !has_kzg_blob_commitments {
        //     debug!(
        //         target = "slots_processor",
        //         slot, "Skipping as beacon block doesn't contain blob kzg commitments"
        //     );

        //     return Ok(());
        // }

        let execution_block_hash = execution_payload.block_hash;

        // Fetch execution block and perform some checks

        // let execution_block = provider
        //     .get_block_with_txs(execution_block_hash)
        //     .await?
        //     .with_context(|| format!("Execution block {execution_block_hash} not found"))?;
        let mut retries = 0;
        let max_retries = 5000;
        let max_delay = Duration::from_secs(600);
        let mut delay = Duration::from_secs(5);

        debug!("process_slot, slot is {}, before get execution block, time is {:?}", slot, SystemTime::now()); 
        let execution_block = loop {
            match provider
                .get_block_with_txs(execution_block_hash)
                .await {
                Ok(execution_block) => break execution_block,
                Err(_e) if retries < max_retries => {
                    retries += 1;
                    println!("Error occurred, retrying... ({}/{})", retries, max_retries);
                    sleep(delay);
                    delay *= 2;
                    if delay > max_delay {
                        delay = max_delay;
                    }
                },
                Err(e) => {
                    return Err(e.into());
                }
            }
        };
        debug!("process_slot, slot is {}, after get execution block, time is {:?}", slot, SystemTime::now()); 
        //transfer execution_block from option to block
        let execution_block = execution_block.unwrap();

        //create versioned_hashes for blob transactions
        let tx_hash_to_versioned_hashes =
            create_tx_hash_versioned_hashes_mapping(&execution_block)?;

        let transactions_entities = execution_block
            .transactions
            .iter()
            // .filter(|tx| tx_hash_to_versioned_hashes.contains_key(&tx.hash))
            .map(|tx| Transaction::try_from((tx, &execution_block)))
            .collect::<Result<Vec<Transaction>>>()?;

        let transactions_entities = transactions_entities
        .into_iter()
        .map(|mut tx| {
            // check whether the to is None
            if tx.to.is_none() {
                //  set to 0xFFFF...
                tx.to = Some(Address::from([0xFF; 20]));
            }
            tx
        })
        .collect::<Vec<Transaction>>();

        if transactions_entities.is_empty() {
            debug!(
                target = "slots_processor",
                slot, "Skipping as there are no transactions to index, it is a empty block!"
            );

            return Ok(());
        }

        let mut retries = 0;
        let max_retries = 5000;
        let max_delay = Duration::from_secs(600);
        let mut delay = Duration::from_secs(5);

        debug!("process_slot, slot is {}, before get_head_validator, time is {:?}", slot, SystemTime::now()); 
        let validator_container = loop {
            match beacon_client.get_head_validator(&proposer_index).await? {
                Some(container) => break Some(container),
                None => if retries < max_retries {
                    retries += 1;
                    println!("Error occurred, retrying... ({}/{})", retries, max_retries);
                    sleep(delay);
                    delay *= 2;
                    if delay > max_delay {
                        delay = max_delay;
                    }
                } else {
                    println!("Failed to get head proposer_index {} validator after {} retries. Skipping slot processing.", 
                        proposer_index, retries
                    );
                    return Err(SlotProcessingError::CustomError("Failed to get validator after retries".to_string()));
                }
            }
        };
        debug!("process_slot, slot is {}, after get_head_validator, time is {:?}", slot, SystemTime::now()); 

        let validator_pubkey;
        match validator_container {
          Some(containers) => {
            validator_pubkey = containers.validator.pubkey.clone();
          },
          None => return Err(SlotProcessingError::CustomError("Failed to get validator pubkey".to_string())) 
        }
        
        let block_entity = Block::try_from((&execution_block, slot, validator_pubkey))?;

        let mut blob_entities: Vec<Blob> = vec![];
        //if there are blobs, create blob entities
        if !blob_kzg_commitments.is_empty() {
            let blobs = BlobsResponse::from(blob_kzg_commitments).data;
            let versioned_hash_to_blob = create_versioned_hash_blob_mapping(&blobs)?;
            for (tx_hash, versioned_hashes) in tx_hash_to_versioned_hashes.iter() {
                for (i, versioned_hash) in versioned_hashes.iter().enumerate() {
                    let blob = *versioned_hash_to_blob.get(versioned_hash).with_context(|| format!("Sidecar not found for blob {i} with versioned hash {versioned_hash} from tx {tx_hash}"))?;
    
                    blob_entities.push(Blob::from((blob, versioned_hash, i, tx_hash)));
                }
            }
        }

        let block_number = block_entity.number.as_u32();
        let mut retries = 0;
        let max_retries = 5000;
        let max_delay = Duration::from_secs(600);
        let mut delay = Duration::from_secs(5);

        debug!("process_slot, slot is {}, before blobscan index, time is {:?}", slot, SystemTime::now()); 
        loop {
            match blobscan_client
                .index(block_entity.clone(), transactions_entities.clone(), blob_entities.clone())
                .await {
                Ok(_) => break,
                Err(_e) if retries < max_retries => {
                    retries += 1;
                    println!("Error occurred, retrying... ({}/{})", retries, max_retries);
                    sleep(delay);
                    delay *= 2;
                    if delay > max_delay {
                        delay = max_delay;
                    }
                },
                Err(e) => {
                    return Err(SlotProcessingError::ClientError(e));
                }
            }
        }
        debug!("process_slot, slot is {}, after blobscan index, time is {:?}", slot, SystemTime::now()); 

        info!(slot, block_number, "Block indexed successfully");
        debug!("process_slot, slot is {}, end time is {:?}", slot, SystemTime::now()); 

        Ok(())
    }
}
