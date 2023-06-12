use core::fmt;

use anyhow::{Context, Result};
use ethers::types::{
    Address, Block as EthersBlock, Bytes, Transaction as EthersTransaction, H256, U256, U64,
};
use serde::{Deserialize, Serialize};

use crate::{clients::beacon::types::Blob as BeaconBlob, utils::web3::calculate_versioned_hash};

#[derive(Serialize, Deserialize, Debug)]
pub struct Block {
    pub number: U64,
    pub hash: H256,
    pub timestamp: U256,
    pub slot: u32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub hash: H256,
    pub from: Address,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub to: Option<Address>,
    pub block_number: U64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Blob {
    pub versioned_hash: H256,
    pub commitment: String,
    pub data: Bytes,
    pub tx_hash: H256,
    pub index: u32,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FailedSlotsChunk {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<u32>,
    pub initial_slot: u32,
    pub final_slot: u32,
}

#[derive(Serialize, Debug)]
pub struct SlotRequest {
    pub slot: u32,
}

#[derive(Deserialize, Debug)]
pub struct SlotResponse {
    pub slot: u32,
}

#[derive(Serialize, Debug)]
pub struct IndexRequest {
    pub block: Block,
    pub transactions: Vec<Transaction>,
    pub blobs: Vec<Blob>,
}

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Blob {{ versioned_hash: {}, commitment: {}, tx_hash: {}, index: {}, data: [omitted] }}",
            self.versioned_hash, self.commitment, self.tx_hash, self.index
        )
    }
}

impl From<(u32, u32)> for FailedSlotsChunk {
    fn from((initial_slot, final_slot): (u32, u32)) -> Self {
        Self {
            id: None,
            initial_slot,
            final_slot,
        }
    }
}

impl<'a> TryFrom<(&'a EthersBlock<EthersTransaction>, u32)> for Block {
    type Error = anyhow::Error;

    fn try_from(
        (ethers_block, slot): (&'a EthersBlock<EthersTransaction>, u32),
    ) -> Result<Self, Self::Error> {
        let number = ethers_block
            .number
            .with_context(|| "Missing block number field in execution block".to_string())?;

        Ok(Self {
            number,
            hash: ethers_block
                .hash
                .with_context(|| format!("Missing block hash field in execution block {number}"))?,
            timestamp: ethers_block.timestamp,
            slot,
        })
    }
}

impl<'a> TryFrom<(&'a EthersTransaction, &'a EthersBlock<EthersTransaction>)> for Transaction {
    type Error = anyhow::Error;

    fn try_from(
        (ethers_tx, ethers_block): (&'a EthersTransaction, &'a EthersBlock<EthersTransaction>),
    ) -> Result<Self, Self::Error> {
        let hash = ethers_tx.hash;

        Ok(Self {
            block_number: ethers_block
                .number
                .with_context(|| "Missing block number field in execution block".to_string())?,
            hash,
            from: ethers_tx.from,
            to: ethers_tx.to,
        })
    }
}

impl<'a> TryFrom<(&'a BeaconBlob, u32, H256)> for Blob {
    type Error = anyhow::Error;

    fn try_from(
        (blob_data, index, tx_hash): (&'a BeaconBlob, u32, H256),
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            tx_hash,
            index,
            commitment: blob_data.kzg_commitment.clone(),
            data: blob_data.blob.clone(),
            versioned_hash: calculate_versioned_hash(&blob_data.kzg_commitment)?,
        })
    }
}

impl<'a> From<(&'a BeaconBlob, &'a H256, usize, &'a H256)> for Blob {
    fn from(
        (blob_data, versioned_hash, index, tx_hash): (&'a BeaconBlob, &'a H256, usize, &'a H256),
    ) -> Self {
        Self {
            tx_hash: *tx_hash,
            index: index as u32,
            commitment: blob_data.kzg_commitment.clone(),
            data: blob_data.blob.clone(),
            versioned_hash: *versioned_hash,
        }
    }
}