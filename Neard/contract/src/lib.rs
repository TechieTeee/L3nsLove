// SPDX-License-Identifier: MIT

use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::collections::{LookupMap, UnorderedMap};
use near_sdk::json_types::U128;
use near_sdk::{env, near_bindgen, AccountId, Balance};

extern crate flate2;

use flate2::{write::ZlibEncoder, Compression};

#[derive(BorshDeserialize, BorshSerialize)]
struct DataRecord {
    id: u64,
    compressed_data: Vec<u8>,
}

#[near_bindgen]
pub struct DataEngine {
    records: UnorderedMap<u64, DataRecord>,
    balances: LookupMap<AccountId, Balance>,
}

impl Default for DataEngine {
    fn default() -> Self {
        Self {
            records: UnorderedMap::new(b"data_records"),
            balances: LookupMap::new(b"account_balances"),
        }
    }
}

fn deflate_data(data: String) -> Result<Vec<u8>, String> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(data.as_bytes()).map_err(|e| e.to_string())?;
    encoder.finish().map_err(|e| e.to_string())
}

fn inflate_data(compressed_data: Vec<u8>) -> Result<String, String> {
    let mut decoder = flate2::read::ZlibDecoder::new(&compressed_data[..]);
    let mut decompressed_data = String::new();
    decoder.read_to_string(&mut decompressed_data).map_err(|e| e.to_string())?;
    Ok(decompressed_data)
}

#[near_bindgen]
impl DataEngine {
    pub fn store_record(&mut self, data: String) -> Result<u64, String> {
        let id = self.records.len() + 1;
        let compressed_data = deflate_data(data)?;
        let record = DataRecord { id, compressed_data };
        self.records.insert(&id, &record);
        Ok(id)
    }

    pub fn get_record(&self, id: u64) -> Option<Result<String, String>> {
        self.records.get(&id).map(|record| inflate_data(record.compressed_data))
    }

    pub fn charge_for_data(&mut self, account_id: AccountId, amount: U128) {
        if let Some(mut balance) = self.balances.get(&account_id) {
            balance -= amount.0;
            self.balances.insert(&account_id, &balance);
        }
    }
}
