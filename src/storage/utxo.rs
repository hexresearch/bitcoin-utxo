use bitcoin_hashes::Hash;
use bitcoin::blockdata;
use bitcoin::blockdata::block::BlockHeader;
use bitcoin::hash_types::BlockHash;
use bitcoin::network::constants;
use byteorder::{ByteOrder, BigEndian};
use rocksdb::{DB, WriteBatch, ColumnFamily};

use crate::storage::scheme::utxo_famiy;

pub fn init_utxo_storage(db: &DB) {
    // let cf = utxo_famiy(db);
    // let mut batch = WriteBatch::default();
    // db.write(batch).unwrap();
}
