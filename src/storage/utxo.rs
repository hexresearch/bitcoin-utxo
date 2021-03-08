use bitcoin_hashes::Hash;
use bitcoin::blockdata;
use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::encode::{Decodable, Encodable, serialize, deserialize};
use bitcoin::hash_types::BlockHash;
use bitcoin::network::constants;
use byteorder::{ByteOrder, BigEndian};
use rocksdb::{DB, WriteBatch, ColumnFamily};

use crate::utxo::{UtxoKey, encode_utxo_key, decode_utxo_key};
use crate::storage::scheme::utxo_famiy;

pub fn init_utxo_storage(db: &DB) {
    // let cf = utxo_famiy(db);
    // let mut batch = WriteBatch::default();
    // db.write(batch).unwrap();
}

pub fn utxo_store_insert<T: Encodable>(db: &DB, batch: &mut WriteBatch, k: &UtxoKey, v: &T) {
    let cf = utxo_famiy(db);
    let kb = encode_utxo_key(k);
    let val = serialize(v);
    batch.put_cf(cf, kb, &val);
}

pub fn utxo_store_delete(db: &DB, batch: &mut WriteBatch, k: &UtxoKey) {
    let cf = utxo_famiy(db);
    let kb = encode_utxo_key(k);
    batch.delete_cf(cf, kb);
}

pub fn utxo_store_read<T: Decodable>(db: &DB, k: &UtxoKey) -> Option<T> {
    let cf = utxo_famiy(db);
    let kb = encode_utxo_key(k);
    db.get_cf(cf, kb).unwrap().map(|bs| deserialize(&bs[..]).unwrap())
}
