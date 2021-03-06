use rocksdb::{DB, WriteBatch};
use bitcoin::hash_types::BlockHash;
use byteorder::{ByteOrder, BigEndian};

use crate::storage::scheme::chain_famiy;

pub fn update_chain(db: &DB, hashes: Vec<BlockHash>) {

}

/// Get height of known main chain
pub fn chain_height(db: &DB) -> u32 {
    db.get_cf(chain_famiy(db), b"height").unwrap().map_or(0, |bs| BigEndian::read_u32(&bs))
}

/// Put update of current height of main chain in batch
pub fn set_chain_height(batch: &mut WriteBatch, h: u32) {
    let mut buf = [0; 4];
    BigEndian::write_u32(&mut buf, h);
    batch.put(b"height", &buf);
}
