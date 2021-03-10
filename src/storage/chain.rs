use bitcoin::blockdata;
use bitcoin::blockdata::block::BlockHeader;
use bitcoin::hash_types::BlockHash;
use bitcoin::network::constants;
use bitcoin_hashes::Hash;
use byteorder::{BigEndian, ByteOrder};
use rocksdb::{ColumnFamily, WriteBatch, DB};
use tokio::time::{sleep, Duration};

use crate::storage::scheme::chain_famiy;

pub fn init_chain_storage(db: &DB) {
    let cf = chain_famiy(db);
    let mut batch = WriteBatch::default();
    let genesis_hash =
        blockdata::constants::genesis_block(constants::Network::Bitcoin).block_hash();
    add_block_to_chain(&mut batch, cf, &genesis_hash, 0);
    db.write(batch).unwrap();
}

pub fn update_chain(db: &DB, headers: &Vec<BlockHeader>) {
    if headers.len() == 0 {
        return;
    }
    let cf = chain_famiy(db);

    let mut height = block_height(db, cf, &headers[0].prev_blockhash).map_or(1, |h| h + 1);
    let mut batch = WriteBatch::default();
    for header in headers {
        add_block_to_chain(&mut batch, cf, &header.block_hash(), height);
        height += 1;
    }
    set_chain_height(&mut batch, cf, height - 1);

    db.write(batch).unwrap();
}

/// Get block locator to get blocks after given height
pub fn get_block_locator(db: &DB, height: u32) -> Vec<BlockHash> {
    let cf = chain_famiy(db);

    let is = get_locator_heights(height);
    let mut hs = vec![];
    for i in is {
        if let Some(h) = get_block(db, cf, i) {
            hs.push(h);
        }
    }
    hs
}

/// Get height of known main chain
pub fn get_chain_height(db: &DB) -> u32 {
    let cf = chain_famiy(db);
    chain_height(db, cf)
}

/// Write down chain height to fixed size (used for restore)
pub fn overwite_chain_height(db: &DB, h: u32) {
    let cf = chain_famiy(db);
    let mut batch = WriteBatch::default();
    set_chain_height(&mut batch, cf, h);
    db.write(batch).unwrap();
}

/// Makes futures that polls chain height and finishes when it is changed
pub async fn chain_height_changes(db: &DB, dur: Duration) {
    let starth = chain_height(db, chain_famiy(db));
    let mut curh = starth;
    while curh == starth {
        sleep(dur).await;
        curh = chain_height(db, chain_famiy(db));
    }
}

/// Ask block hash for given height at main chain
pub fn get_block_hash(db: &DB, height: u32) -> Option<BlockHash> {
    let cf = chain_famiy(db);
    get_block(db, cf, height)
}

/// Construct block locator height indecies to fetch blocks after given height
fn get_locator_heights(height: u32) -> Vec<u32> {
    let mut is = vec![];
    let mut step = 1;
    let mut i = height as i32;
    while i > 0 {
        if is.len() >= 10 {
            step *= 2;
        }
        is.push(i as u32);
        i -= step;
    }
    is.push(0);
    is
}

/// Construct key with prefix for height->hash mapping
fn block_height_key(h: u32) -> Vec<u8> {
    let mut buf = [0; 4];
    BigEndian::write_u32(&mut buf, h);
    [vec![1u8], buf.to_vec()].concat()
}

/// Construct key with prefix for hash->height mapping
fn block_hash_key(hash: &BlockHash) -> Vec<u8> {
    [vec![0u8], hash.to_vec()].concat()
}

/// Construct value for height
fn height_value(h: u32) -> [u8; 4] {
    let mut buf = [0; 4];
    BigEndian::write_u32(&mut buf, h);
    buf
}

/// Get height of known main chain
fn chain_height(db: &DB, cf: &ColumnFamily) -> u32 {
    db.get_cf(cf, b"height")
        .unwrap()
        .map_or(0, |bs| BigEndian::read_u32(&bs))
}

/// Put update of current height of main chain in batch
fn set_chain_height(batch: &mut WriteBatch, cf: &ColumnFamily, h: u32) {
    let val = height_value(h);
    batch.put_cf(cf, b"height", &val);
}

/// Find heigh of block in main chain
fn block_height(db: &DB, cf: &ColumnFamily, hash: &BlockHash) -> Option<u32> {
    let k = block_hash_key(hash);
    db.get_cf(cf, k).unwrap().map(|bs| BigEndian::read_u32(&bs))
}

/// Find block by height in main chain
fn get_block(db: &DB, cf: &ColumnFamily, h: u32) -> Option<BlockHash> {
    let k = block_height_key(h);
    db.get_cf(cf, k)
        .unwrap()
        .map(|bs| BlockHash::from_hash(Hash::from_slice(&bs).unwrap()))
}

/// Write all mapping between height and block hash to database
fn add_block_to_chain(batch: &mut WriteBatch, cf: &ColumnFamily, hash: &BlockHash, h: u32) {
    add_hash_to_height(batch, cf, hash, h);
    add_height_to_hash(batch, cf, hash, h);
}

/// Write mapping from block hash to height
fn add_hash_to_height(batch: &mut WriteBatch, cf: &ColumnFamily, hash: &BlockHash, h: u32) {
    let k = block_hash_key(hash);
    let val = height_value(h);
    batch.put_cf(cf, k, &val);
}

/// Write mapping from height to block hash
fn add_height_to_hash(batch: &mut WriteBatch, cf: &ColumnFamily, hash: &BlockHash, h: u32) {
    let k = block_height_key(h);
    batch.put_cf(cf, k, hash.to_vec());
}
