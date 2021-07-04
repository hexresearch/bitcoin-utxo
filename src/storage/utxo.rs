use bitcoin::consensus::encode::{deserialize, serialize, Decodable, Encodable};
use byteorder::{BigEndian, ByteOrder};
use rocksdb::{ColumnFamily, DBIterator, IteratorMode, WriteBatch, DB};
use std::marker::PhantomData;

use crate::storage::scheme::utxo_famiy;
use crate::utxo::*;

pub fn init_utxo_storage(_: &DB) {}

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
    db.get_cf(cf, kb)
        .unwrap()
        .map(|bs| deserialize(&bs[..]).unwrap())
}

/// Construct value for height
fn height_value(h: u32) -> [u8; 4] {
    let mut buf = [0; 4];
    BigEndian::write_u32(&mut buf, h);
    buf
}

/// Get height of processed utxo
pub fn utxo_height(db: &DB) -> u32 {
    let cf = utxo_famiy(db);
    db.get_cf(cf, b"height")
        .unwrap()
        .map_or(0, |bs| BigEndian::read_u32(&bs))
}

/// Put update of current height of main chain in batch
pub fn set_utxo_height(batch: &mut WriteBatch, cf: &ColumnFamily, h: u32) {
    let val = height_value(h);
    batch.put_cf(cf, b"height", &val);
}

pub struct UtxoIterator<'a, T>(DBIterator<'a>, PhantomData<T>);

impl<'a, T: Decodable> Iterator for UtxoIterator<'a, T> {
    type Item = (UtxoKey, T);

    fn next(&mut self) -> Option<(UtxoKey, T)> {
        if let Some((kbs, vbs)) = self.0.next() {
            if b"height"[..] == *kbs {
                self.next()
            } else {
                let k = decode_utxo_key(kbs.to_vec()).expect("Utxo key is not parsable");
                let v = deserialize(&vbs[..]).expect("Utxo value is not parsable");
                Some((k, v))
            }
        } else {
            None
        }
    }
}

/// Return iterator over all utxos
pub fn utxo_iterator<T: Decodable>(db: &DB) -> UtxoIterator<T> {
    UtxoIterator(
        db.iterator_cf(utxo_famiy(db), IteratorMode::Start),
        PhantomData,
    )
}
