use bitcoin::blockdata::transaction::{Transaction, OutPoint};
use bitcoin::blockdata::block::BlockHeader;
use bitcoin::consensus::encode::{Decodable, Encodable};
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use rocksdb::{DB, WriteBatch};
use std::collections::hash_map::RandomState;

use crate::utxo::{UtxoKey, UtxoState};
use crate::storage::utxo::*;

/// Maximum fork depth after which we can flush UTXO to disk
pub const UTXO_FORK_MAX_DEPTH: u32 = 100;

/// Cache of UTXO coins with T payload. We keep unrollbackable UTXO set in database
/// and the most recent UTXO set in memory.
pub type UtxoCache<T> = DashMap<UtxoKey, CoinChange<T>, RandomState>;

/// Tracks changes of UTXO set. Each N blocks we dump changes to disk, that allows to get
/// cheap support for fork resistance. If fork is dected, we drop cache and start from
/// storage backed state of UTXO.
pub enum CoinChange<T> {
    Pure(T),
    Add(T, u32),
    Remove(u32),
}

impl<T> CoinChange<T> {
    /// Get payload of utxo state change
    pub fn payload(&self) -> Option<&T> {
        match self {
            CoinChange::Pure(t) => Some(t),
            CoinChange::Add(t,_) => Some(t),
            CoinChange::Remove(_) => None,
        }
    }
}

/// Remove all inputs from UTXO and add all outputs.
pub fn update_utxo<T: UtxoState>(cache: &UtxoCache<T>, h: u32, header: &BlockHeader, tx: &Transaction) {
    for txin in &tx.input {
        remove_utxo(cache, h, &txin.previous_output);
    }
    let mut out = OutPoint {
        txid: tx.txid(),
        vout: 0,
    };
    for i in 0..tx.output.len() {
        out.vout = i as u32;
        let t = T::new_utxo(h, header, &tx, i as u32);
        add_utxo(cache, h, &out, t);
    }
}

fn remove_utxo<T>(cache: &UtxoCache<T>, h: u32, k: &UtxoKey) {
    cache.insert(*k, CoinChange::<T>::Remove(h) );
}

fn add_utxo<T>(cache: &UtxoCache<T>, h: u32, k: &UtxoKey, t: T) {
    cache.insert(*k, CoinChange::<T>::Add(t, h) );
}

/// Flush UTXO to database if UTXO changes are old enough to avoid forks.
pub fn finish_block<T: Encodable>(db: &DB, cache: &UtxoCache<T>, h: u32) {
    if h > 0 && h % UTXO_FORK_MAX_DEPTH == 0 {
        println!("Writing UTXO to disk...");
        flush_utxo(db, cache, h-UTXO_FORK_MAX_DEPTH);
        println!("Writing UTXO to disk is done");
    }
}

/// Flush all UTXO changes to database if change older or equal than given height.
pub fn flush_utxo<T: Encodable>(db: &DB, cache: &UtxoCache<T>, h: u32) {
    let mut ks = vec![];
    let mut batch = WriteBatch::default();
    for r in cache {
        let k = r.key();
        match r.value() {
            CoinChange::Add(t, add_h) if *add_h <= h => {
                ks.push(*k);
                utxo_store_insert(db, &mut batch, k, &t);
            }
            CoinChange::Remove(del_h) if *del_h <= h => {
                ks.push(*k);
                utxo_store_delete(db, &mut batch, k);
            }
            _ => (),
        }
    }
    db.write(batch).unwrap();
    ks.iter().for_each(|k| { cache.remove(k); } );
}

/// Get UTXO coin from cache and if not found, load it from disk.
pub fn get_utxo<'a, T: Decodable>(db: &DB, cache: &'a UtxoCache<T>, k: &UtxoKey) -> Option<Ref<'a, UtxoKey, CoinChange<T>>> {
    match cache.get(k) {
        Some(r) => Some(r),
        None => {
            let dbres = utxo_store_read(db, k);
            match dbres {
                None => None,
                Some(t) => {
                    cache.insert(*k, CoinChange::<T>::Pure(t));
                    cache.get(k)
                }
            }
        }
    }
}
