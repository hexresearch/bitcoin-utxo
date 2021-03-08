use bitcoin::blockdata::transaction::{Transaction, OutPoint};
use bitcoin::blockdata::block::BlockHeader;
use dashmap::DashMap;
use rocksdb::DB;
use std::collections::hash_map::RandomState;
use std::sync::Arc;

use crate::utxo::{UtxoKey, UtxoState};
use crate::storage::utxo;

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

/// Remove all inputs from UTXO and add all outputs.
pub fn update_utxo<T: UtxoState>(cache: &UtxoCache<T>, h: u32, header: &BlockHeader, tx: &Transaction) {
    for txin in &tx.input {
        remove_utxo(cache, h, &txin.previous_output);
    }
    let mut outPoint = OutPoint {
        txid: tx.txid(),
        vout: 0,
    };
    for i in 0..tx.output.len() {
        outPoint.vout = i as u32;
        let t = T::newUtxoState(h, header, &tx, i as u32);
        add_utxo(cache, h, &outPoint, t);
    }
}

fn remove_utxo<T>(cache: &UtxoCache<T>, h: u32, k: &UtxoKey) {
    cache.insert(*k, CoinChange::<T>::Remove(h) );
}

fn add_utxo<T>(cache: &UtxoCache<T>, h: u32, k: &UtxoKey, t: T) {
    cache.insert(*k, CoinChange::<T>::Add(t, h) );
}
