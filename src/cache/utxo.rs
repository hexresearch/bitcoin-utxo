use crate::storage::utxo::*;
use crate::utxo::{UtxoKey, UtxoState};
use crate::{storage::scheme::utxo_famiy, sync::utxo::UtxoSyncError};
use bitcoin::blockdata::block::BlockHeader;
use bitcoin::blockdata::transaction::{OutPoint, Transaction};
use bitcoin::consensus::encode::{Decodable, Encodable};
use dashmap::iter::Iter;
use dashmap::mapref::multiple::RefMulti;
use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use rayon::prelude::*;
use rocksdb::{WriteBatch, DB};
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::ops::Div;
use std::sync::Arc;
use time::Instant;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

/// Maximum fork depth after which we can flush UTXO to disk
pub const UTXO_FORK_MAX_DEPTH: u32 = 100;
/// Dump UTXO every given amount of blocks
pub const UTXO_FLUSH_PERIOD: u32 = 15000;
/// Dump UTXO if we get more than given amount of coins to save memory
pub const UTXO_CACHE_MAX_COINS: usize = 17_000_000;

/// Cache of UTXO coins with T payload. We keep unrollbackable UTXO set in database
/// and the most recent UTXO set in memory.
pub type UtxoCache<T> = DashMap<UtxoKey, CoinChange<T>, RandomState>;

pub type UtxoIterator<'a, T> = Iter<'a, UtxoKey, CoinChange<T>, RandomState, UtxoCache<T>>;

pub type UtxoRef<'a, T> = RefMulti<'a, UtxoKey, CoinChange<T>, RandomState>;

pub fn new_cache<T>() -> UtxoCache<T> {
    DashMap::new()
}

/// Tracks changes of UTXO set. Each N blocks we dump changes to disk, that allows to get
/// cheap support for fork resistance. If fork is dected, we drop cache and start from
/// storage backed state of UTXO.
#[derive(Debug, Clone)]
pub enum CoinChange<T> {
    Pure(T, u32), // height when we loaded the value in memory
    Add(T, u32),
    Remove(T, u32, u32), // Store old value for slowpoke threads that need old value. First height is when coin was added, second when deleted.
}

impl<T> CoinChange<T> {
    /// Get payload of utxo state change
    pub fn payload(&self) -> &T {
        match self {
            CoinChange::Pure(t, _) => t,
            CoinChange::Add(t, _) => t,
            CoinChange::Remove(t, _, _) => t,
        }
    }
}

/// Remove all inputs from UTXO and add all outputs.
pub fn update_utxo<T: UtxoState + Decodable + Clone>(
    db: &DB,
    cache: &UtxoCache<T>,
    h: u32,
    header: &BlockHeader,
    tx: &Transaction,
) {
    update_utxo_inputs(db, cache, h, tx);
    update_utxo_outputs(cache, h, header, tx);
}

/// Remove all inputs of tx from UTXO set
pub fn update_utxo_inputs<T: Decodable + Clone>(
    db: &DB,
    cache: &UtxoCache<T>,
    h: u32,
    tx: &Transaction,
) {
    for txin in &tx.input {
        remove_utxo(db, cache, h, &txin.previous_output);
    }
}

/// Add all outputs of transaction to UTXO set
pub fn update_utxo_outputs<T: UtxoState>(
    cache: &UtxoCache<T>,
    h: u32,
    header: &BlockHeader,
    tx: &Transaction,
) {
    let mut out = OutPoint {
        txid: tx.txid(),
        vout: 0,
    };
    for i in 0..tx.output.len() {
        if !tx.output[i].script_pubkey.is_op_return() {
            out.vout = i as u32;
            let t = T::new_utxo(h, header, &tx, i as u32);
            add_utxo(cache, h, &out, t);
        }
    }
}

fn remove_utxo<T: Decodable + Clone>(db: &DB, cache: &UtxoCache<T>, h: u32, k: &UtxoKey) {
    let mut insert = None;
    match cache.get(k) {
        None => {
            insert = utxo_store_read(db, k);
        }
        Some(v) => match v.value() {
            CoinChange::Pure(t, _) => insert = Some((t.clone(), h)),
            CoinChange::Add(t, ah) => insert = Some((t.clone(), *ah)),
            CoinChange::Remove(_, _, _) => (),
        },
    };
    if let Some((t, ch)) = insert {
        cache.insert(*k, CoinChange::<T>::Remove(t, ch, h));
    }
}

fn add_utxo<T>(cache: &UtxoCache<T>, h: u32, k: &UtxoKey, t: T) {
    let mut insert = false;
    match cache.get(k) {
        None => insert = true,
        Some(_) => (),
    }
    if insert {
        cache.insert(*k, CoinChange::<T>::Add(t, h));
    }
}

/// Flush UTXO to database if UTXO changes are old enough to avoid forks.
pub async fn finish_block<T: 'static + Encodable + Clone + Send + Sync>(
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    fork_height: u32,
    max_coins: usize,
    flush_period: u32,
    start_h: u32,
    end_h: u32,
    force: bool,
) {
    let coins = cache.len();
    if force && end_h > fork_height {
        println!("Writing UTXO to disk...");
        flush_utxo(
            db,
            cache,
            end_h - flush_period / 2,
            end_h - fork_height,
            coins > max_coins,
        )
        .await;
        println!("Writing UTXO to disk is done");
    } else if end_h > fork_height
        && ((start_h.div(flush_period) != end_h.div(flush_period)) || coins > max_coins)
    {
        println!("UTXO cache size is {:?} coins", coins);
        println!("Writing UTXO to disk...");
        flush_utxo(
            db,
            cache,
            end_h - flush_period / 2,
            end_h - fork_height,
            coins > max_coins,
        )
        .await;
        println!("Writing UTXO to disk is done");
    }
}

/// Flush all UTXO changes to database if change older or equal than given height.
pub async fn flush_utxo<T: 'static + Encodable + Clone + Send + Sync>(
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    oldest_pure: u32,
    h: u32,
    flush_pure: bool,
) {
    let mut ks: HashMap<OutPoint, Option<CoinChange<T>>> = HashMap::new();
    let batch = Arc::new(Mutex::new(WriteBatch::default()));
    let start = Instant::now();
    println!("Choose which coins to dump");

    for r in cache.iter() {
        let k = r.key();
        match r.value() {
            CoinChange::Add(t, add_h) if *add_h <= h => {
                if *add_h >= oldest_pure {
                    ks.insert(*k, Some(CoinChange::Pure(t.clone(), *add_h)));
                }
                let mut batch = batch.lock().await;
                utxo_store_insert(&db, &mut batch, k, &t);
            }
            CoinChange::Remove(t, add_h, del_h)
                if *add_h <= h && *del_h > h && *add_h != *del_h =>
            {
                ks.insert(*k, Some(CoinChange::Remove(t.clone(), *add_h, *del_h)));
                let mut batch = batch.lock().await;
                utxo_store_insert(&db, &mut batch, &k, &t);
            }
            CoinChange::Remove(_, _, del_h) if *del_h <= h => {
                ks.insert(*k, None);
                let mut batch = batch.lock().await;
                utxo_store_delete(&db, &mut batch, &k);
            }
            CoinChange::Pure(_, touch_h) if flush_pure && *touch_h < oldest_pure => {
                ks.insert(*k, None);
            }
            _ => (),
        }
    }

    println!("Cleaning cache");
    ks.par_iter().for_each(|(k, v)| {
        if let Some(t) = v {
            cache.insert(*k, t.clone());
        } else {
            cache.remove(k);
        }
    });
    println!(
        "Required {} seconds for cache traversal.",
        start.elapsed().as_seconds_f32()
    );

    let mut batch = Arc::try_unwrap(batch)
        .unwrap_or_else(|_| panic!("Impossible!"))
        .into_inner();
    set_utxo_height(&mut batch, utxo_famiy(&db), h);
    println!("Writing to disk");
    db.write(batch).unwrap();
}

/// Get UTXO coin from cache and if not found, load it from disk.
pub fn get_utxo<'a, T: Decodable>(
    db: &DB,
    cache: &'a UtxoCache<T>,
    k: &UtxoKey,
    h: u32,
) -> Option<Ref<'a, UtxoKey, CoinChange<T>>> {
    match cache.get(k) {
        Some(r) => Some(r),
        None => {
            let dbres = utxo_store_read(db, k);
            match dbres {
                None => None,
                Some(t) => {
                    cache.insert(*k, CoinChange::<T>::Pure(t, h));
                    cache.get(k)
                }
            }
        }
    }
}

/// Get UTXO coin from cache/storage and if not found, wait until it appears.
pub async fn wait_utxo<T: Decodable + Clone>(
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    k: &UtxoKey,
    h: u32,
    dur: Duration,
) -> Result<T, UtxoSyncError> {
    let mut value = get_utxo(&db, &cache, k, h);
    let mut counter: u32 = 0;
    loop {
        match value {
            None => {
                // println!("Awaiting UTXO for {}", k);
                sleep(dur).await;
                value = get_utxo(&db, &cache, k, h);
                counter += 1;
                if counter > 1000 {
                    return Err(UtxoSyncError::CoinWaitTimeout(h, *k));
                }
            }
            Some(v) => return Ok(v.value().payload().clone()),
        }
    }
}

/// Get UTXO coin from cache and if not found, load it from disk.
/// Does not ask for the height at which the coin was encountered
/// Nor does it modify the cache
/// Used to build mempool filters
pub fn get_utxo_noh<'a, T: Decodable + Clone>(
    db: &DB,
    cache: &'a UtxoCache<T>,
    k: &UtxoKey,
) -> Option<T> {
    cache
        .get(k)
        .map(|cc| cc.payload().clone())
        .or_else(|| utxo_store_read(db, k))
}

/// Get UTXO coin from cache/storage and if not found, wait until it appears.
/// Does not ask for the height at which the coin was encountered
/// Nor does it modify the cache
/// Used to build mempool filters
pub async fn wait_utxo_noh<T: Decodable + Clone>(
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    k: &UtxoKey,
    dur: Duration,
) -> Result<T, UtxoSyncError> {
    let mut value = get_utxo_noh(&db, &cache, k);
    let mut counter: u32 = 0;
    loop {
        match value {
            None => {
                // println!("Awaiting UTXO for {}", k);
                sleep(dur).await;
                value = get_utxo_noh(&db, &cache, k);
                counter += 1;
                if counter > 1000 {
                    return Err(UtxoSyncError::CoinWaitTimeout(0, *k));
                }
            }
            Some(v) => return Ok(v),
        }
    }
}
