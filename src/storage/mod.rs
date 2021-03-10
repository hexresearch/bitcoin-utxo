pub mod chain;
pub mod scheme;
pub mod utxo;

use crate::storage::chain::init_chain_storage;
use crate::storage::scheme::open_storage;
use crate::storage::utxo::init_utxo_storage;
use rocksdb::{Error, DB};

pub fn init_storage(path: &str, cfs: Vec<&str>) -> Result<DB, Error> {
    let db = open_storage(path, cfs)?;
    init_chain_storage(&db);
    init_utxo_storage(&db);
    Ok(db)
}
