pub mod scheme;
pub mod chain;
pub mod utxo;

use rocksdb::{DB, Error};
use crate::storage::chain::init_chain_storage;
use crate::storage::scheme::open_storage;

pub fn init_storage(path: &str) -> Result<DB, Error> {
    let db = open_storage(path)?;
    init_chain_storage(&db);
    Ok(db)
}
