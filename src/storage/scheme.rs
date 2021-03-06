use rocksdb::{DB, ColumnFamily, ColumnFamilyDescriptor, Options, Error};

const CHAIN_FAMILY: &str = "chain";
const UTXO_FAMILY: &str = "utxo";

pub fn open_storage(path: &str) -> Result<DB, Error> {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    let cfs = column_families();
    DB::open_cf_descriptors(&db_opts, path, cfs)
}

fn column_families() -> Vec<ColumnFamilyDescriptor> {
    let chain_cf = ColumnFamilyDescriptor::new(CHAIN_FAMILY, Options::default());
    let utxo_cf = ColumnFamilyDescriptor::new(UTXO_FAMILY, Options::default());

    vec![chain_cf, utxo_cf]
}

pub fn chain_famiy(db: &DB) -> &ColumnFamily {
    db.cf_handle(CHAIN_FAMILY).unwrap()
}

pub fn utxo_famiy(db: &DB) -> &ColumnFamily {
    db.cf_handle(UTXO_FAMILY).unwrap()
}
