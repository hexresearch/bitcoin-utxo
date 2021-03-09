use rocksdb::{DB, ColumnFamily, ColumnFamilyDescriptor, Options, Error};

const CHAIN_FAMILY: &str = "chain";
const UTXO_FAMILY: &str = "utxo";

pub fn open_storage(path: &str, cfs: Vec<&str>) -> Result<DB, Error> {
    let mut db_opts = Options::default();
    db_opts.create_missing_column_families(true);
    db_opts.create_if_missing(true);
    let cfs = column_families(cfs);
    DB::open_cf_descriptors(&db_opts, path, cfs)
}

fn column_families(cfs: Vec<&str>) -> Vec<ColumnFamilyDescriptor> {
    let mut ret = vec![];
    for n in cfs {
        ret.push(ColumnFamilyDescriptor::new(n, Options::default()));
    }
    ret.push(ColumnFamilyDescriptor::new(CHAIN_FAMILY, Options::default()));
    ret.push(ColumnFamilyDescriptor::new(UTXO_FAMILY, Options::default()));
    ret
}

pub fn chain_famiy(db: &DB) -> &ColumnFamily {
    db.cf_handle(CHAIN_FAMILY).unwrap()
}

pub fn utxo_famiy(db: &DB) -> &ColumnFamily {
    db.cf_handle(UTXO_FAMILY).unwrap()
}
