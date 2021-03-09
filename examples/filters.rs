// The example calculates BIP158 filters for each block
extern crate bitcoin;
extern crate bitcoin_utxo;

use futures::pin_mut;
use futures::SinkExt;
use futures::stream;

use rocksdb::{DB, WriteBatch};

use std::{env, process};
use std::collections::HashMap;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::time::Duration;
use hex;

use bitcoin::util::bip158::BlockFilter;
use bitcoin::util::bip158;
use bitcoin::{BlockHeader, Block, BlockHash, Transaction, Script, OutPoint};
use bitcoin::consensus::encode;
use bitcoin::consensus::encode::{Decodable, Encodable};
use bitcoin::network::constants;

use bitcoin_utxo::cache::utxo::*;
use bitcoin_utxo::connection::connect;
use bitcoin_utxo::storage::init_storage;
use bitcoin_utxo::sync::headers::sync_headers;
use bitcoin_utxo::sync::utxo::*;
use bitcoin_utxo::utxo::UtxoState;

#[derive(Debug, Clone)]
struct FilterCoin {
    script: Script,
}

impl UtxoState for FilterCoin {
    fn new_utxo(_height: u32, _header: &BlockHeader, tx: &Transaction, vout: u32) -> Self {
        FilterCoin {
            script: tx.output[vout as usize].script_pubkey.clone(),
        }
    }
}

impl Encodable for FilterCoin {
    fn consensus_encode<W: io::Write>(&self, writer: W) -> Result<usize, io::Error> {
        let len = self.script.consensus_encode(writer)?;
        Ok(len)
    }
}
impl Decodable for FilterCoin {
     fn consensus_decode<D: io::Read>(mut d: D) -> Result<Self, encode::Error> {
         Ok(FilterCoin {
             script: Decodable::consensus_decode(&mut d)?,
         })
     }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("not enough arguments");
        process::exit(1);
    }

    let str_address = &args[1];

    let address: SocketAddr = str_address.parse().unwrap_or_else(|error| {
        eprintln!("Error parsing address: {:?}", error);
        process::exit(1);
    });

    let db = Arc::new(init_storage("./filters_utxo_db", vec!["filters"])?);
    let cache = Arc::new(new_cache::<FilterCoin>());

    let (headers_stream, headers_sink) = sync_headers(db.clone()).await;
    pin_mut!(headers_sink);
    let (sync_future, utxo_stream, utxo_sink) = sync_utxo_with(db.clone(), cache.clone(), move |h, block| {
        let block = block.clone();
        let db = db.clone();
        let cache = cache.clone();
        async move {
            let hash = block.block_hash();
            let filter = generate_filter(db.clone(), cache, block).await;
            if h % 1000 == 0 {
                println!("Filter for block {:?}: {:?}", h, hex::encode(&filter.content));
            }
            store_filter(db, &hash, filter);
        }
    }).await;
    pin_mut!(utxo_sink);


    let msg_stream = stream::select(headers_stream, utxo_stream);
    let msg_sink = headers_sink.fanout(utxo_sink);
    let conn_future = connect(
        &address,
        constants::Network::Bitcoin,
        "rust-client".to_string(),
        0,
        msg_stream,
        msg_sink,
    );

    tokio::spawn(async move {
        sync_future.await;
    });
    conn_future.await.unwrap();

    Ok(())
}

async fn generate_filter(db: Arc<DB>, cache: Arc<UtxoCache<FilterCoin>>, block: Block) -> BlockFilter {
    let mut hashmap = HashMap::<OutPoint, Script>::new();
    for tx in &block.txdata {
        if !tx.is_coin_base() {
            for i in &tx.input {
                let coin = wait_utxo(db.clone(), cache.clone(), &i.previous_output, Duration::from_millis(100)).await;
                hashmap.insert(i.previous_output, coin.script);
            }
        }
    }
    BlockFilter::new_script_filter(&block, |out| {
        hashmap.get(out).map_or(Err(bip158::Error::UtxoMissing(*out)), |s| Ok(s.clone()))
    }).unwrap()
}

fn store_filter(db: Arc<DB>, hash: &BlockHash, filter:BlockFilter) {
    let cf = db.cf_handle("filters").unwrap();
    let mut batch = WriteBatch::default();
    batch.put_cf(cf, hash, filter.content);
    db.write(batch).unwrap();
}
