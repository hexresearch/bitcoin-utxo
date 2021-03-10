// The example calculates average days UTXO set left unspend
extern crate bitcoin;
extern crate bitcoin_utxo;

use futures::pin_mut;
use futures::stream;
use futures::SinkExt;
use tokio::time::Duration;

use rocksdb::DB;

use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, process};

use bitcoin::consensus::encode;
use bitcoin::consensus::encode::{Decodable, Encodable};
use bitcoin::network::constants;
use bitcoin::{BlockHeader, Transaction};

use bitcoin_utxo::cache::utxo::new_cache;
use bitcoin_utxo::connection::connect;
use bitcoin_utxo::storage::init_storage;
use bitcoin_utxo::storage::utxo::utxo_iterator;
// use bitcoin_utxo::storage::chain::overwite_chain_height;
use bitcoin_utxo::sync::headers::sync_headers;
use bitcoin_utxo::sync::utxo::*;
use bitcoin_utxo::utxo::UtxoState;

#[derive(Debug, Copy, Clone)]
struct DaysCoin {
    created: u32,
}

impl UtxoState for DaysCoin {
    fn new_utxo(_height: u32, header: &BlockHeader, _tx: &Transaction, _vout: u32) -> Self {
        DaysCoin {
            created: header.time,
        }
    }
}

impl Encodable for DaysCoin {
    fn consensus_encode<W: io::Write>(&self, writer: W) -> Result<usize, io::Error> {
        let len = self.created.consensus_encode(writer)?;
        Ok(len)
    }
}
impl Decodable for DaysCoin {
    fn consensus_decode<D: io::Read>(mut d: D) -> Result<Self, encode::Error> {
        Ok(DaysCoin {
            created: Decodable::consensus_decode(&mut d)?,
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

    let db = Arc::new(init_storage("./days_utxo_db", vec![])?);
    let cache = Arc::new(new_cache::<DaysCoin>());

    // overwite_chain_height(&db, 673837);

    let (headers_stream, headers_sink) = sync_headers(db.clone()).await;
    pin_mut!(headers_sink);
    let (sync_future, utxo_stream, utxo_sink) = sync_utxo(db.clone(), cache).await;
    pin_mut!(utxo_sink);
    let days_future = watch_utxo_days(db);

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
    tokio::spawn(async move {
        days_future.await;
    });
    conn_future.await.unwrap();

    Ok(())
}

async fn watch_utxo_days(db: Arc<DB>) {
    loop {
        wait_utxo_sync(db.clone(), Duration::from_secs(2)).await;
        println!("Days calculation...");
        let days = calc_days(db.clone());
        println!("Average unspent days in UTXO: {:?}", days);
        wait_utxo_height_changes(db.clone(), Duration::from_secs(2)).await;
    }
}

const SECS_IN_DAY: u32 = 60 * 60 * 24;

fn calc_days(db: Arc<DB>) -> f64 {
    let start: u32 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs() as u32;
    let start_days = start / SECS_IN_DAY;
    let mut total_days = 0;
    let mut n = 0;
    for (_, v) in utxo_iterator::<DaysCoin>(&db) {
        total_days += v.created / SECS_IN_DAY - start_days;
        n += 1;
    }
    total_days as f64 / n as f64
}
