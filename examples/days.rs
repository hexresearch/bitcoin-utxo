// The example calculates average days UTXO set left unspend
extern crate bitcoin;
extern crate bitcoin_utxo;

use futures::pin_mut;
use std::{env, process};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;

use bitcoin::{BlockHeader, Transaction};
use bitcoin::network::constants;

use bitcoin_utxo::connection::connect;
use bitcoin_utxo::sync::headers::sync_headers;
use bitcoin_utxo::storage::init_storage;
use bitcoin_utxo::cache::utxo::new_cache;
use bitcoin_utxo::utxo::UtxoState;

struct DaysCoin {
    created: u32,
}

impl UtxoState for DaysCoin {
    fn new_utxo(height: u32, header: &BlockHeader, tx: &Transaction, vout: u32) -> Self {
        DaysCoin {
            created: header.time,
        }
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

    let db = Arc::new(init_storage("./utxo_db")?);
    let cache = Arc::new(new_cache::<DaysCoin>());

    let (msg_stream, msg_sink) = sync_headers(db).await;
    pin_mut!(msg_sink);

    connect(
        &address,
        constants::Network::Bitcoin,
        "rust-client".to_string(),
        0,
        msg_stream,
        msg_sink,
    )
    .await?;

    Ok(())
}
