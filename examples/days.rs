// The example calculates average days UTXO set left unspend
extern crate bitcoin;
extern crate bitcoin_utxo;

use futures::pin_mut;
use std::{env, process};
use std::error::Error;
use std::net::SocketAddr;
use std::sync::Arc;
use std::io;

use bitcoin::{BlockHeader, Transaction};
use bitcoin::consensus::encode;
use bitcoin::consensus::encode::{Decodable, Encodable};
use bitcoin::network::constants;

use bitcoin_utxo::connection::connect;
use bitcoin_utxo::sync::headers::sync_headers;
use bitcoin_utxo::storage::init_storage;
use bitcoin_utxo::cache::utxo::new_cache;
use bitcoin_utxo::utxo::UtxoState;

#[derive(Debug, Copy, Clone)]
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
