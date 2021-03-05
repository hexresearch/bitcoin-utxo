// The example calculates average days UTXO set left unspend
extern crate bitcoin;
extern crate bitcoin_utxo;

use futures::sink;
use futures::stream;
use futures::SinkExt;
use std::error::Error;
use std::io;
use std::net::SocketAddr;
use std::{env, process};

// use bitcoin::blockdata;
use bitcoin::consensus::encode;
// use bitcoin::secp256k1;
// use bitcoin::secp256k1::rand::Rng;
// use bitcoin::BlockHash;
use bitcoin::network::constants;

use bitcoin_utxo::connection::connect;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // This example establishes a connection to a Bitcoin node, sends the intial
    // "version" message, waits for the reply, and finally closes the connection.
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

    // let genesis_hash = blockdata::constants::genesis_block(constants::Network::Bitcoin).block_hash();
    // let locator_hashes = vec![genesis_hash];
    // let stop_hash = BlockHash::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap();
    // let blocks_request = message_blockdata::GetBlocksMessage::new(locator_hashes, stop_hash);
    // let test_message = message::RawNetworkMessage {
    //     magic: constants::Network::Bitcoin.magic(),
    //     payload: message::NetworkMessage::GetBlocks(blocks_request),
    // };
    // let _ = stream.write_all(encode::serialize(&test_message).as_slice());
    // println!("Sent request for blocks after genesis");

    let imsgs = stream::pending();
    let outmsgs = sink::drain().sink_map_err(|_| {
        encode::Error::Io(io::Error::new(
            io::ErrorKind::Other,
            "Impossible error in sink!",
        ))
    });
    connect(
        &address,
        constants::Network::Bitcoin,
        "rust-client".to_string(),
        0,
        imsgs,
        outmsgs,
    )
    .await?;

    Ok(())
}
