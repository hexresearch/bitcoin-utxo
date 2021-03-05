// The example calculates average days UTXO set left unspend
extern crate bitcoin;
extern crate bitcoin_utxo;

use futures::pin_mut;
use futures::sink;
use std::{env, process};
use std::error::Error;
use std::net::SocketAddr;
use std::str::FromStr;
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;

use bitcoin::blockdata;
use bitcoin::BlockHash;
use bitcoin::consensus::encode;
use bitcoin::network::constants;
use bitcoin::network::message_blockdata;
use bitcoin::network::message;

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

    const BUFFER_SIZE: usize = 10;
    let (msg_sender, msg_reciver) = mpsc::channel::<message::NetworkMessage>(BUFFER_SIZE);
    let msg_sink = sink::unfold(msg_sender, |sender, msg: message::NetworkMessage| async move {
        match msg {
            message::NetworkMessage::Verack => {
                let genesis_hash = blockdata::constants::genesis_block(constants::Network::Bitcoin).block_hash();
                let locator_hashes = vec![genesis_hash];
                let stop_hash = BlockHash::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap();
                let blocks_request = message_blockdata::GetBlocksMessage::new(locator_hashes, stop_hash);
                sender.send(message::NetworkMessage::GetBlocks(blocks_request)).await.unwrap();
                println!("Sent request for blocks after genesis");
                Ok::<_, encode::Error>(sender)
            }
            _ => {
                println!("Got message {:?}", msg);
                Ok::<_, encode::Error>(sender)
            }
        }
    });
    pin_mut!(msg_sink);
    let msg_stream = ReceiverStream::new(msg_reciver);

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
