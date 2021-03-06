// The example calculates average days UTXO set left unspend
extern crate bitcoin;
extern crate bitcoin_utxo;

use futures::future;
use futures::pin_mut;
use futures::stream;
use futures::StreamExt;
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
use bitcoin::network::message_blockdata::Inventory;

use bitcoin_utxo::connection::connect;
use bitcoin_utxo::connection::message::process_messages;

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

    let (msg_stream, msg_sink) = process_messages(|sender, msg| async move {
        match msg {
            message::NetworkMessage::Verack => {
                let genesis_hash = blockdata::constants::genesis_block(constants::Network::Bitcoin).block_hash();
                let locator_hashes = vec![genesis_hash];
                let stop_hash = BlockHash::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap();
                let blocks_request = message_blockdata::GetBlocksMessage::new(locator_hashes, stop_hash);
                sender.send(message::NetworkMessage::GetBlocks(blocks_request)).await.unwrap();
                println!("Sent request for blocks after genesis");
            }
            message::NetworkMessage::Inv(invs) => {
                let s = &sender;
                stream::iter(invs).for_each_concurrent(1, |inv| async move {
                    match inv {
                        Inventory::Block(hash) => {
                            s.send(message::NetworkMessage::GetData(vec![inv])).await.unwrap();
                            println!("Sent request for block {:?}", hash);
                        }
                        _ => (),
                    }
                }).await;
            }
            _ => {
                println!("Got message {:?}", msg);
            }
        }
        sender
    });
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
