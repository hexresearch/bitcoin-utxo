use futures::sink::{Sink, unfold};
use futures::stream;
use futures::stream::{Stream, StreamExt};
use bitcoin::BlockHash;
use bitcoin::consensus::encode;
use bitcoin::network::message_blockdata;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::network::message;
use bitcoin::network::message::NetworkMessage;
use futures::Future;
use rocksdb::DB;
use std::str::FromStr;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;

use crate::connection::message::process_messages;
use crate::storage::chain::{get_chain_height, get_block_locator, update_chain};

pub async fn sync_headers(db: Arc<DB>) -> (impl Stream<Item = NetworkMessage> + Unpin, impl Sink<NetworkMessage, Error = encode::Error>)
{
    process_messages(move |sender, msg| {
        let db = db.clone();
        let mut synced = false;
        async move {
            match msg {
                message::NetworkMessage::Verack => {
                    ask_headers(&db, &sender).await;
                }
                message::NetworkMessage::Headers(headers) => {
                    println!("Got {:?} headers", headers.len());
                    update_chain(&db, &headers);
                    if headers.len() < 2000 {
                        println!("Synced all headers");
                        synced = true;
                    } else {
                        ask_headers(&db, &sender).await;
                    }
                },
                message::NetworkMessage::Inv(invs) if synced => {
                    let s = &sender;
                    stream::iter(invs).for_each_concurrent(1, |inv| {
                        let db = db.clone();
                        async move {
                            match inv {
                                Inventory::Block(_) | Inventory::WitnessBlock(_) => {
                                    ask_headers(&db, s).await;
                                }
                                _ => (),
                            }
                        }
                    }).await;
                }
                _ => (),
            }
            sender
        }
    })
}

async fn ask_headers(db: &Arc<DB>, sender: &mpsc::Sender<NetworkMessage>) {
    let h = get_chain_height(&db);
    let locator_hashes = get_block_locator(&db, h);
    let stop_hash = BlockHash::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap();
    let blocks_request = message_blockdata::GetHeadersMessage::new(locator_hashes, stop_hash);
    sender.send(message::NetworkMessage::GetHeaders(blocks_request)).await.unwrap();
    println!("Sent request for blocks after {:?}", h);
}
