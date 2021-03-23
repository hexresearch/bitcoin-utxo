use bitcoin::BlockHash;
use bitcoin::consensus::encode;
use bitcoin::network::message_blockdata;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::network::message;
use bitcoin::network::message::NetworkMessage;
use futures::future::Future;
use futures::sink::Sink;
use futures::stream;
use futures::stream::{Stream, StreamExt};
use rocksdb::DB;
use std::error::Error;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::Duration;

use crate::connection::message::process_messages;
use crate::storage::chain::{get_block_locator, get_chain_height, update_chain};

/// Request all headers from node and keep DB for main chain updated
pub async fn sync_headers(
    db: Arc<DB>,
) -> (
    impl Future<Output = Result<(), Box<dyn Error>>>,
    impl Stream<Item = NetworkMessage> + Unpin,
    impl Sink<NetworkMessage, Error = encode::Error>,
) {
    let synced = Arc::new(Mutex::new(false));
    let db_fut = db.clone();
    let synced_fut = synced.clone();
    let (sender, msg_stream, msg_sink) = process_messages(move |sender, msg| {
        let db = db.clone();
        let synced = synced.clone();
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
                        let mut s = synced.lock().unwrap();
                        *s = true;
                    } else {
                        ask_headers(&db, &sender).await;
                        let mut s = synced.lock().unwrap();
                        *s = false;
                    }
                }
                message::NetworkMessage::Inv(invs) => {
                    let s = synced.lock().unwrap();
                    if *s {
                        stream::iter(invs)
                            .for_each(|inv| {
                                let db = db.clone();
                                let sender = sender.clone();
                                async move {
                                    match inv {
                                        Inventory::Block(_) | Inventory::WitnessBlock(_) => {
                                            ask_headers(&db, &sender).await;
                                        }
                                        _ => (),
                                    }
                                }
                            })
                            .await;
                    }
                }
                _ => (),
            }
            sender
        }
    });
    let request_future = {
        let db = db_fut.clone();
        let synced = synced_fut.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                let is_synced: bool = *synced.lock().unwrap();
                if is_synced {
                    ask_headers(&db, &sender).await;
                }
            }
        }
    };
    (request_future, msg_stream, msg_sink)
}

async fn ask_headers(db: &Arc<DB>, sender: &mpsc::Sender<NetworkMessage>) {
    let h = get_chain_height(&db);
    let locator_hashes = get_block_locator(&db, h);
    let stop_hash =
        BlockHash::from_str("0000000000000000000000000000000000000000000000000000000000000000")
            .unwrap();
    let blocks_request = message_blockdata::GetHeadersMessage::new(locator_hashes, stop_hash);
    sender
        .send(message::NetworkMessage::GetHeaders(blocks_request))
        .await
        .unwrap();
    println!("Sent request for blocks after {:?}", h);
}
