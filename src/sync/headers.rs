use futures::sink::{Sink, unfold};
use futures::stream::{Stream};
use futures::Future;
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;
use bitcoin::consensus::encode;
use bitcoin::network::message;
use bitcoin::network::message_blockdata;
use bitcoin::network::message::NetworkMessage;
use bitcoin::BlockHash;
use std::str::FromStr;
use std::sync::Arc;
use rocksdb::DB;

use crate::connection::message::process_messages;
use crate::storage::chain::{get_chain_height, get_block_locator, update_chain};

pub async fn sync_headers(db: Arc<DB>) -> (impl Stream<Item = NetworkMessage> + Unpin, impl Sink<NetworkMessage, Error = encode::Error>)
{
    process_messages(move |sender, msg| {
        let db = db.clone();
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
                    } else {
                        ask_headers(&db, &sender).await;
                    }
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
