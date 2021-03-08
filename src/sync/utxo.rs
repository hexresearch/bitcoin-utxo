use bitcoin::{Block, BlockHash};
use bitcoin::consensus::encode;
use bitcoin::consensus::encode::{Decodable, Encodable};
use bitcoin::network::message_blockdata;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::network::message;
use bitcoin::network::message::NetworkMessage;
use futures::sink;
use futures::sink::Sink;
use futures::stream;
use futures::stream::{Stream, StreamExt};
use rocksdb::DB;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time::Duration;

use crate::cache::utxo::{UtxoCache, update_utxo};
use crate::connection::message::process_messages;
use crate::storage::chain::*;
use crate::storage::utxo::utxo_height;
use crate::utxo::UtxoState;

/// Request blocks from main chain for utxo and process them.
pub async fn sync_utxo<T: UtxoState + Decodable + Encodable>(db: Arc<DB>, cache: Arc<UtxoCache<T>>) -> (impl Stream<Item = NetworkMessage> + Unpin, impl Sink<NetworkMessage, Error = encode::Error>)
{
    const BUFFER_SIZE: usize = 100;
    let (broad_sender, _) = broadcast::channel(100);
    let (msg_sender, msg_reciver) = mpsc::channel::<NetworkMessage>(BUFFER_SIZE);
    loop {
        let utxo_h = utxo_height(&db);
        let chain_h = get_chain_height(&db);
        if chain_h > utxo_h {
            stream::iter(utxo_h+1 .. chain_h+1).for_each_concurrent(10, |h| {
                let db = db.clone();
                let cache = cache.clone();
                let broad_sender = broad_sender.clone();
                let msg_sender = msg_sender.clone();
                async move {
                    sync_block(db, cache, h, &broad_sender, &msg_sender).await;
                }
            }).await;
        }
        chain_height_changes(&db, Duration::from_secs(10)).await;
    }
    let msg_stream = ReceiverStream::new(msg_reciver);
    let msg_sink = sink::unfold(broad_sender, |broad_sender, msg| async move { broad_sender.send(msg).unwrap(); Ok::<_, encode::Error>(broad_sender) });
    (msg_stream, msg_sink)
}

async fn sync_block<T: UtxoState + Decodable + Encodable>(db: Arc<DB>, cache: Arc<UtxoCache<T>>, h: u32, broad_sender: &broadcast::Sender<NetworkMessage>, msg_sender: &mpsc::Sender<NetworkMessage>)
{
    let hash = get_block_hash(&db, h).unwrap();
    let block = request_block(&hash, broad_sender, msg_sender).await;
    println!("UTXO processing block {:?}", h);
    for tx in block.txdata {
        update_utxo(&cache, h, &block.header, &tx);
    }
}

/// Ask node about block and wait for reply.
async fn request_block(hash: &BlockHash, broad_sender: &broadcast::Sender<NetworkMessage>, msg_sender: &mpsc::Sender<NetworkMessage>) -> Block {
    let block_msg = message::NetworkMessage::GetData(vec![Inventory::Block(*hash)]);
    msg_sender.send(block_msg).await.unwrap();
    let mut receiver = broad_sender.subscribe();
    let mut block = None;
    while block == None {
        let msg = receiver.recv().await.unwrap();
        match msg {
            NetworkMessage::Block(b) if b.block_hash() == *hash => {
                block = Some(b);
            }
            _ => (),
        }
    }
    block.unwrap()
}
