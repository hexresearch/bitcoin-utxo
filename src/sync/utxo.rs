use bitcoin::consensus::encode;
use bitcoin::consensus::encode::{Decodable, Encodable};
use bitcoin::network::message;
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_blockdata::Inventory;
use bitcoin::{Block, BlockHash};
use chrono::Utc;
use futures::sink;
use futures::sink::Sink;
use futures::stream;
use futures::stream::{Stream, TryStreamExt, StreamExt};
use futures::Future;
use rocksdb::DB;
use std::sync::Arc;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::sync::Barrier;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::cache::utxo::*;
use crate::storage::chain::*;
use crate::storage::utxo::utxo_height;
use crate::utxo::UtxoState;

use std::fmt::Debug;

/// Amount of blocks to process in parallel
pub const DEF_BLOCK_BATCH: usize = 100;

#[derive(Debug)]
pub enum UtxoSyncError {
    BlockReq(u32, BlockHash)
}

/// Future blocks until utxo height == chain height
pub async fn wait_utxo_sync(db: Arc<DB>, dur: Duration) {
    loop {
        let utxo_h = utxo_height(&db);
        let chain_h = get_chain_height(&db);
        if utxo_h == chain_h {
            break;
        } else {
            sleep(dur).await;
        }
    }
}

/// Future that blocks until current height of utxo is changed
pub async fn wait_utxo_height_changes(db: Arc<DB>, dur: Duration) {
    let start_h = utxo_height(&db);
    loop {
        let cur_h = utxo_height(&db);
        if cur_h == start_h {
            sleep(dur).await;
        } else {
            break;
        }
    }
}

pub async fn sync_utxo<T>(
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    fork_height: u32,
    max_coins: usize,
    flush_period: u32,
    block_batch: usize,
) -> (
    impl Future<Output = Result<(), UtxoSyncError>>,
    impl Stream<Item = NetworkMessage> + Unpin,
    impl Sink<NetworkMessage, Error = encode::Error>,
)
where
    T: UtxoState + Decodable + Encodable + Clone + Debug + Sync + Send + 'static,
{
    sync_utxo_with(
        db,
        cache,
        fork_height,
        max_coins,
        flush_period,
        block_batch,
        |_, _| async move {},
    )
    .await
}

pub async fn sync_utxo_with<T, F, U>(
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    fork_height: u32,
    max_coins: usize,
    flush_period: u32,
    block_batch: usize,
    with: F,
) -> (
    impl Future<Output = Result<(), UtxoSyncError>>,
    impl Stream<Item = NetworkMessage> + Unpin,
    impl Sink<NetworkMessage, Error = encode::Error>,
)
where
    T: UtxoState + Decodable + Encodable + Clone + Debug + Sync + Send + 'static,
    F: FnMut(u32, &Block) -> U + Clone + Send + 'static,
    U: Future<Output = ()> + Send,
{
    const BUFFER_SIZE: usize = 100;
    let (broad_sender, _) = broadcast::channel(BUFFER_SIZE);
    let (msg_sender, msg_reciver) = mpsc::unbounded_channel::<NetworkMessage>();
    let sync_future = {
        let broad_sender = broad_sender.clone();
        async move {
            wait_handshake(&broad_sender).await;
            let mut last_sync_height = 0;
            loop {
                let utxo_h = utxo_height(&db).max(last_sync_height);
                let chain_h = get_chain_height(&db);
                let barrier = Arc::new(Barrier::new(block_batch));
                println!("UTXO height {:?}, chain height {:?}", utxo_h, chain_h);
                if chain_h > utxo_h {
                    // We should add padding futures at end of sync to successfully finish sync
                    let clip_batch = (((chain_h as f32) / (block_batch as f32)).ceil() * block_batch as f32) as u32;
                    let min_batch = utxo_h + block_batch as u32;
                    let upper_h = min_batch.max(clip_batch);
                    stream::iter(utxo_h + 1 .. upper_h + 1).map(Ok)
                        .try_for_each_concurrent(block_batch, |h| {
                            let db = db.clone();
                            let cache = cache.clone();
                            let broad_sender = broad_sender.clone();
                            let msg_sender = msg_sender.clone();
                            let with = with.clone();
                            let barrier = barrier.clone();
                            let flush_h = utxo_height(&db) + flush_period;
                            async move {
                                tokio::spawn(async move {
                                    if h <= chain_h { // If we are padding thread, just wait on barriers
                                        sync_block(
                                            db.clone(),
                                            cache.clone(),
                                            h,
                                            chain_h,
                                            with,
                                            &broad_sender,
                                            &msg_sender,
                                        )
                                        .await?;
                                    }
                                    finish_block_barrier(
                                        &db,
                                        &cache,
                                        fork_height,
                                        max_coins,
                                        flush_period,
                                        flush_h,
                                        h,
                                        false,
                                        barrier,
                                    )
                                    .await;
                                    Ok::<(), UtxoSyncError>(())
                                })
                                .await
                                .unwrap()?;
                                Ok::<(), UtxoSyncError>(())
                            }
                        })
                        .await?;
                    println!("UTXO sync finished");
                    last_sync_height = chain_h;
                }
                finish_block(
                    &db,
                    &cache,
                    fork_height,
                    max_coins,
                    flush_period,
                    chain_h,
                    chain_h,
                    true,
                );
                chain_height_changes(&db, Duration::from_secs(10)).await;

            }
        }
    };
    let msg_stream = UnboundedReceiverStream::new(msg_reciver);
    let msg_sink = sink::unfold(broad_sender, |broad_sender, msg| async move {
        broad_sender.send(msg).unwrap_or(0);
        Ok::<_, encode::Error>(broad_sender)
    });
    (sync_future, msg_stream, msg_sink)
}

async fn sync_block<T, F, U>(
    db: Arc<DB>,
    cache: Arc<UtxoCache<T>>,
    h: u32,
    maxh: u32,
    mut with: F,
    broad_sender: &broadcast::Sender<NetworkMessage>,
    msg_sender: &mpsc::UnboundedSender<NetworkMessage>,
) -> Result<(), UtxoSyncError>
  where
    T: UtxoState + Decodable + Encodable + Clone + Debug,
    F: FnMut(u32, &Block) -> U,
    U: Future<Output = ()>,
{
    let hash = get_block_hash(&db, h).unwrap();
    // println!("Requesting block {:?} and hash {:?}", h, hash);
    let block = request_block(h, &hash, broad_sender, msg_sender).await?;
    let now = Utc::now().format("%Y-%m-%d %H:%M:%S");
    let progress = 100.0 * h as f32 / maxh as f32;
    println!("{}: UTXO processing block {:?} ({:.2}%)", now, h, progress);
    for tx in &block.txdata {
        update_utxo_outputs(&cache, h, &block.header, &tx);
    }
    with(h, &block).await;
    for tx in block.txdata {
        update_utxo_inputs(&db, &cache, h, &tx);
    }
    Ok(())
}

/// Ask node about block and wait for reply.
async fn request_block(
    h: u32,
    hash: &BlockHash,
    broad_sender: &broadcast::Sender<NetworkMessage>,
    msg_sender: &mpsc::UnboundedSender<NetworkMessage>,
) -> Result<Block, UtxoSyncError> {
    let block_msg = message::NetworkMessage::GetData(vec![Inventory::Block(*hash)]);
    msg_sender.send(block_msg).map_err(|e| { println!("Error when requesting block at height {:?}: {:?}", h, e); UtxoSyncError::BlockReq(h, *hash)})?;
    let mut receiver = broad_sender.subscribe();
    let mut block = None;
    while block == None {
        let resend_future = tokio::time::sleep(Duration::from_secs(5));
        tokio::pin!(resend_future);
        tokio::select! {
            _ = &mut resend_future => {
                println!("Resend request for block {:?}", hash);
                let block_msg = message::NetworkMessage::GetData(vec![Inventory::Block(*hash)]);
                msg_sender.send(block_msg).map_err(|e| { println!("Error when rerequesting block at height {:?}: {:?}", h, e); UtxoSyncError::BlockReq(h, *hash)})?;
            }
            emsg = receiver.recv() => match emsg {
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    let block_msg = message::NetworkMessage::GetData(vec![Inventory::Block(*hash)]);
                    msg_sender.send(block_msg).map_err(|e| { println!("Error when rerequesting block at height {:?}: {:?}", h, e); UtxoSyncError::BlockReq(h, *hash)})?;
                }
                Err(e) => {
                    eprintln!("Request block {:?} failed with recv error: {:?}", hash, e);
                    panic!("Failed to request block");
                }
                Ok(msg) => match msg {
                    NetworkMessage::Block(b) if b.block_hash() == *hash => {
                        block = Some(b);
                    }
                    _ => (),
                },
            }
        }
    }
    Ok(block.unwrap())
}

async fn wait_handshake(broad_sender: &broadcast::Sender<NetworkMessage>) {
    let mut receiver = broad_sender.subscribe();
    let mut handshaked = false;
    while !handshaked {
        let msg = receiver.recv().await.unwrap();
        match msg {
            NetworkMessage::Verack => {
                handshaked = true;
            }
            _ => (),
        }
    }
}
