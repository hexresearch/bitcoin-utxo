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
use futures::stream::{Stream, StreamExt, TryStreamExt};
use futures::Future;
use log::*;
use rocksdb::WriteBatch;
use rocksdb::DB;
use std::marker::Send;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::Mutex;

use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::storage::chain::*;
use crate::storage::scheme::utxo_famiy;
use crate::storage::utxo::{set_sync_height, sync_height, utxo_height};
use crate::utxo::UtxoState;
use crate::{cache::utxo::*, utxo::UtxoKey};

use std::fmt::{Debug, Display};

/// Amount of blocks to process in parallel
pub const DEF_BLOCK_BATCH: usize = 100;

pub trait SyncUserError: std::error::Error + Display {}

impl<T: std::error::Error + Display> SyncUserError for T {}

#[derive(Error, Debug)]
pub enum UtxoSyncError {
    #[error("Block request failed for height {0} and hash {1}")]
    BlockReq(u32, BlockHash),
    #[error("Block processing error: {0}")]
    UserWith(Box<dyn SyncUserError + Send>),
    #[error("Possible database corruption, cannot get utxo for height {0} and input {1}")]
    CoinWaitTimeout(u32, UtxoKey),
}

/// Future blocks until utxo height == chain height
pub async fn wait_utxo_sync(db: Arc<DB>, dur: Duration) {
    loop {
        let utxo_h = sync_height(&db);
        let chain_h = get_chain_height(&db);
        trace!("Utxo height {utxo_h}, chain height {chain_h}");
        if utxo_h == chain_h {
            break;
        } else {
            sleep(dur).await;
        }
    }
}

/// Future that blocks until current height of utxo is changed
pub async fn wait_utxo_height_changes(db: Arc<DB>, dur: Duration) {
    let start_h = sync_height(&db);
    loop {
        let cur_h = sync_height(&db);
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
    Arc<Mutex<()>>,
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
        |_, _| async move { Ok(()) },
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
    Arc<Mutex<()>>,
    impl Stream<Item = NetworkMessage> + Unpin,
    impl Sink<NetworkMessage, Error = encode::Error>,
)
where
    T: UtxoState + Decodable + Encodable + Clone + Debug + Sync + Send + 'static,
    F: FnMut(u32, &Block) -> U + Clone + Send + 'static,
    U: Future<Output = Result<(), UtxoSyncError>> + Send,
{
    const BUFFER_SIZE: usize = 100;
    let (broad_sender, _) = broadcast::channel(BUFFER_SIZE);
    let (msg_sender, msg_reciver) = mpsc::unbounded_channel::<NetworkMessage>();
    let sync_mutex = Arc::new(Mutex::new(()));
    let sync_future = {
        let broad_sender = broad_sender.clone();
        let sync_mutex = sync_mutex.clone();
        async move {
            println!("Waiting handshake with node");
            wait_handshake(&broad_sender).await;
            let mut last_sync_height = 0;
            loop {
                let utxo_h = utxo_height(&db).max(last_sync_height);
                let chain_h = get_chain_height(&db);
                {
                    // Lock 'syncing' semaphore, so that other threads know not to access utxo cache
                    let _guard = sync_mutex.lock().await;
                    println!("UTXO height {:?}, chain height {:?}", utxo_h, chain_h);
                    if chain_h > utxo_h {
                        let current_utxo_h = Arc::new(AtomicU32::new(utxo_h));
                        while chain_h > current_utxo_h.load(Ordering::Relaxed) {
                            let start_h = current_utxo_h.load(Ordering::Relaxed) + 1;
                            let end_h = start_h + block_batch as u32;
                            stream::iter(start_h..end_h + 1)
                                .map(Ok)
                                .try_for_each_concurrent(block_batch, |h| {
                                    let db = db.clone();
                                    let cache = cache.clone();
                                    let broad_sender = broad_sender.clone();
                                    let msg_sender = msg_sender.clone();
                                    let with = with.clone();
                                    async move {
                                        tokio::spawn({
                                            let cache = cache.clone();
                                            async move {
                                                if h <= chain_h {
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
                                                Ok::<(), UtxoSyncError>(())
                                            }
                                        })
                                        .await
                                        .unwrap()?;
                                        Ok::<(), UtxoSyncError>(())
                                    }
                                })
                                .await?;
                            finish_block(
                                db.clone(),
                                cache.clone(),
                                fork_height,
                                max_coins,
                                flush_period,
                                start_h,
                                end_h,
                                false,
                            )
                            .await;
                            current_utxo_h.store(end_h, Ordering::SeqCst);
                        }
                        println!("UTXO sync finished");
                        last_sync_height = chain_h;
                        let mut batch = WriteBatch::default();
                        set_sync_height(&mut batch, utxo_famiy(&db), last_sync_height);
                        db.write(batch).unwrap();
                    }
                } // sync_mutex unlocked here
                println!("Waiting new height after {}", chain_h);
                chain_height_changes(&db, chain_h, Duration::from_secs(10)).await;
            }
        }
    };
    let msg_stream = UnboundedReceiverStream::new(msg_reciver);
    let msg_sink = sink::unfold(broad_sender, |broad_sender, msg| async move {
        broad_sender.send(msg).unwrap_or(0);
        Ok::<_, encode::Error>(broad_sender)
    });
    (sync_future, sync_mutex, msg_stream, msg_sink)
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
    U: Future<Output = Result<(), UtxoSyncError>>,
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
    with(h, &block).await?;
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
    msg_sender.send(block_msg).map_err(|e| {
        println!("Error when requesting block at height {:?}: {:?}", h, e);
        UtxoSyncError::BlockReq(h, *hash)
    })?;
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
    loop {
        let msg = receiver.recv().await.unwrap();

        if let NetworkMessage::Verack = msg {
            break;
        }
    }
}
