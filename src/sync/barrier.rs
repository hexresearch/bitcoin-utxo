use std::sync::Arc;
use tokio::sync::{Barrier, BarrierWaitResult, Mutex};
use futures::stream::{self, StreamExt};

pub struct FlushBarrier {
    barrier: Barrier,
    finished: Mutex<Vec<u32>>,
    n: usize,
}

impl FlushBarrier {
    pub fn new(n: usize) -> FlushBarrier {
        FlushBarrier {
            barrier: Barrier::new(n),
            finished: Mutex::new(Vec::new()),
            n
        }
    }

    pub async fn wait<F>(fbarrier: Arc<Self>, i:u32, do_flush: F) -> Option<BarrierWaitResult>
        where
        F: FnOnce(u32) -> bool
    {
        let flush_self = {
            let mut v = fbarrier.finished.lock().await;
            v.push(i);
            v.sort();
            let mfinished = FlushBarrier::filled_by(&v[..]);
            if let Some(finished) = mfinished {
                if do_flush(finished) {
                    Some((v[0], finished))
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some((start, finished)) = flush_self {
            FlushBarrier::flush(fbarrier, start, finished).await;
            None
        } else {
            let res = fbarrier.barrier.wait().await;
            if res.is_leader() {
                let mut v = fbarrier.finished.lock().await;
                *v = Vec::new();
            }
            Some(res)
        }
    }

    /// Check if barrier finished by maximum chain of 0 .. i threads where no gaps allowed
    fn filled_by(v: &[u32]) -> Option<u32> {
        let mut cur = None;
        for &i in v.iter() {
            if let Some(prev) = cur {
                if i > prev+1 {
                    break;
                } else {
                    cur = Some(i);
                }
            } else {
                cur = Some(i);
            }
        }
        cur
    }

    /// Fill the barrier with missing threads to reset barrier cycle
    async fn flush(flush_barrier: Arc<Self>, start: u32, filled: u32) {
        println!("Flushing from filled by {:?}, from {:?} to {:?}", filled, filled-start, flush_barrier.n);
        stream::iter(filled-start .. (flush_barrier.n as u32)).for_each_concurrent(flush_barrier.n, |_| {
            let flush_barrier = flush_barrier.clone();
            async move {
                tokio::spawn(async move {
                    flush_barrier.barrier.wait().await
                }).await.unwrap();
            }
        }).await
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::time::Duration;
    use tokio::time::timeout;
    use futures::stream::{self, StreamExt};

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn barrier_simple() {
        let res = async {
            let barrier = Arc::new(FlushBarrier::new(1));
            FlushBarrier::wait(barrier, 0, |_| false).await;
        };
        if let Err(_) = timeout(Duration::from_millis(10), res).await {
            panic!("Timeout for test passed out");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn barrier_even() {
        let res = async {
            let barrier = Arc::new(FlushBarrier::new(10));
            stream::iter(0 .. 100).for_each_concurrent(10, |i| {
                let barrier = barrier.clone();
                async move {
                    tokio::spawn(async move {
                        FlushBarrier::wait(barrier, i, |_| false).await;
                    }).await.unwrap()
                }
            }).await;
        };
        if let Err(_) = timeout(Duration::from_millis(10), res).await {
            panic!("Timeout for test passed out");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn barrier_uneven() {
        let res = async {
            let batch = 10;
            let n = 97;
            let barrier = Arc::new(FlushBarrier::new(batch));
            stream::iter(0 .. n).for_each_concurrent(batch, |i| {
                let barrier = barrier.clone();
                async move {
                    tokio::spawn(async move {
                        FlushBarrier::wait(barrier, i, |filled| filled >= n-1).await;
                    }).await.unwrap()
                }
            }).await;
        };
        if let Err(_) = timeout(Duration::from_millis(100), res).await {
            panic!("Timeout for test passed out");
        }
    }

}
