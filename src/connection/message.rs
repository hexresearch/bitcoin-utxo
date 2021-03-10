use futures::sink::{unfold, Sink};
use futures::stream::Stream;
use futures::Future;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Create a sink and a stream of messages that allows you to process message and then send another one
/// as response.
pub fn process_messages<'a, E, T, F, U>(
    f: F,
) -> (impl Stream<Item = T> + Unpin, impl Sink<T, Error = E>)
where
    F: FnMut(mpsc::Sender<T>, T) -> U,
    U: Future<Output = mpsc::Sender<T>>,
{
    const BUFFER_SIZE: usize = 100;
    let (msg_sender, msg_reciver) = mpsc::channel::<T>(BUFFER_SIZE);
    let msg_sink = unfold((f, msg_sender), |(mut f, sender), msg: T| async move {
        let sender = f(sender, msg).await;
        Ok::<_, E>((f, sender))
    });
    let msg_stream = ReceiverStream::new(msg_reciver);
    (msg_stream, msg_sink)
}
