pub mod codec;

use futures::{future, Sink, SinkExt, Stream, StreamExt};
use std::{error::Error, net::SocketAddr};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use bitcoin::network::constants::Network;
use bitcoin::network::message::NetworkMessage;
use bitcoin::consensus::encode;

use crate::connection::codec::MessageCodec;

pub async fn connect(
    addr: &SocketAddr,
    network: Network,
    mut inmsgs: impl Stream<Item = Result<NetworkMessage, encode::Error>> + Unpin,
    mut outmsgs: impl Sink<NetworkMessage, Error = encode::Error> + Unpin,
) -> Result<(), Box<dyn Error>> {
    let mut stream = TcpStream::connect(addr).await?;
    let (r, w) = stream.split();
    let mut sink = FramedWrite::new(w, MessageCodec::new(network));
    // filter map Result<BytesMut, Error> stream into just a Bytes stream to match outmsgs Sink
    // on the event of an Error, log the error and end the stream
    let mut stream = FramedRead::new(r, MessageCodec::new(network))
        .filter_map(|i| match i {
            //BytesMut into Bytes
            Ok(i) => future::ready(Some(i)),
            Err(e) => {
                println!("failed to read from socket; error={}", e);
                future::ready(None)
            }
        })
        .map(Ok);

    match future::join(sink.send_all(&mut inmsgs), outmsgs.send_all(&mut stream)).await {
        (Err(e), _) | (_, Err(e)) => Err(e.into()),
        _ => Ok(()),
    }
}
