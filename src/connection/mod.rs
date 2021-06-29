pub mod codec;
pub mod message;

use futures::future::{AbortHandle, Abortable, Aborted};
use futures::pin_mut;
use futures::stream;
use futures::{future, Sink, SinkExt, Stream, StreamExt};
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    error::Error,
    net::{IpAddr, Ipv4Addr, SocketAddr},
};
use tokio::net::TcpStream;
use tokio_util::codec::{FramedRead, FramedWrite};

use bitcoin::consensus::encode;
use bitcoin::network::address;
use bitcoin::network::constants;
use bitcoin::network::constants::Network;
use bitcoin::network::message::NetworkMessage;
use bitcoin::network::message_network::VersionMessage;
use bitcoin::secp256k1;
use bitcoin::secp256k1::rand::Rng;

use crate::connection::codec::MessageCodec;
use crate::connection::message::process_messages;

pub async fn connect(
    addr: &SocketAddr,
    network: Network,
    user_agent: String,
    start_height: i32,
    inmsgs: impl Stream<Item = NetworkMessage> + Unpin,
    outmsgs: impl Sink<NetworkMessage, Error = encode::Error> + Unpin,
) -> Result<(), Box<dyn Error>> {
    let handshake_stream =
        stream::once(async { build_version_message(addr, user_agent, start_height) });
    pin_mut!(handshake_stream);

    let (_, verack_stream, verack_sink) = process_messages(|sender, msg| async move {
        match msg {
            NetworkMessage::Version(_) => {
                println!("Received version message: {:?}", msg);
                println!("Sent verack message");
                sender.send(NetworkMessage::Verack).await.unwrap();
            }
            NetworkMessage::Verack => {
                println!("Received verack message: {:?}", msg);
            },
            NetworkMessage::Ping(u) => {
                sender.send(NetworkMessage::Pong(u)).await.unwrap();
            }
            _ => (),
        };
        sender
    });
    pin_mut!(verack_sink);

    let internal_inmsgs = stream::select(stream::select(inmsgs, handshake_stream), verack_stream);

    raw_connect(addr, network, internal_inmsgs, outmsgs.fanout(verack_sink)).await
}

/// Connect to node and parse and serialize messages. Doesn't perform handshake.
pub async fn raw_connect(
    addr: &SocketAddr,
    network: Network,
    inmsgs: impl Stream<Item = NetworkMessage> + Unpin,
    mut outmsgs: impl Sink<NetworkMessage, Error = encode::Error> + Unpin,
) -> Result<(), Box<dyn Error>> {
    let (abort_handle, abort_registration) = AbortHandle::new_pair();
    let mut stream = TcpStream::connect(addr).await?;
    let (r, w) = stream.split();
    let mut sink = FramedWrite::new(w, MessageCodec::new(network));
    let mut stream = FramedRead::new(r, MessageCodec::new(network)).filter_map(|i| match i {
        Ok(i) => future::ready(Some(Ok(i))),
        Err(e) => {
            println!("Failed to read from socket; error={}", e);
            abort_handle.abort();
            future::ready(Some(Err(e)))
        }
    });

    let mut inmsgs_err = inmsgs.map(Ok);
    match Abortable::new(
        future::join(
            sink.send_all(&mut inmsgs_err),
            outmsgs.send_all(&mut stream),
        ),
        abort_registration,
    )
    .await
    {
        Err(Aborted) => Err(encode::Error::Io(io::Error::new(
            io::ErrorKind::Other,
            "Connection closed!",
        ))
        .into()),
        Ok(res) => match res {
            (Err(e), _) | (_, Err(e)) => Err(e.into()),
            _ => Ok(()),
        },
    }
}

fn build_version_message(
    address: &SocketAddr,
    user_agent: String,
    start_height: i32,
) -> NetworkMessage {
    // Building version message, see https://en.bitcoin.it/wiki/Protocol_documentation#version
    let my_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), 0);

    // "bitfield of features to be enabled for this connection"
    let services = constants::ServiceFlags::NONE;

    // "standard UNIX timestamp in seconds"
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time error")
        .as_secs();

    // "The network address of the node receiving this message"
    let addr_recv = address::Address::new(address, constants::ServiceFlags::NONE);

    // "The network address of the node emitting this message"
    let addr_from = address::Address::new(&my_address, constants::ServiceFlags::NONE);

    // "Node random nonce, randomly generated every time a version packet is sent. This nonce is used to detect connections to self."
    let nonce: u64 = secp256k1::rand::thread_rng().gen();

    // Construct the message
    NetworkMessage::Version(VersionMessage::new(
        services,
        timestamp as i64,
        addr_recv,
        addr_from,
        nonce,
        user_agent,
        start_height,
    ))
}
