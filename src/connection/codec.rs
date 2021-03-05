use tokio_util::codec::Decoder;
use tokio_util::codec::Encoder;

use bitcoin::consensus::encode;
use bitcoin::network::constants::Network;
use bitcoin::network::message::{RawNetworkMessage, NetworkMessage};
use bytes::{BufMut, BytesMut};
use std::io;

#[derive(Copy, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MessageCodec {
    network: Network,
}

impl Default for MessageCodec {
    fn default() -> MessageCodec {
        MessageCodec { network: Network::Bitcoin }
    }
}

impl MessageCodec {
    /// Creates a new `MessageCodec` for shipping around raw bytes.
    pub fn new(network: Network) -> MessageCodec {
        MessageCodec { network: network }
    }
}

impl Decoder for MessageCodec {
    type Item = NetworkMessage;
    type Error = encode::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<NetworkMessage>, encode::Error> {
        if !buf.is_empty() {
            match encode::deserialize_partial::<RawNetworkMessage>(buf) {
                Err(encode::Error::Io(ref err)) if err.kind () == io::ErrorKind::UnexpectedEof => {
                    Ok(None)
                }
                Err(err) => return Err(err),
                Ok((message, index)) => {
                    let _ = buf.split_to(index);
                    Ok(Some(message.payload))
                },
            }
        } else {
            Ok(None)
        }
    }
}

impl Encoder<NetworkMessage> for MessageCodec {
    type Error = encode::Error;

    fn encode(&mut self, data: NetworkMessage, buf: &mut BytesMut) -> Result<(), encode::Error> {
        let rawmsg = RawNetworkMessage {
            magic: self.network.magic(),
            payload: data,
        };
        let bytes = encode::serialize(&rawmsg);
        buf.reserve(bytes.len());
        buf.put(bytes.as_slice());
        Ok(())
    }
}
