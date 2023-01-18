use bitcoin::blockdata::block::BlockHeader;
use bitcoin::blockdata::transaction::{OutPoint, Transaction};
use bitcoin::consensus::{encode, Decodable, Encodable};
use bitcoin::hash_types::Txid;
use bitcoin_hashes::{Error, Hash};
use byteorder::{BigEndian, ByteOrder};

/// Type of key that we use to address coin in utxo
pub type UtxoKey = OutPoint;

pub fn encode_utxo_key(k: &UtxoKey) -> Vec<u8> {
    let mut buf = [0; 4];
    BigEndian::write_u32(&mut buf, k.vout);
    [k.txid.to_vec(), buf.to_vec()].concat()
}

pub fn decode_utxo_key(bs: Vec<u8>) -> Result<UtxoKey, Error> {
    let txid = Txid::from_hash(Hash::from_slice(&bs[0..32])?);
    let vout = BigEndian::read_u32(&bs[32..36]);
    Ok(OutPoint { txid, vout })
}

#[derive(Debug, Clone, Copy)]
pub struct DummyState;

/// Describes user defined state that is stored in UTXO records.
pub trait UtxoState {
    fn new_utxo(height: u32, header: &BlockHeader, tx: &Transaction, vout: u32) -> Self;
}

impl UtxoState for DummyState {
    fn new_utxo(_: u32, _: &BlockHeader, _: &Transaction, _: u32) -> Self {
        DummyState
    }
}

impl Encodable for DummyState {
    fn consensus_encode<W: std::io::Write>(&self, _: W) -> Result<usize, std::io::Error> {
        Ok(0)
    }
}
impl Decodable for DummyState {
    fn consensus_decode<D: std::io::Read>(_: D) -> Result<Self, encode::Error> {
        Ok(DummyState {})
    }
}
