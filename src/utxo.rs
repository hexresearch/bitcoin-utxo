use bitcoin_hashes::{Hash, Error};
use bitcoin::blockdata::block::BlockHeader;
use bitcoin::blockdata::transaction::{Transaction, OutPoint};
use bitcoin::hash_types::Txid;
use byteorder::{ByteOrder, BigEndian};

/// Type of key that we use to address coin in utxo
pub type UtxoKey = OutPoint;

pub fn encode_utxo_key(k: &UtxoKey) -> Vec<u8> {
    let mut buf = [0; 4];
    BigEndian::write_u32(&mut buf, k.vout);
    [k.txid.to_vec(), buf.to_vec()].concat()
}

pub fn decode_utxo_key(bs: Vec<u8>) -> Result<UtxoKey, Error> {
    let txid = Txid::from_hash(Hash::from_slice(&bs[0 .. 32])?);
    let vout = BigEndian::read_u32(&bs[32..36]);
    Ok(OutPoint {
        txid: txid,
        vout: vout,
    })
}

/// Describes user defined state that is stored in UTXO records.
pub trait UtxoState {
    fn new_utxo(height: u32, header: &BlockHeader, tx: &Transaction, vout: u32) -> Self;
}
