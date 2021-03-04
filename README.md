# bitcoin-utxo

The small library that builds Bitcoin UTXO set from scratch by connecting to
Bitcoin node over P2P protocol and parsing transaction from the genesis block.

# Examples

Demo to calculate average days unspent:
```
cargo run --example days -- 89.245.85.72:8333
```
You can use any public bitcoin node.
