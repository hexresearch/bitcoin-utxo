# bitcoin-utxo

The small library that builds Bitcoin UTXO set from scratch by connecting to
Bitcoin node over P2P protocol and parsing transaction from the genesis block.

# Examples

Demo to calculate average days unspent:
```
cargo run --example days -- 89.245.85.72:8333
```
You can use any public bitcoin node.

Demo that calculates BIP158 filters, expect heavy load on system and I recommend using local node for speed:
```
cargo run --example filters -- 127.0.0.1:8333
```
