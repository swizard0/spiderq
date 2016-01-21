# spiderq daemon

## Summary

Standalone priority queue with simple zeromq-based [binary protocol](https://github.com/swizard0/spiderq-proto).

## Building spiderq

### Get spiderq

```
% git clone https://github.com/swizard0/spiderq.git
% cd spiderq
% git submodule init
% git submodule update
```

### Compile binary

Requirements:

* Stable [rust](https://www.rust-lang.org/downloads.html) compiler.
* [cargo](https://crates.io/install) tool.
* [ZeroMQ](http://zeromq.org/intro:get-the-software) >= 4.0

```
% cargo build --release
```

After successfull building the binary should be in `target/release/spiderq` directory.

## Running spiderq

### Command line parameters

```
Usage: target/release/spiderq

Options:
    -d, --database      database directory path (optional, default: ./spiderq)
    -l, --flush-limit   database disk sync threshold (items modified before
                        flush) (optional, default: 131072)
    -z, --zmq-addr      zeromq interface listen address (optional, default:
                        ipc://./spiderq.ipc)
```

## Existing protocol implementations

* [spiderq-proto](https://github.com/swizard0/spiderq-proto): Rust crate
* [espiderq](https://github.com/swizard0/espiderq): Erlang module


