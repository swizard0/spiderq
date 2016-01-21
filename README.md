# spiderq server

## Summary

Fast standalone priority queue / KV database with simple zeromq-based [binary protocol](https://github.com/swizard0/spiderq-proto).

It is supposed to be a part of some schedule related software (web crawlers, monitoring, pollers, status checkers, etc).

Initially has been develloped for [Kribrum](http://www.kribrum.ru) project.

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

## Design.

`Spiderq` creates a socket of `ZMQ_ROUTER` type for external communication and binds it to the address given in `--zmq-addr` command line parameter. So clients should connect it using `ZMQ_REQ` (usually), `ZMQ_DEALER` or `ZMQ_ROUTER` sockets.

Protocol uses simple request-reply schema (synchronous). Both requests and replies are encoded according to [specifications](https://github.com/swizard0/spiderq-proto#specifications) in last frame of zeromq message. `Spiderq` server can handle any number of frames in message: all of them except the last one will be returned to the client back, and the last frame will be the actual server reply. This allows a client to pass arbitrary information with requests, for example, cookies, authentication, state or some internal routing data (see [espiderq](https://github.com/swizard0/espiderq) implementation).

