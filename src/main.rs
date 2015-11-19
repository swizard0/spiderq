#![feature(drain)]

extern crate zmq;
extern crate time;
extern crate getopts;
extern crate tempdir;
extern crate byteorder;
extern crate spiderq_proto as proto;
#[cfg(test)] extern crate rand;

use std::{io, env, mem, process};
use std::io::Write;
use std::convert::From;
use std::thread::{Builder, JoinHandle};
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use std::num::ParseIntError;
use getopts::Options;
use time::{SteadyTime, Duration};

pub mod db;
pub mod pq;
use proto::{Key, Value, ProtoError, GlobalReq, GlobalRep};

const MAX_POLL_TIMEOUT: i64 = 100;

#[derive(Debug)]
pub enum Error {
    Getopts(getopts::Fail),
    Db(db::Error),
    Zmq(ZmqError),
    InvalidFlushLimit(ParseIntError),
}

#[derive(Debug)]
pub enum ZmqError {
    Message(zmq::Error),
    Socket(zmq::Error),
    Connect(zmq::Error),
    Bind(zmq::Error),
    Recv(zmq::Error),
    Send(zmq::Error),
    Poll(zmq::Error),
    GetSockOpt(zmq::Error),
}

impl From<db::Error> for Error {
    fn from(err: db::Error) -> Error {
        Error::Db(err)
    }
}

pub fn bootstrap(maybe_matches: getopts::Result) -> Result<(zmq::Context, JoinHandle<()>), Error> {
    let matches = try!(maybe_matches.map_err(|e| Error::Getopts(e)));
    let database_dir = matches.opt_str("database").unwrap_or("./spiderq".to_owned());
    let zmq_addr = matches.opt_str("zmq-addr").unwrap_or("ipc://./spiderq.ipc".to_owned());
    let flush_limit: usize = try!(matches.opt_str("flush-limit").unwrap_or("131072".to_owned()).parse().map_err(|e| Error::InvalidFlushLimit(e)));
    entrypoint(&zmq_addr, &database_dir, flush_limit)
}

pub fn entrypoint(zmq_addr: &str, database_dir: &str, flush_limit: usize) -> Result<(zmq::Context, JoinHandle<()>), Error> {
    let db = try!(db::Database::new(database_dir, flush_limit).map_err(|e| Error::Db(e)));
    let mut pq = pq::PQueue::new();
    // initially fill pq
    for (k, _) in db.iter() {
        pq.add(k.clone())
    }

    let mut ctx = zmq::Context::new();
    let mut sock_master_ext = try!(ctx.socket(zmq::ROUTER).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_master_db_rx = try!(ctx.socket(zmq::PULL).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_master_pq_rx = try!(ctx.socket(zmq::PULL).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_db_master_tx = try!(ctx.socket(zmq::PUSH).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_pq_master_tx = try!(ctx.socket(zmq::PUSH).map_err(|e| Error::Zmq(ZmqError::Socket(e))));

    try!(sock_master_ext.bind(zmq_addr).map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_master_db_rx.bind("inproc://db_rxtx").map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_db_master_tx.connect("inproc://db_rxtx").map_err(|e| Error::Zmq(ZmqError::Connect(e))));
    try!(sock_master_pq_rx.bind("inproc://pq_rxtx").map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_pq_master_tx.connect("inproc://pq_rxtx").map_err(|e| Error::Zmq(ZmqError::Connect(e))));

    let (chan_master_db_tx, chan_db_master_rx) = channel();
    let (chan_db_master_tx, chan_master_db_rx) = channel();
    let (chan_master_pq_tx, chan_pq_master_rx) = channel();
    let (chan_pq_master_tx, chan_master_pq_rx) = channel();

    Builder::new().name("worker_db".to_owned())
        .spawn(move || worker_db(sock_db_master_tx, chan_db_master_tx, chan_db_master_rx, db).unwrap()).unwrap();
    Builder::new().name("worker_pq".to_owned())
        .spawn(move || worker_pq(sock_pq_master_tx, chan_pq_master_tx, chan_pq_master_rx, pq).unwrap()).unwrap();
    let master_thread = Builder::new().name("master".to_owned())
   	.spawn(move || master(sock_master_ext,
                              sock_master_db_rx,
                              sock_master_pq_rx,
                              chan_master_db_tx,
                              chan_master_db_rx,
                              chan_master_pq_tx,
                              chan_master_pq_rx).unwrap())
        .unwrap();
    Ok((ctx, master_thread))
}

#[derive(Debug, PartialEq)]
pub enum DbLocalReq {
    LoadLent(u64, Key),
    RepayUpdate(Key, Value),
    Stop,
}

#[derive(Debug, PartialEq)]
pub enum PqLocalReq {
    NextTrigger,
    Enqueue(Key),
    LendUntil(u64, SteadyTime),
    RepayTimedOut,
    Heartbeat(u64, Key, SteadyTime),
    Stop,
}

#[derive(Debug, PartialEq)]
pub enum DbLocalRep {
    Added(Key),
    Stopped,
}

#[derive(Debug, PartialEq)]
pub enum PqLocalRep {
    TriggerGot(Option<SteadyTime>),
    Lent(u64, Key),
    EmptyQueueHit { timeout: u64, },
    Repaid(Key, Value),
    Stopped,
}

pub type Headers = Vec<zmq::Message>;

#[derive(Debug, PartialEq)]
pub enum DbReq {
    Global(GlobalReq),
    Local(DbLocalReq),
}

#[derive(Debug, PartialEq)]
pub enum PqReq {
    Global(GlobalReq),
    Local(PqLocalReq),
}

#[derive(Debug, PartialEq)]
pub enum DbRep {
    Global(GlobalRep),
    Local(DbLocalRep),
}

#[derive(Debug, PartialEq)]
pub enum PqRep {
    Global(GlobalRep),
    Local(PqLocalRep),
}

pub struct Message<R> {
    pub headers: Option<Headers>,
    pub load: R,
}

fn rx_sock(sock: &mut zmq::Socket) -> Result<(Option<Headers>, Result<GlobalReq, ProtoError>), Error> {
    let mut frames = Vec::new();
    loop {
        frames.push(try!(sock.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e)))));
        if !try!(sock.get_rcvmore().map_err(|e| Error::Zmq(ZmqError::GetSockOpt(e)))) {
            break
        }
    }

    let load_msg = frames.pop().unwrap();
    Ok((Some(frames), GlobalReq::decode(&load_msg).map(|p| p.0)))
}

fn tx_sock(packet: GlobalRep, maybe_headers: Option<Headers>, sock: &mut zmq::Socket) -> Result<(), Error> {
    let required = packet.encode_len();
    let mut load_msg = try!(zmq::Message::with_capacity(required).map_err(|e| Error::Zmq(ZmqError::Message(e))));
    packet.encode(&mut load_msg);

    if let Some(headers) = maybe_headers {
        for header in headers {
            try!(sock.send_msg(header, zmq::SNDMORE).map_err(|e| Error::Zmq(ZmqError::Send(e))));
        }
    }
    sock.send_msg(load_msg, 0).map_err(|e| Error::Zmq(ZmqError::Send(e)))
}

pub fn tx_chan<R>(packet: R, maybe_headers: Option<Headers>, chan: &Sender<Message<R>>) {
    chan.send(Message { headers: maybe_headers, load: packet, }).unwrap()
}

fn notify_sock(sock: &mut zmq::Socket) -> Result<(), Error> {
    sock.send_msg(try!(zmq::Message::new().map_err(|e| Error::Zmq(ZmqError::Message(e)))), 0)
        .map_err(|e| Error::Zmq(ZmqError::Send(e)))
}

fn tx_chan_n<R>(packet: R, maybe_headers: Option<Headers>, chan: &Sender<Message<R>>, sock: &mut zmq::Socket) -> Result<(), Error> {
    tx_chan(packet, maybe_headers, chan);
    notify_sock(sock)
}


pub fn worker_db(mut sock_tx: zmq::Socket, 
                 chan_tx: Sender<Message<DbRep>>,
                 chan_rx: Receiver<Message<DbReq>>,
                 mut db: db::Database) -> Result<(), Error>
{
    try!(notify_sock(&mut sock_tx));
    loop {
        let req = chan_rx.recv().unwrap();
        match req.load {
            DbReq::Global(GlobalReq::Add(key, value)) => {
                if db.lookup(&key).is_some() {
                    try!(tx_chan_n(DbRep::Global(GlobalRep::Kept), req.headers, &chan_tx, &mut sock_tx))
                } else {
                    db.insert(key.clone(), value);
                    try!(tx_chan_n(DbRep::Local(DbLocalRep::Added(key)), req.headers, &chan_tx, &mut sock_tx))
                }
            },
            DbReq::Global(GlobalReq::Update(key, value)) => {
                if db.lookup(&key).is_some() {
                    db.insert(key.clone(), value);
                    try!(tx_chan_n(DbRep::Global(GlobalRep::Updated), req.headers, &chan_tx, &mut sock_tx))
                } else {
                    try!(tx_chan_n(DbRep::Global(GlobalRep::NotFound), req.headers, &chan_tx, &mut sock_tx))
                }
            },
            DbReq::Global(GlobalReq::Lookup(key)) => {
                if let Some(value) = db.lookup(&key) {
                    try!(tx_chan_n(DbRep::Global(GlobalRep::ValueFound(value.clone())), req.headers, &chan_tx, &mut sock_tx))
                } else {
                    try!(tx_chan_n(DbRep::Global(GlobalRep::ValueNotFound), req.headers, &chan_tx, &mut sock_tx))
                }
            },
            DbReq::Global(GlobalReq::Flush) => {
                db.flush();
                try!(tx_chan_n(DbRep::Global(GlobalRep::Flushed), req.headers, &chan_tx, &mut sock_tx));
            },
            DbReq::Global(..) =>
                unreachable!(),
            DbReq::Local(DbLocalReq::LoadLent(lend_key, key)) =>
                if let Some(value) = db.lookup(&key) {
                    try!(tx_chan_n(DbRep::Global(GlobalRep::Lent { lend_key: lend_key, key: key, value: value.clone(), }),
                                   req.headers, &chan_tx, &mut sock_tx))
                } else {
                    try!(tx_chan_n(DbRep::Global(GlobalRep::Error(ProtoError::DbQueueOutOfSync(key.clone()))), req.headers, &chan_tx, &mut sock_tx))
                },
            DbReq::Local(DbLocalReq::RepayUpdate(key, value)) => {
                db.insert(key, value);
                try!(tx_chan_n(DbRep::Global(GlobalRep::Repaid), req.headers, &chan_tx, &mut sock_tx))
            },
            DbReq::Local(DbLocalReq::Stop) => {
                try!(tx_chan_n(DbRep::Local(DbLocalRep::Stopped), req.headers, &chan_tx, &mut sock_tx));
                return Ok(())
            },
        }
    }
}

pub fn worker_pq(mut sock_tx: zmq::Socket, 
                 chan_tx: Sender<Message<PqRep>>,
                 chan_rx: Receiver<Message<PqReq>>,
                 mut pq: pq::PQueue) -> Result<(), Error> 
{
    try!(notify_sock(&mut sock_tx));
    loop {
        let req = chan_rx.recv().unwrap();
        match req.load {
            PqReq::Global(GlobalReq::Count) =>
                try!(tx_chan_n(PqRep::Global(GlobalRep::Counted(pq.len())), req.headers, &chan_tx, &mut sock_tx)),
            PqReq::Global(GlobalReq::Repay { lend_key: rlend_key, key: rkey, value: rvalue, status: rstatus, }) =>
                if pq.repay(rlend_key, rkey.clone(), rstatus) {
                    try!(tx_chan_n(PqRep::Local(PqLocalRep::Repaid(rkey, rvalue)), req.headers, &chan_tx, &mut sock_tx))
                } else {
                    try!(tx_chan_n(PqRep::Global(GlobalRep::NotFound), req.headers, &chan_tx, &mut sock_tx))
                },
            PqReq::Global(..) =>
                unreachable!(),
            PqReq::Local(PqLocalReq::Enqueue(key)) =>
                pq.add(key),
            PqReq::Local(PqLocalReq::LendUntil(timeout, trigger_at)) =>
                if let Some((key, serial)) = pq.top() {
                    try!(tx_chan_n(PqRep::Local(PqLocalRep::Lent(serial, key)), req.headers, &chan_tx, &mut sock_tx));
                    pq.lend(trigger_at);
                } else {
                    try!(tx_chan_n(PqRep::Local(PqLocalRep::EmptyQueueHit { timeout: timeout }), req.headers, &chan_tx, &mut sock_tx))
                },
            PqReq::Local(PqLocalReq::Heartbeat(lend_key, ref key, trigger_at)) => {
                if pq.heartbeat(lend_key, key, trigger_at) {
                    try!(tx_chan_n(PqRep::Global(GlobalRep::Heartbeaten), req.headers, &chan_tx, &mut sock_tx))
                } else {
                    try!(tx_chan_n(PqRep::Global(GlobalRep::Skipped), req.headers, &chan_tx, &mut sock_tx))
                }
            },
            PqReq::Local(PqLocalReq::NextTrigger) =>
                try!(tx_chan_n(PqRep::Local(PqLocalRep::TriggerGot(pq.next_timeout())), req.headers, &chan_tx, &mut sock_tx)),
            PqReq::Local(PqLocalReq::RepayTimedOut) =>
                pq.repay_timed_out(),
            PqReq::Local(PqLocalReq::Stop) => {
                try!(tx_chan_n(PqRep::Local(PqLocalRep::Stopped), req.headers, &chan_tx, &mut sock_tx));
                return Ok(())
            },
        }
    }
}

fn master(mut sock_ext: zmq::Socket,
          mut sock_db_rx: zmq::Socket,
          mut sock_pq_rx: zmq::Socket,
          chan_db_tx: Sender<Message<DbReq>>,
          chan_db_rx: Receiver<Message<DbRep>>,
          chan_pq_tx: Sender<Message<PqReq>>,
          chan_pq_rx: Receiver<Message<PqRep>>) -> Result<(), Error>
{
    enum StopState {
        NotTriggered,
        WaitingPqAndDb(Option<Headers>),
        WaitingPq(Option<Headers>),
        WaitingDb(Option<Headers>),
        Finished(Option<Headers>),
    }

    let mut stats_count = 0;
    let mut stats_add = 0;
    let mut stats_update = 0;
    let mut stats_lookup = 0;
    let mut stats_lend = 0;
    let mut stats_repay = 0;
    let mut stats_heartbeat = 0;
    let mut stats_stats = 0;

    let (mut incoming_queue, mut pending_queue) = (Vec::new(), Vec::new());
    let mut next_timeout: Option<Option<SteadyTime>> = None;
    let mut stop_state = StopState::NotTriggered;

    // sync with workers
    let _ = try!(sock_db_rx.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e))));
    let _ = try!(sock_pq_rx.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e))));

    loop {
        // check if it is time to quit
        match stop_state {
            StopState::Finished(headers) => {
                try!(tx_sock(GlobalRep::Terminated, headers, &mut sock_ext));
                return Ok(())
            },
            _ => (),
        }

        let before_poll_ts = SteadyTime::now();

        // calculate poll delay
        let timeout = match next_timeout {
            None => {
                tx_chan(PqReq::Local(PqLocalReq::NextTrigger), None, &chan_pq_tx);
                MAX_POLL_TIMEOUT
            },
            Some(None) =>
                MAX_POLL_TIMEOUT,
            Some(Some(next_trigger)) => {
                let interval = next_trigger - before_poll_ts;
                match interval.num_milliseconds() {
                    timeout if timeout <= 0 => {
                        next_timeout = None;
                        tx_chan(PqReq::Local(PqLocalReq::RepayTimedOut), None, &chan_pq_tx);
                        continue;
                    },
                    timeout if timeout > MAX_POLL_TIMEOUT =>
                        MAX_POLL_TIMEOUT,
                    timeout =>
                        timeout,
                }
            },
        };

        let avail_socks = {
            let mut pollitems = [sock_ext.as_poll_item(zmq::POLLIN),
                                 sock_db_rx.as_poll_item(zmq::POLLIN),
                                 sock_pq_rx.as_poll_item(zmq::POLLIN)];
            try!(zmq::poll(&mut pollitems, timeout).map_err(|e| Error::Zmq(ZmqError::Poll(e))));
            [pollitems[0].get_revents() == zmq::POLLIN,
             pollitems[1].get_revents() == zmq::POLLIN,
             pollitems[2].get_revents() == zmq::POLLIN]
        };

        let after_poll_ts = SteadyTime::now();
        if avail_socks[0] {
            // sock_ext is online
            match rx_sock(&mut sock_ext) {
                Ok((headers, Ok(req))) => incoming_queue.push((headers, req)),
                Ok((headers, Err(e))) => try!(tx_sock(GlobalRep::Error(e), headers, &mut sock_ext)),
                Err(e) => return Err(e),
            }
        }

        if avail_socks[1] {
            // sock_db is online, skip ping msg
            let _ = try!(sock_db_rx.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e))));
        }

        if avail_socks[2] {
            // sock_pq is online, skip ping msg
            let _ = try!(sock_pq_rx.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e))));
        }

        loop {
            // process messages from db
            match stop_state {
                StopState::Finished(..) | StopState::WaitingPq(..) => break,
                _ => (),
            }

            match chan_db_rx.try_recv() {
                Ok(message) => match message.load {
                    DbRep::Global(rep @ GlobalRep::Kept) => {
                        stats_add += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    DbRep::Global(rep @ GlobalRep::Updated) => {
                        stats_update += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    DbRep::Global(rep @ GlobalRep::NotFound) => {
                        stats_update += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    DbRep::Global(rep @ GlobalRep::ValueFound(..)) => {
                        stats_lookup += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    DbRep::Global(rep @ GlobalRep::ValueNotFound) => {
                        stats_lookup += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    DbRep::Global(rep @ GlobalRep::Lent { .. }) => {
                        stats_lend += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    DbRep::Global(rep @ GlobalRep::Repaid) => {
                        stats_repay += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    DbRep::Global(rep @ GlobalRep::Flushed) =>
                        try!(tx_sock(rep, message.headers, &mut sock_ext)),
                    DbRep::Global(rep @ GlobalRep::Error(..)) =>
                        try!(tx_sock(rep, message.headers, &mut sock_ext)),
                    DbRep::Global(GlobalRep::Counted(..)) |
                    DbRep::Global(GlobalRep::Added) |
                    DbRep::Global(GlobalRep::Heartbeaten) |
                    DbRep::Global(GlobalRep::Skipped) |
                    DbRep::Global(GlobalRep::StatsGot { .. }) |
                    DbRep::Global(GlobalRep::Terminated) =>
                        unreachable!(),
                    DbRep::Local(DbLocalRep::Added(key)) => {
                        tx_chan(PqReq::Local(PqLocalReq::Enqueue(key)), None, &chan_pq_tx);
                        stats_add += 1;
                        try!(tx_sock(GlobalRep::Added, message.headers, &mut sock_ext));
                    },
                    DbRep::Local(DbLocalRep::Stopped) =>
                        stop_state = match stop_state {
                            StopState::NotTriggered | StopState::WaitingPq(..) | StopState::Finished(..) => unreachable!(),
                            StopState::WaitingPqAndDb(headers) => StopState::WaitingPq(headers),
                            StopState::WaitingDb(headers) => StopState::Finished(headers),
                        },
                },
                Err(TryRecvError::Empty) =>
                    break,
                Err(TryRecvError::Disconnected) =>
                    panic!("db worker thread is down"),
            }
        }

        loop {
            // process messages from pq
            match stop_state {
                StopState::Finished(..) | StopState::WaitingDb(..) => break,
                _ => (),
            }

            match chan_pq_rx.try_recv() {
                Ok(message) => match message.load {
                    PqRep::Global(rep @ GlobalRep::Counted(..)) => {
                        stats_count += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    PqRep::Global(rep @ GlobalRep::Heartbeaten) => {
                        stats_heartbeat += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                        next_timeout = None;
                    },
                    PqRep::Global(rep @ GlobalRep::Skipped) => {
                        stats_heartbeat += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    PqRep::Global(rep @ GlobalRep::NotFound) => {
                        stats_repay += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    PqRep::Global(GlobalRep::Added) |
                    PqRep::Global(GlobalRep::Kept) |
                    PqRep::Global(GlobalRep::Updated) |
                    PqRep::Global(GlobalRep::ValueFound(..)) |
                    PqRep::Global(GlobalRep::ValueNotFound) |
                    PqRep::Global(GlobalRep::Lent { .. }) |
                    PqRep::Global(GlobalRep::Repaid) |
                    PqRep::Global(GlobalRep::StatsGot { .. }) |
                    PqRep::Global(GlobalRep::Flushed) |
                    PqRep::Global(GlobalRep::Terminated) |
                    PqRep::Global(GlobalRep::Error(..)) =>
                        unreachable!(),
                    PqRep::Local(PqLocalRep::Lent(lend_key, key)) => {
                        tx_chan(DbReq::Local(DbLocalReq::LoadLent(lend_key, key)), message.headers, &chan_db_tx);
                        next_timeout = None;
                    },
                    PqRep::Local(PqLocalRep::EmptyQueueHit { timeout: t, }) =>
                        pending_queue.push((message.headers, GlobalReq::Lend { timeout: t, })),
                    PqRep::Local(PqLocalRep::Repaid(key, value)) => {
                        tx_chan(DbReq::Local(DbLocalReq::RepayUpdate(key, value)), message.headers, &chan_db_tx);
                        next_timeout = None;
                    }
                    PqRep::Local(PqLocalRep::TriggerGot(trigger_at)) =>
                        next_timeout = Some(trigger_at),
                    PqRep::Local(PqLocalRep::Stopped) =>
                        stop_state = match stop_state {
                            StopState::NotTriggered | StopState::WaitingDb(..) | StopState::Finished(..) => unreachable!(),
                            StopState::WaitingPqAndDb(headers) => StopState::WaitingDb(headers),
                            StopState::WaitingPq(headers) => StopState::Finished(headers),
                        },
                },
                Err(TryRecvError::Empty) =>
                    break,
                Err(TryRecvError::Disconnected) =>
                    panic!("pq worker thread is down"),
            }
        }

        // process incoming messages
        for (headers, global_req) in incoming_queue.drain(..) {
            match global_req {
                req @ GlobalReq::Count =>
                    tx_chan(PqReq::Global(req), headers, &chan_pq_tx),
                req @ GlobalReq::Add(..) =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Update(..) =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Lookup(..) =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Repay { .. } =>
                    tx_chan(PqReq::Global(req), headers, &chan_pq_tx),
                req @ GlobalReq::Flush =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                GlobalReq::Lend { timeout: t, } => {
                    let trigger_at = after_poll_ts + Duration::milliseconds(t as i64);
                    tx_chan(PqReq::Local(PqLocalReq::LendUntil(t, trigger_at)), headers, &chan_pq_tx);
                },
                GlobalReq::Heartbeat { lend_key: l, key: k, timeout: t, } => {
                    let trigger_at = after_poll_ts + Duration::milliseconds(t as i64);
                    tx_chan(PqReq::Local(PqLocalReq::Heartbeat(l, k, trigger_at)), headers, &chan_pq_tx);
                },
                GlobalReq::Stats => {
                    stats_stats += 1;
                    try!(tx_sock(GlobalRep::StatsGot { count: stats_count,
                                                       add: stats_add,
                                                       update: stats_update,
                                                       lookup: stats_lookup,
                                                       lend: stats_lend,
                                                       repay: stats_repay,
                                                       heartbeat: stats_heartbeat,
                                                       stats: stats_stats, }, headers, &mut sock_ext));
                },
                GlobalReq::Terminate => {
                    tx_chan(PqReq::Local(PqLocalReq::Stop), None, &chan_pq_tx);
                    tx_chan(DbReq::Local(DbLocalReq::Stop), None, &chan_db_tx);
                    stop_state = StopState::WaitingPqAndDb(headers);
                },
            }
        }

        mem::swap(&mut incoming_queue, &mut pending_queue);
    }
}

fn main() {
    let mut args = env::args();
    let cmd_proc = args.next().unwrap();
    let mut opts = Options::new();

    opts.optopt("d", "database", "database directory path (optional, default: ./spiderq)", "");
    opts.optopt("l", "flush-limit", "database disk sync threshold (items modified before flush) (optional, default: 131072)", "");
    opts.optopt("z", "zmq-addr", "zeromq interface listen address (optional, default: ipc://./spiderq.ipc)", "");

    match bootstrap(opts.parse(args)) {
        Ok((_ctx, master_thread)) =>
            master_thread.join().unwrap(),
        Err(cause) => {
            let _ = writeln!(&mut io::stderr(), "Error: {:?}", cause);
            let usage = format!("Usage: {}", cmd_proc);
            let _ = writeln!(&mut io::stderr(), "{}", opts.usage(&usage[..]));
            process::exit(1);
        }
    };
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::sync::Arc;
    use std::fmt::Debug;
    use std::thread::spawn;
    use time::{SteadyTime, Duration};
    use rand::{thread_rng, sample, Rng};
    use std::sync::mpsc::{channel, Sender, Receiver};
    use super::proto::{Key, Value, RepayStatus, GlobalReq, GlobalRep, ProtoError};
    use super::{zmq, db, pq, worker_db, worker_pq, tx_chan, entrypoint};
    use super::{Message, DbReq, DbRep, PqReq, PqRep, DbLocalReq, DbLocalRep, PqLocalReq, PqLocalRep};

    fn with_worker<WF, MF, Req, Rep>(base_addr: &str, worker_fn: WF, master_fn: MF) where
        WF: FnOnce(zmq::Socket, Sender<Message<Rep>>, Receiver<Message<Req>>) + Send + 'static,
        MF: FnOnce(zmq::Socket, Sender<Message<Req>>, Receiver<Message<Rep>>) + Send + 'static,
        Req: Send + 'static, Rep: Send + 'static
    {
        let mut ctx = zmq::Context::new();
        let mut sock_master_slave_rx = ctx.socket(zmq::PULL).unwrap();
        let mut sock_slave_master_tx = ctx.socket(zmq::PUSH).unwrap();

        let rx_addr = format!("inproc://{}_rxtx", base_addr);
        sock_master_slave_rx.bind(&rx_addr).unwrap();
        sock_slave_master_tx.connect(&rx_addr).unwrap();

        let (chan_master_slave_tx, chan_slave_master_rx) = channel();
        let (chan_slave_master_tx, chan_master_slave_rx) = channel();

        let worker = spawn(move || worker_fn(sock_slave_master_tx, chan_slave_master_tx, chan_slave_master_rx));
        assert_eq!(sock_master_slave_rx.recv_bytes(0).unwrap(), &[]); // sync
        master_fn(sock_master_slave_rx, chan_master_slave_tx, chan_master_slave_rx);
        worker.join().unwrap();
    }

    fn assert_worker_cmd<Req, Rep>(sock: &mut zmq::Socket, tx: &Sender<Message<Req>>, rx: &Receiver<Message<Rep>>, req: Req, rep: Rep) 
        where Rep: PartialEq + Debug
    {
        tx_chan(req, None, tx);
        assert_eq!(sock.recv_bytes(0).unwrap(), &[]);
        assert_eq!(rx.recv().unwrap().load, rep);
    }

    fn rnd_kv() -> (Key, Value) {
        let mut rng = thread_rng();
        let key_len = rng.gen_range(1, 64);
        let value_len = rng.gen_range(1, 64);
        (Arc::new(sample(&mut rng, 0 .. 255, key_len)),
         Arc::new(sample(&mut rng, 0 .. 255, value_len)))
    }

    #[test]
    fn db_worker() {
        let path = "/tmp/spiderq_main";
        let _ = fs::remove_dir_all(path);
        let db = db::Database::new(path, 16).unwrap();
        with_worker(
            "db",
            move |sock_db_master_tx, chan_tx, chan_rx| worker_db(sock_db_master_tx, chan_tx, chan_rx, db).unwrap(),
            move |mut sock, tx, rx| {
                let ((key_a, value_a), (key_b, value_b), (key_c, value_c)) = (rnd_kv(), rnd_kv(), rnd_kv());
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Add(key_a.clone(), value_a.clone())),
                                  DbRep::Local(DbLocalRep::Added(key_a.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Add(key_a.clone(), value_b.clone())),
                                  DbRep::Global(GlobalRep::Kept));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Add(key_b.clone(), value_b.clone())),
                                  DbRep::Local(DbLocalRep::Added(key_b.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(177, key_a.clone())),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 177, key: key_a.clone(), value: value_a.clone(), }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(277, key_b.clone())),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 277, key: key_b.clone(), value: value_b.clone(), }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(377, key_c.clone())),
                                  DbRep::Global(GlobalRep::Error(ProtoError::DbQueueOutOfSync(key_c.clone()))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::RepayUpdate(key_a.clone(), value_c.clone())),
                                  DbRep::Global(GlobalRep::Repaid));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(177, key_a.clone())),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 177, key: key_a.clone(), value: value_c.clone(), }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Update(key_b.clone(), value_c.clone())),
                                  DbRep::Global(GlobalRep::Updated));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(277, key_b.clone())),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 277, key: key_b.clone(), value: value_c.clone(), }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Update(key_c.clone(), value_c.clone())),
                                  DbRep::Global(GlobalRep::NotFound));
                assert_worker_cmd(&mut sock, &tx, &rx, DbReq::Local(DbLocalReq::Stop), DbRep::Local(DbLocalRep::Stopped));
            });
    }

    #[test]
    fn pq_worker() {
        let pq = pq::PQueue::new();
        with_worker(
            "pq",
            move |sock_pq_master_tx, chan_tx, chan_rx| worker_pq(sock_pq_master_tx, chan_tx, chan_rx, pq).unwrap(),
            move |mut sock, tx, rx| {
                let ((key_a, value_a), (key_b, value_b)) = (rnd_kv(), rnd_kv());
                let now = SteadyTime::now();
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Global(GlobalReq::Count), PqRep::Global(GlobalRep::Counted(0)));
                tx_chan(PqReq::Local(PqLocalReq::Enqueue(key_a.clone())), None, &tx);
                tx_chan(PqReq::Local(PqLocalReq::Enqueue(key_b.clone())), None, &tx);
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Local(PqLocalReq::NextTrigger), PqRep::Local(PqLocalRep::TriggerGot(None)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(1000, now + Duration::milliseconds(1000))),
                                  PqRep::Local(PqLocalRep::Lent(3, key_a.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(1000)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500))),
                                  PqRep::Local(PqLocalRep::Lent(3, key_b.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(500)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(250, SteadyTime::now() + Duration::milliseconds(250))),
                                  PqRep::Local(PqLocalRep::EmptyQueueHit { timeout: 250 }));
                tx_chan(PqReq::Local(PqLocalReq::RepayTimedOut), None, &tx);
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(1000)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500))),
                                  PqRep::Local(PqLocalRep::Lent(4, key_b.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Global(GlobalReq::Repay { lend_key: 3,
                                                                   key: key_a.clone(),
                                                                   value: value_a.clone(),
                                                                   status: RepayStatus::Penalty }),
                                  PqRep::Local(PqLocalRep::Repaid(key_a.clone(), value_a.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(500)))));
                tx_chan(PqReq::Local(PqLocalReq::RepayTimedOut), None, &tx);
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Global(GlobalReq::Count), PqRep::Global(GlobalRep::Counted(2)));
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Local(PqLocalReq::NextTrigger), PqRep::Local(PqLocalRep::TriggerGot(None)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500))),
                                  PqRep::Local(PqLocalRep::Lent(6, key_b.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Global(GlobalReq::Repay { lend_key: 6,
                                                                   key: key_b.clone(),
                                                                   value: value_b.clone(),
                                                                   status: RepayStatus::Drop }),
                                  PqRep::Local(PqLocalRep::Repaid(key_b.clone(), value_b.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Global(GlobalReq::Count), PqRep::Global(GlobalRep::Counted(1)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Global(GlobalReq::Repay { lend_key: 6,
                                                                   key: key_b.clone(),
                                                                   value: value_b.clone(),
                                                                   status: RepayStatus::Penalty }),
                                  PqRep::Global(GlobalRep::NotFound));
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Local(PqLocalReq::NextTrigger), PqRep::Local(PqLocalRep::TriggerGot(None)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500))),
                                  PqRep::Local(PqLocalRep::Lent(6, key_a.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(500)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::Heartbeat(6, key_a.clone(), now + Duration::milliseconds(1000))),
                                  PqRep::Global(GlobalRep::Heartbeaten));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::Heartbeat(6, key_b.clone(), now + Duration::milliseconds(1000))),
                                  PqRep::Global(GlobalRep::Skipped));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(1000)))));

                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Local(PqLocalReq::Stop), PqRep::Local(PqLocalRep::Stopped));
            });
    }

    fn tx_sock(packet: GlobalReq, sock: &mut zmq::Socket) {
        let required = packet.encode_len();
        let mut msg = zmq::Message::with_capacity(required).unwrap();
        packet.encode(&mut msg);
        sock.send_msg(msg, 0).unwrap();
    }

    fn rx_sock(sock: &mut zmq::Socket) -> GlobalRep {
        let msg = sock.recv_msg(0).unwrap();
        assert!(!sock.get_rcvmore().unwrap());
        let (rep, _) = GlobalRep::decode(&msg).unwrap();
        rep
    }

    #[test]
    fn server() {
        let path = "/tmp/spiderq_server";
        let _ = fs::remove_dir_all(path);
        let zmq_addr = "inproc://server";
        let (mut ctx, master_thread) = entrypoint(zmq_addr, path, 16).unwrap();
        let mut sock_ftd = ctx.socket(zmq::REQ).unwrap();
        sock_ftd.connect(zmq_addr).unwrap();
        let sock = &mut sock_ftd;

        let ((key_a, value_a), (key_b, value_b)) = (rnd_kv(), rnd_kv());
        tx_sock(GlobalReq::Add(key_a.clone(), value_a.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::Added);
        tx_sock(GlobalReq::Lookup(key_a.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::ValueFound(value_a.clone()));
        tx_sock(GlobalReq::Lookup(key_b.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::ValueNotFound);
        tx_sock(GlobalReq::Add(key_a.clone(), value_b.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::Kept);
        tx_sock(GlobalReq::Add(key_b.clone(), value_b.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::Added);
        tx_sock(GlobalReq::Lookup(key_b.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::ValueFound(value_b.clone()));
        tx_sock(GlobalReq::Lend { timeout: 1000, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 3, key: key_a.clone(), value: value_a.clone(), });
        tx_sock(GlobalReq::Lend { timeout: 500, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 3, key: key_b.clone(), value: value_b.clone(), });
        tx_sock(GlobalReq::Count, sock); assert_eq!(rx_sock(sock), GlobalRep::Counted(0));
        tx_sock(GlobalReq::Repay { lend_key: 3, key: key_b.clone(), value: value_a.clone(), status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Repaid);
        tx_sock(GlobalReq::Repay { lend_key: 3, key: key_a.clone(), value: value_b.clone(), status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Repaid);
        tx_sock(GlobalReq::Count, sock); assert_eq!(rx_sock(sock), GlobalRep::Counted(2));
        tx_sock(GlobalReq::Lend { timeout: 10000, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 5, key: key_b.clone(), value: value_a.clone(), });
        tx_sock(GlobalReq::Lend { timeout: 1000, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 5, key: key_a.clone(), value: value_b.clone(), });
        tx_sock(GlobalReq::Count, sock); assert_eq!(rx_sock(sock), GlobalRep::Counted(0));

        fn round_ms(t: SteadyTime, expected: i64, variance: i64) -> i64 {
            let value = (SteadyTime::now() - t).num_milliseconds();
            ((value + variance) / expected) * expected
        }
        let t_start_a = SteadyTime::now();
        tx_sock(GlobalReq::Lend { timeout: 500, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 6, key: key_a.clone(), value: value_b.clone(), });
        assert_eq!(round_ms(t_start_a, 1000, 100), 1000);
        tx_sock(GlobalReq::Count, sock); assert_eq!(rx_sock(sock), GlobalRep::Counted(0));
        tx_sock(GlobalReq::Update(key_a.clone(), value_a.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::Updated);
        tx_sock(GlobalReq::Heartbeat { lend_key: 6, key: key_a.clone(), timeout: 1000, }, sock); assert_eq!(rx_sock(sock), GlobalRep::Heartbeaten);
        let t_start_b = SteadyTime::now();
        tx_sock(GlobalReq::Lend { timeout: 500, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 7, key: key_a.clone(), value: value_a.clone(), });
        assert_eq!(round_ms(t_start_b, 1000, 100), 1000);
        tx_sock(GlobalReq::Repay { lend_key: 7, key: key_a.clone(), value: value_a.clone(), status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Repaid);
        tx_sock(GlobalReq::Repay { lend_key: 7, key: key_a.clone(), value: value_a.clone(), status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::NotFound);
        tx_sock(GlobalReq::Stats, sock);
        assert_eq!(rx_sock(sock),
                   GlobalRep::StatsGot {
                       count: 4,
                       add: 3,
                       update: 1,
                       lookup: 3,
                       lend: 6,
                       repay: 4,
                       heartbeat: 1,
                       stats: 1, });
        tx_sock(GlobalReq::Terminate, sock); assert_eq!(rx_sock(sock), GlobalRep::Terminated);
        master_thread.join().unwrap();
    }
}

