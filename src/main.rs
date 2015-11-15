#![feature(drain, slice_bytes)]

extern crate zmq;
extern crate time;
extern crate getopts;
extern crate tempdir;
extern crate byteorder;
#[cfg(test)] extern crate rand;

use std::{io, env, mem, process};
use std::io::Write;
use std::convert::From;
use std::thread::spawn;
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use std::num::ParseIntError;
use getopts::Options;
use time::{SteadyTime, Duration};

pub mod db;
pub mod pq;
pub mod proto;
use proto::{Key, Value, RepayStatus, ProtoError, GlobalReq, GlobalRep};

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

fn entrypoint(maybe_matches: getopts::Result) -> Result<(), Error> {
    let matches = try!(maybe_matches.map_err(|e| Error::Getopts(e)));
    let database_dir = matches.opt_str("database").unwrap_or("./spiderq".to_owned());
    let zmq_addr = matches.opt_str("zmq-addr").unwrap_or("ipc://./spiderq.ipc".to_owned());
    let flush_limit: usize = try!(matches.opt_str("flush-limit").unwrap_or("131072".to_owned()).parse().map_err(|e| Error::InvalidFlushLimit(e)));

    let db = try!(db::Database::new(&database_dir, flush_limit).map_err(|e| Error::Db(e)));
    let pq = pq::PQueue::new();
    let mut ctx = zmq::Context::new();
    let mut sock_master_ext = try!(ctx.socket(zmq::ROUTER).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_master_db_rx = try!(ctx.socket(zmq::PULL).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_master_pq_rx = try!(ctx.socket(zmq::PULL).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_db_master_tx = try!(ctx.socket(zmq::PUSH).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_pq_master_tx = try!(ctx.socket(zmq::PUSH).map_err(|e| Error::Zmq(ZmqError::Socket(e))));

    try!(sock_master_ext.bind(&zmq_addr).map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_master_db_rx.bind("inproc://db_rxtx").map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_db_master_tx.connect("inproc://db_rxtx").map_err(|e| Error::Zmq(ZmqError::Connect(e))));
    try!(sock_master_pq_rx.bind("inproc://pq_rxtx").map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_pq_master_tx.connect("inproc://pq_rxtx").map_err(|e| Error::Zmq(ZmqError::Connect(e))));

    let (chan_master_db_tx, chan_db_master_rx) = channel();
    let (chan_db_master_tx, chan_master_db_rx) = channel();
    let (chan_master_pq_tx, chan_pq_master_rx) = channel();
    let (chan_pq_master_tx, chan_master_pq_rx) = channel();

    spawn(move || worker_db(sock_db_master_tx, chan_db_master_tx, chan_db_master_rx, db).unwrap());
    spawn(move || worker_pq(sock_pq_master_tx, chan_pq_master_tx, chan_pq_master_rx, pq).unwrap());
    master(sock_master_ext,
           sock_master_db_rx,
           sock_master_pq_rx,
           chan_master_db_tx,
           chan_master_db_rx,
           chan_master_pq_tx,
           chan_master_pq_rx)
}

#[derive(Debug, PartialEq)]
pub enum DbLocalReq {
    LoadLent(Key),
    RepayUpdate(Key, Value),
    Stop,
}

#[derive(Debug, PartialEq)]
pub enum PqLocalReq {
    NextTrigger,
    Enqueue(Key),
    LendUntil(u64, SteadyTime),
    RepayTimedOut,
    RepayQueue(Key, RepayStatus),
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
    Lent(Key),
    EmptyQueueHit { timeout: u64, },
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

pub fn rx_sock(sock: &mut zmq::Socket) -> Result<(Option<Headers>, Result<GlobalReq, ProtoError>), Error> {
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

pub fn tx_sock(packet: GlobalRep, maybe_headers: Option<Headers>, sock: &mut zmq::Socket) -> Result<(), Error> {
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

fn tx_chan_n<R>(packet: R, maybe_headers: Option<Headers>, chan: &Sender<Message<R>>, notify_sock: &mut zmq::Socket) -> Result<(), Error> {
    tx_chan(packet, maybe_headers, chan);
    notify_sock
        .send_msg(try!(zmq::Message::new().map_err(|e| Error::Zmq(ZmqError::Message(e)))), 0)
        .map_err(|e| Error::Zmq(ZmqError::Send(e)))
}


pub fn worker_db(mut sock_tx: zmq::Socket, 
                 chan_tx: Sender<Message<DbRep>>,
                 chan_rx: Receiver<Message<DbReq>>,
                 mut db: db::Database) -> Result<(), Error>
{
    loop {
        let req = chan_rx.recv().unwrap();
        match req.load {
            DbReq::Global(GlobalReq::Add(key, value)) =>
                if db.lookup(&key).is_some() {
                    try!(tx_chan_n(DbRep::Global(GlobalRep::Kept), req.headers, &chan_tx, &mut sock_tx))
                } else {
                    db.insert(key.clone(), value);
                    try!(tx_chan_n(DbRep::Local(DbLocalRep::Added(key)), req.headers, &chan_tx, &mut sock_tx))
                },
            DbReq::Global(..) =>
                unreachable!(),
            DbReq::Local(DbLocalReq::LoadLent(key)) =>
                if let Some(value) = db.lookup(&key) {
                    try!(tx_chan_n(DbRep::Global(GlobalRep::Lent(key, value.clone())), req.headers, &chan_tx, &mut sock_tx))
                } else {
                    try!(tx_chan_n(DbRep::Global(GlobalRep::Error(ProtoError::DbQueueOutOfSync(key.clone()))), req.headers, &chan_tx, &mut sock_tx))
                },
            DbReq::Local(DbLocalReq::RepayUpdate(key, value)) =>
                db.insert(key, value),
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
    loop {
        let req = chan_rx.recv().unwrap();
        match req.load {
            PqReq::Global(GlobalReq::Count) =>
                try!(tx_chan_n(PqRep::Global(GlobalRep::Counted(pq.len())), req.headers, &chan_tx, &mut sock_tx)),
            PqReq::Global(..) =>
                unreachable!(),
            PqReq::Local(PqLocalReq::Enqueue(key)) =>
                pq.add(key),
            PqReq::Local(PqLocalReq::LendUntil(timeout, trigger_at)) =>
                if let Some(key) = pq.top() {
                    try!(tx_chan_n(PqRep::Local(PqLocalRep::Lent(key)), req.headers, &chan_tx, &mut sock_tx));
                    pq.lend(trigger_at)
                } else {
                    try!(tx_chan_n(PqRep::Local(PqLocalRep::EmptyQueueHit { timeout: timeout }), req.headers, &chan_tx, &mut sock_tx))
                },
            PqReq::Local(PqLocalReq::RepayQueue(key, status)) =>
                pq.repay(key, status),
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

pub fn master(mut sock_ext: zmq::Socket,
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
    let mut stats_lend = 0;
    let mut stats_repay = 0;
    let mut stats_stats = 0;

    let (mut incoming_queue, mut pending_queue) = (Vec::new(), Vec::new());
    let mut next_timeout: Option<SteadyTime> = None;
    let mut stop_state = StopState::NotTriggered;
    loop {
        // check if it is time to quit
        match stop_state {
            StopState::Finished(headers) => {
                try!(tx_sock(GlobalRep::Terminated, headers, &mut sock_ext));
                return Ok(())
            },
            _ => (),
        }

        let current_time = SteadyTime::now();

        // calculate poll delay
        let timeout = if let Some(next_trigger) = next_timeout {
            let interval = next_trigger - current_time;
            let timeout = interval.num_milliseconds();
            if timeout <= 0 {
                next_timeout = None;
                tx_chan(PqReq::Local(PqLocalReq::RepayTimedOut), None, &chan_pq_tx);
                continue;
            } else {
                timeout
            }
        } else {
            tx_chan(PqReq::Local(PqLocalReq::NextTrigger), None, &chan_pq_tx);
            0
        };

        let avail_socks = {
            let mut pollitems = [sock_ext.as_poll_item(zmq::POLLIN),
                                 sock_db_rx.as_poll_item(zmq::POLLIN),
                                 sock_pq_rx.as_poll_item(zmq::POLLIN)];
            if try!(zmq::poll(&mut pollitems, timeout).map_err(|e| Error::Zmq(ZmqError::Poll(e)))) == 0 {
                continue
            }

            [pollitems[0].get_revents() == zmq::POLLIN,
             pollitems[1].get_revents() == zmq::POLLIN,
             pollitems[2].get_revents() == zmq::POLLIN]
        };

        if avail_socks[0] {
            // sock_ext is online
            match rx_sock(&mut sock_ext) {
                Ok((headers, Ok(req))) => pending_queue.push((headers, req)),
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
            match chan_db_rx.try_recv() {
                Ok(message) => match message.load {
                    DbRep::Global(rep @ GlobalRep::Kept) => {
                        stats_add += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    DbRep::Global(rep @ GlobalRep::Lent(..)) => {
                        stats_lend += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    DbRep::Global(rep @ GlobalRep::Error(..)) =>
                        try!(tx_sock(rep, message.headers, &mut sock_ext)),
                    DbRep::Global(..) =>
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
            match chan_pq_rx.try_recv() {
                Ok(message) => match message.load {
                    PqRep::Global(rep @ GlobalRep::Counted(..)) => {
                        stats_count += 1;
                        try!(tx_sock(rep, message.headers, &mut sock_ext));
                    },
                    PqRep::Global(..) =>
                        unreachable!(),
                    PqRep::Local(PqLocalRep::Lent(key)) =>
                        tx_chan(DbReq::Local(DbLocalReq::LoadLent(key)), message.headers, &chan_db_tx),
                    PqRep::Local(PqLocalRep::EmptyQueueHit { timeout: t, }) =>
                        pending_queue.push((message.headers, GlobalReq::Lend { timeout: t, })),
                    PqRep::Local(PqLocalRep::TriggerGot(Some(trigger))) =>
                        next_timeout = Some(trigger),
                    PqRep::Local(PqLocalRep::TriggerGot(None)) =>
                        next_timeout = Some(current_time + Duration::milliseconds(100)),
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
                    panic!("db worker thread is down"),
            }
        }

        // process incoming messages
        for (headers, global_req) in incoming_queue.drain(..) {
            match global_req {
                req @ GlobalReq::Count =>
                    tx_chan(PqReq::Global(req), headers, &chan_pq_tx),
                req @ GlobalReq::Add(..) =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                GlobalReq::Lend { timeout: t, } => {
                    let trigger_at = current_time + Duration::milliseconds(t as i64);
                    tx_chan(PqReq::Local(PqLocalReq::LendUntil(t, trigger_at)), headers, &chan_pq_tx);
                },
                GlobalReq::Repay(key, value, status) => {
                    tx_chan(PqReq::Local(PqLocalReq::RepayQueue(key.clone(), status)), None, &chan_pq_tx);
                    tx_chan(DbReq::Local(DbLocalReq::RepayUpdate(key, value)), None, &chan_db_tx);
                    stats_repay += 1;
                    try!(tx_sock(GlobalRep::Repaid, headers, &mut sock_ext));
                },
                GlobalReq::Stats => {
                    stats_stats += 1;
                    try!(tx_sock(GlobalRep::StatsGot { count: stats_count,
                                                       add: stats_add,
                                                       lend: stats_lend,
                                                       repay: stats_repay,
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

    match entrypoint(opts.parse(args)) {
        Ok(()) => (),
        Err(cause) => {
            let _ = writeln!(&mut io::stderr(), "Error: {:?}", cause);
            let usage = format!("Usage: {}", cmd_proc);
            let _ = writeln!(&mut io::stderr(), "{}", opts.usage(&usage[..]));
            process::exit(1);
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::sync::Arc;
    use std::fmt::Debug;
    use std::thread::spawn;
    use rand::{thread_rng, sample, Rng};
    use std::sync::mpsc::{channel, Sender, Receiver};
    use super::proto::{Key, Value, GlobalReq, GlobalRep, ProtoError};
    use super::{zmq, db, pq, worker_db, worker_pq, tx_chan};
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
                                  DbReq::Local(DbLocalReq::LoadLent(key_a.clone())),
                                  DbRep::Global(GlobalRep::Lent(key_a.clone(), value_a.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(key_b.clone())),
                                  DbRep::Global(GlobalRep::Lent(key_b.clone(), value_b.clone())));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(key_c.clone())),
                                  DbRep::Global(GlobalRep::Error(ProtoError::DbQueueOutOfSync(key_c.clone()))));
                tx_chan(DbReq::Local(DbLocalReq::RepayUpdate(key_a.clone(), value_c.clone())), None, &tx);
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(key_a.clone())),
                                  DbRep::Global(GlobalRep::Lent(key_a.clone(), value_c.clone())));
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
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Local(PqLocalReq::Stop), PqRep::Local(PqLocalRep::Stopped));
            });
    }
}

