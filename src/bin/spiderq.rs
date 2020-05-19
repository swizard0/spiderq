use std::{
    io::{
        self,
        Write,
    },
    env,
    process,
    convert::From,
    thread::{
        Builder,
        JoinHandle,
    },
    sync::mpsc::{
        channel,
        Sender,
        Receiver,
        TryRecvError,
    },
};

use time::{
    Duration,
};

use getopts::Options;

use simple_signal::Signal;

use spiderq::{db, pq};

use spiderq_proto::{
    Key,
    Value,
    LendMode,
    AddMode,
    ProtoError,
    GlobalReq,
    GlobalRep,
};

const MAX_POLL_TIMEOUT: i64 = 100;

#[derive(Debug)]
pub enum Error {
    Getopts(getopts::Fail),
    Db(db::Error),
    Pq(pq::Error),
    Zmq(ZmqError),
}

impl From<pq::Error> for Error {
    fn from(err: pq::Error) -> Self {
        Error::Pq(err)
    }
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
    let matches = maybe_matches.map_err(Error::Getopts)?;
    let database_dir = matches.opt_str("database").unwrap_or("./spiderq".to_owned());
    let zmq_addr = matches.opt_str("zmq-addr").unwrap_or("ipc://./spiderq.ipc".to_owned());
    let zmq_addr_cloned = zmq_addr.replace("//*:", "//127.0.0.1:");
    simple_signal::set_handler(&[Signal::Hup, Signal::Int, Signal::Quit, Signal::Abrt, Signal::Term], move |signals| {
        let zmq_ctx = zmq::Context::new();
        let sock = zmq_ctx.socket(zmq::REQ).map_err(ZmqError::Socket).unwrap();
        sock.connect(&zmq_addr_cloned).map_err(ZmqError::Connect).unwrap();
        println!(" ;; {:?} received, terminating server...", signals);
        let packet = GlobalReq::Terminate;
        let required = packet.encode_len();
        let mut msg = zmq::Message::with_capacity(required)
            .map_err(ZmqError::Message)
            .unwrap();
        packet.encode(&mut msg);
        sock.send_msg(msg, 0).unwrap();
        let reply_msg = sock.recv_msg(0).map_err(ZmqError::Recv).unwrap();
        let (rep, _) = GlobalRep::decode(&reply_msg).unwrap();
        match rep {
            GlobalRep::Terminated => (),
            other => panic!("unexpected reply for terminate: {:?}", other),
        }
    });
    entrypoint(&zmq_addr, &database_dir)
}

pub fn entrypoint(zmq_addr: &str, database_dir: &str) -> Result<(zmq::Context, JoinHandle<()>), Error> {
    let db = db::Database::new(database_dir).map_err(Error::Db)?;
    let pq = pq::PQueue::new(database_dir).map_err(Error::Pq)?;

    let ctx = zmq::Context::new();
    let sock_master_ext = ctx.socket(zmq::ROUTER).map_err(ZmqError::Socket).map_err(Error::Zmq)?;
    let sock_master_db_rx = ctx.socket(zmq::PULL).map_err(ZmqError::Socket).map_err(Error::Zmq)?;
    let sock_master_pq_rx = ctx.socket(zmq::PULL).map_err(ZmqError::Socket).map_err(Error::Zmq)?;
    let sock_db_master_tx = ctx.socket(zmq::PUSH).map_err(ZmqError::Socket).map_err(Error::Zmq)?;
    let sock_pq_master_tx = ctx.socket(zmq::PUSH).map_err(ZmqError::Socket).map_err(Error::Zmq)?;

    sock_master_ext.bind(zmq_addr).map_err(ZmqError::Bind).map_err(Error::Zmq)?;
    sock_master_db_rx.bind("inproc://db_rxtx").map_err(ZmqError::Bind).map_err(Error::Zmq)?;
    sock_db_master_tx.connect("inproc://db_rxtx").map_err(ZmqError::Connect).map_err(Error::Zmq)?;
    sock_master_pq_rx.bind("inproc://pq_rxtx").map_err(ZmqError::Bind).map_err(Error::Zmq)?;
    sock_pq_master_tx.connect("inproc://pq_rxtx").map_err(ZmqError::Connect).map_err(Error::Zmq)?;

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
    LoadLent(u64, Key, u64, pq::Instant, LendMode),
    RepayUpdate(Key, Value),
    Stop,
}

#[derive(Debug, PartialEq)]
pub enum PqLocalReq {
    NextTrigger,
    Enqueue(Key, AddMode),
    LendUntil(u64, pq::Instant, LendMode),
    RepayTimedOut,
    Heartbeat(u64, Key, pq::Instant),
    Remove(Key),
    Stop,
}

#[derive(Debug, PartialEq)]
pub enum DbLocalRep {
    Added(Key, AddMode),
    Removed(Key),
    LentNotFound(Key, u64, pq::Instant, LendMode),
    Stopped,
}

#[derive(Debug, PartialEq)]
pub enum PqLocalRep {
    TriggerGot(Option<pq::Instant>),
    Lent(u64, Key, u64, pq::Instant, LendMode),
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
        frames.push(sock.recv_msg(0).map_err(ZmqError::Recv).map_err(Error::Zmq)?);
        if !sock.get_rcvmore().map_err(ZmqError::GetSockOpt).map_err(Error::Zmq)? {
            break
        }
    }

    let load_msg = frames.pop().unwrap();
    Ok((Some(frames), GlobalReq::decode(&load_msg).map(|p| p.0)))
}

fn tx_sock(packet: GlobalRep, maybe_headers: Option<Headers>, sock: &mut zmq::Socket) -> Result<(), Error> {
    let required = packet.encode_len();
    let mut load_msg = zmq::Message::with_capacity(required)
        .map_err(ZmqError::Message)
        .map_err(Error::Zmq)?;
    packet.encode(&mut load_msg);

    if let Some(headers) = maybe_headers {
        for header in headers {
            sock.send_msg(header, zmq::SNDMORE)
                .map_err(ZmqError::Send)
                .map_err(Error::Zmq)?;
        }
    }
    sock.send_msg(load_msg, 0)
        .map_err(ZmqError::Send)
        .map_err(Error::Zmq)
}

pub fn tx_chan<R>(packet: R, maybe_headers: Option<Headers>, chan: &Sender<Message<R>>) {
    chan.send(Message { headers: maybe_headers, load: packet, }).unwrap()
}

fn notify_sock(sock: &mut zmq::Socket) -> Result<(), Error> {
    let msg = zmq::Message::new()
        .map_err(ZmqError::Message)
        .map_err(Error::Zmq)?;
    sock.send_msg(msg, 0)
        .map_err(ZmqError::Send)
        .map_err(Error::Zmq)
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
    notify_sock(&mut sock_tx)?;
    loop {
        let req = chan_rx.recv().unwrap();
        match req.load {
            DbReq::Global(GlobalReq::Add { key: k, value: v, mode: m, }) => {
                if db.lookup(&k).is_some() {
                    tx_chan_n(DbRep::Global(GlobalRep::Kept), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    db.insert(k.clone(), v)?;
                    tx_chan_n(DbRep::Local(DbLocalRep::Added(k, m)), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            DbReq::Global(GlobalReq::Update(key, value)) => {
                if db.lookup(&key).is_some() {
                    db.insert(key.clone(), value)?;
                    tx_chan_n(DbRep::Global(GlobalRep::Updated), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    tx_chan_n(DbRep::Global(GlobalRep::NotFound), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            DbReq::Global(GlobalReq::Lookup(key)) => {
                if let Some(value) = db.lookup(&key) {
                    tx_chan_n(DbRep::Global(GlobalRep::ValueFound(value.clone())), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    tx_chan_n(DbRep::Global(GlobalRep::ValueNotFound), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            DbReq::Global(GlobalReq::Remove(key)) => {
                if db.lookup(&key).is_some() {
                    db.remove(key.clone())?;
                    tx_chan_n(DbRep::Local(DbLocalRep::Removed(key)), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    tx_chan_n(DbRep::Global(GlobalRep::NotRemoved), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            DbReq::Global(GlobalReq::Flush) => {
                db.flush()?;
                tx_chan_n(DbRep::Global(GlobalRep::Flushed), req.headers, &chan_tx, &mut sock_tx)?
            },
            DbReq::Global(..) =>
                unreachable!(),
            DbReq::Local(DbLocalReq::LoadLent(lend_key, key, timeout, trigger_at, mode)) =>
                if let Some(value) = db.lookup(&key) {
                    tx_chan_n(DbRep::Global(GlobalRep::Lent { lend_key: lend_key, key: key, value: value.clone(), }),
                              req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    tx_chan_n(DbRep::Local(DbLocalRep::LentNotFound(key, timeout, trigger_at, mode)), req.headers, &chan_tx, &mut sock_tx)?
                },
            DbReq::Local(DbLocalReq::RepayUpdate(key, value)) => {
                db.insert(key, value)?;
                tx_chan_n(DbRep::Global(GlobalRep::Repaid), req.headers, &chan_tx, &mut sock_tx)?
            },
            DbReq::Local(DbLocalReq::Stop) => {
                tx_chan_n(DbRep::Local(DbLocalRep::Stopped), req.headers, &chan_tx, &mut sock_tx)?;
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
    notify_sock(&mut sock_tx)?;
    loop {
        let req = chan_rx.recv().unwrap();
        match req.load {
            PqReq::Global(GlobalReq::Count) =>
                tx_chan_n(PqRep::Global(GlobalRep::Counted(pq.len())), req.headers, &chan_tx, &mut sock_tx)?,
            PqReq::Global(GlobalReq::Repay { lend_key: rlend_key, key: rkey, value: rvalue, status: rstatus, }) =>
                if pq.repay(rlend_key, rkey.clone(), rstatus)? {
                    tx_chan_n(PqRep::Local(PqLocalRep::Repaid(rkey, rvalue)), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    tx_chan_n(PqRep::Global(GlobalRep::NotFound), req.headers, &chan_tx, &mut sock_tx)?
                },
            PqReq::Global(..) =>
                unreachable!(),
            PqReq::Local(PqLocalReq::Enqueue(key, mode)) =>
                pq.add(key, mode)?,
            PqReq::Local(PqLocalReq::Remove(key)) =>
                pq.remove(key)?,
            PqReq::Local(PqLocalReq::LendUntil(timeout, trigger_at, mode)) =>
                if let Some((key, serial)) = pq.top()? {
                    tx_chan_n(PqRep::Local(PqLocalRep::Lent(serial, key, timeout, trigger_at, mode)), req.headers, &chan_tx, &mut sock_tx)?;
                    pq.lend(trigger_at)?;
                } else {
                    match mode {
                        LendMode::Block =>
                            tx_chan_n(PqRep::Local(PqLocalRep::EmptyQueueHit { timeout: timeout }), req.headers, &chan_tx, &mut sock_tx)?,
                        LendMode::Poll =>
                            tx_chan_n(PqRep::Global(GlobalRep::QueueEmpty), req.headers, &chan_tx, &mut sock_tx)?,
                    }
                },
            PqReq::Local(PqLocalReq::Heartbeat(lend_key, ref key, trigger_at)) => {
                if pq.heartbeat(lend_key, key, trigger_at)? {
                    tx_chan_n(PqRep::Global(GlobalRep::Heartbeaten), req.headers, &chan_tx, &mut sock_tx)?
                } else {
                    tx_chan_n(PqRep::Global(GlobalRep::Skipped), req.headers, &chan_tx, &mut sock_tx)?
                }
            },
            PqReq::Local(PqLocalReq::NextTrigger) =>
                tx_chan_n(PqRep::Local(PqLocalRep::TriggerGot(pq.next_timeout()?)), req.headers, &chan_tx, &mut sock_tx)?,
            PqReq::Local(PqLocalReq::RepayTimedOut) =>
                pq.repay_timed_out()?,
            PqReq::Local(PqLocalReq::Stop) => {
                tx_chan_n(PqRep::Local(PqLocalRep::Stopped), req.headers, &chan_tx, &mut sock_tx)?;
                return Ok(())
            },
        }
    }
}

fn master(mut sock_ext: zmq::Socket,
          sock_db_rx: zmq::Socket,
          sock_pq_rx: zmq::Socket,
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

    let mut stats_ping = 0;
    let mut stats_count = 0;
    let mut stats_add = 0;
    let mut stats_update = 0;
    let mut stats_lookup = 0;
    let mut stats_remove = 0;
    let mut stats_lend = 0;
    let mut stats_repay = 0;
    let mut stats_heartbeat = 0;
    let mut stats_stats = 0;

    let (mut incoming_queue, mut pending_queue) = (Vec::new(), Vec::new());
    let mut next_timeout: Option<Option<pq::Instant>> = None;
    let mut stop_state = StopState::NotTriggered;

    // sync with workers
    let _ = sock_db_rx.recv_msg(0).map_err(ZmqError::Recv).map_err(Error::Zmq)?;
    let _ = sock_pq_rx.recv_msg(0).map_err(ZmqError::Recv).map_err(Error::Zmq)?;

    loop {
        // check if it is time to quit
        match stop_state {
            StopState::Finished(headers) => {
                tx_sock(GlobalRep::Terminated, headers, &mut sock_ext)?;
                return Ok(())
            },
            _ => (),
        }

        let mut pq_changed = false;
        let before_poll_ts = pq::Instant::now();

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
                        tx_chan(PqReq::Local(PqLocalReq::RepayTimedOut), None, &chan_pq_tx);
                        tx_chan(PqReq::Local(PqLocalReq::NextTrigger), None, &chan_pq_tx);
                        next_timeout = None;
                        pq_changed = true;
                        MAX_POLL_TIMEOUT
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
            zmq::poll(&mut pollitems, timeout)
                .map_err(ZmqError::Poll)
                .map_err(Error::Zmq)?;
            [pollitems[0].get_revents() == zmq::POLLIN,
             pollitems[1].get_revents() == zmq::POLLIN,
             pollitems[2].get_revents() == zmq::POLLIN]
        };

        let after_poll_ts = pq::Instant::now();
        if avail_socks[0] {
            // sock_ext is online
            match rx_sock(&mut sock_ext) {
                Ok((headers, Ok(req))) => incoming_queue.push((headers, req)),
                Ok((headers, Err(e))) => tx_sock(GlobalRep::Error(e), headers, &mut sock_ext)?,
                Err(e) => return Err(e),
            }
        }

        if avail_socks[1] {
            // sock_db is online, skip ping msg
            let _ = sock_db_rx.recv_msg(0).map_err(ZmqError::Recv).map_err(Error::Zmq)?;
        }

        if avail_socks[2] {
            // sock_pq is online, skip ping msg
            let _ = sock_pq_rx.recv_msg(0).map_err(ZmqError::Recv).map_err(Error::Zmq)?;
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
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::Updated) => {
                        stats_update += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::NotFound) => {
                        stats_update += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::ValueFound(..)) => {
                        stats_lookup += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::ValueNotFound) => {
                        stats_lookup += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::NotRemoved) => {
                        stats_remove += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::Lent { .. }) => {
                        stats_lend += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::Repaid) => {
                        stats_repay += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Global(rep @ GlobalRep::Flushed) =>
                        tx_sock(rep, message.headers, &mut sock_ext)?,
                    DbRep::Global(rep @ GlobalRep::Error(..)) =>
                        tx_sock(rep, message.headers, &mut sock_ext)?,
                    DbRep::Global(GlobalRep::Pong) |
                    DbRep::Global(GlobalRep::Counted(..)) |
                    DbRep::Global(GlobalRep::Added) |
                    DbRep::Global(GlobalRep::Removed) |
                    DbRep::Global(GlobalRep::QueueEmpty) |
                    DbRep::Global(GlobalRep::Heartbeaten) |
                    DbRep::Global(GlobalRep::Skipped) |
                    DbRep::Global(GlobalRep::StatsGot { .. }) |
                    DbRep::Global(GlobalRep::Terminated) =>
                        unreachable!(),
                    DbRep::Local(DbLocalRep::Added(key, mode)) => {
                        tx_chan(PqReq::Local(PqLocalReq::Enqueue(key, mode)), None, &chan_pq_tx);
                        stats_add += 1;
                        pq_changed = true;
                        tx_sock(GlobalRep::Added, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Local(DbLocalRep::Removed(key)) => {
                        tx_chan(PqReq::Local(PqLocalReq::Remove(key)), None, &chan_pq_tx);
                        stats_remove += 1;
                        pq_changed = true;
                        tx_sock(GlobalRep::Removed, message.headers, &mut sock_ext)?;
                    },
                    DbRep::Local(DbLocalRep::LentNotFound(key, timeout, trigger_at, mode)) => {
                        tx_chan(PqReq::Local(PqLocalReq::Remove(key.clone())), None, &chan_pq_tx);
                        tx_chan(PqReq::Local(PqLocalReq::LendUntil(timeout, trigger_at, mode)), message.headers, &chan_pq_tx);
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
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    PqRep::Global(rep @ GlobalRep::Heartbeaten) => {
                        stats_heartbeat += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                        next_timeout = None;
                        pq_changed = true;
                    },
                    PqRep::Global(rep @ GlobalRep::Skipped) => {
                        stats_heartbeat += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    PqRep::Global(rep @ GlobalRep::NotFound) => {
                        stats_repay += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    PqRep::Global(rep @ GlobalRep::QueueEmpty) => {
                        stats_lend += 1;
                        tx_sock(rep, message.headers, &mut sock_ext)?;
                    },
                    PqRep::Global(GlobalRep::Pong) |
                    PqRep::Global(GlobalRep::Added) |
                    PqRep::Global(GlobalRep::Kept) |
                    PqRep::Global(GlobalRep::Updated) |
                    PqRep::Global(GlobalRep::ValueFound(..)) |
                    PqRep::Global(GlobalRep::ValueNotFound) |
                    PqRep::Global(GlobalRep::Removed) |
                    PqRep::Global(GlobalRep::NotRemoved) |
                    PqRep::Global(GlobalRep::Lent { .. }) |
                    PqRep::Global(GlobalRep::Repaid) |
                    PqRep::Global(GlobalRep::StatsGot { .. }) |
                    PqRep::Global(GlobalRep::Flushed) |
                    PqRep::Global(GlobalRep::Terminated) |
                    PqRep::Global(GlobalRep::Error(..)) =>
                        unreachable!(),
                    PqRep::Local(PqLocalRep::Lent(lend_key, key, timeout, trigger_at, mode)) => {
                        tx_chan(DbReq::Local(DbLocalReq::LoadLent(lend_key, key, timeout, trigger_at, mode)), message.headers, &chan_db_tx);
                        next_timeout = None;
                    },
                    PqRep::Local(PqLocalRep::EmptyQueueHit { timeout: t, }) =>
                        pending_queue.push((message.headers, GlobalReq::Lend { timeout: t, mode: LendMode::Block, })),
                    PqRep::Local(PqLocalRep::Repaid(key, value)) => {
                        tx_chan(DbReq::Local(DbLocalReq::RepayUpdate(key, value)), message.headers, &chan_db_tx);
                        next_timeout = None;
                        pq_changed = true;
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

        // repeat pending requests if need to
        if pq_changed {
            incoming_queue.extend(pending_queue.drain(..));
        }

        // process incoming messages
        for (headers, global_req) in incoming_queue.drain(..) {
            match global_req {
                GlobalReq::Ping => {
                    stats_ping += 1;
                    tx_sock(GlobalRep::Pong, headers, &mut sock_ext)?
                },
                req @ GlobalReq::Count =>
                    tx_chan(PqReq::Global(req), headers, &chan_pq_tx),
                req @ GlobalReq::Add { .. } =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Update(..) =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Lookup(..) =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Remove(..) =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                req @ GlobalReq::Repay { .. } =>
                    tx_chan(PqReq::Global(req), headers, &chan_pq_tx),
                req @ GlobalReq::Flush =>
                    tx_chan(DbReq::Global(req), headers, &chan_db_tx),
                GlobalReq::Lend { timeout: t, mode: m, } => {
                    let trigger_at = after_poll_ts + Duration::milliseconds(t as i64);
                    tx_chan(PqReq::Local(PqLocalReq::LendUntil(t, trigger_at, m)), headers, &chan_pq_tx);
                },
                GlobalReq::Heartbeat { lend_key: l, key: k, timeout: t, } => {
                    let trigger_at = after_poll_ts + Duration::milliseconds(t as i64);
                    tx_chan(PqReq::Local(PqLocalReq::Heartbeat(l, k, trigger_at)), headers, &chan_pq_tx);
                },
                GlobalReq::Stats => {
                    stats_stats += 1;
                    tx_sock(GlobalRep::StatsGot {
                        ping: stats_ping,
                        count: stats_count,
                        add: stats_add,
                        update: stats_update,
                        lookup: stats_lookup,
                        remove: stats_remove,
                        lend: stats_lend,
                        repay: stats_repay,
                        heartbeat: stats_heartbeat,
                        stats: stats_stats,
                    }, headers, &mut sock_ext)?;
                },
                GlobalReq::Terminate => {
                    tx_chan(PqReq::Local(PqLocalReq::Stop), None, &chan_pq_tx);
                    tx_chan(DbReq::Local(DbLocalReq::Stop), None, &chan_db_tx);
                    stop_state = StopState::WaitingPqAndDb(headers);
                },
            }
        }
    }
}

fn main() {
    let mut args = env::args();
    let cmd_proc = args.next().unwrap();
    let mut opts = Options::new();

    opts.optopt("d", "database", "database directory path (optional, default: ./spiderq)", "");
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
    use std::fmt::Debug;
    use std::thread::spawn;
    use time::Duration;
    use rand::{thread_rng, Rng, distributions::Uniform};
    use std::sync::mpsc::{channel, Sender, Receiver};
    use spiderq_proto::{Key, Value, LendMode, AddMode, RepayStatus, GlobalReq, GlobalRep};
    use zmq;
    use super::{db, pq, worker_db, worker_pq, tx_chan, entrypoint};
    use super::{Message, DbReq, DbRep, PqReq, PqRep, DbLocalReq, DbLocalRep, PqLocalReq, PqLocalRep};

    fn with_worker<WF, MF, Req, Rep>(base_addr: &str, worker_fn: WF, master_fn: MF) where
        WF: FnOnce(zmq::Socket, Sender<Message<Rep>>, Receiver<Message<Req>>) + Send + 'static,
        MF: FnOnce(zmq::Socket, Sender<Message<Req>>, Receiver<Message<Rep>>) + Send + 'static,
        Req: Send + 'static, Rep: Send + 'static
    {
        let ctx = zmq::Context::new();
        let sock_master_slave_rx = ctx.socket(zmq::PULL).unwrap();
        let sock_slave_master_tx = ctx.socket(zmq::PUSH).unwrap();

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
        let byte_range = Uniform::from(0..255);

        let key: Key = rng
            .sample_iter(byte_range)
            .take(rng.gen_range(1, 64))
            .collect();

        let value: Value = rng
            .sample_iter(byte_range)
            .take(rng.gen_range(1, 64))
            .collect();

        (key, value)
    }

    #[test]
    fn db_worker() {
        let path = "/tmp/spiderq_main";
        let _ = fs::remove_dir_all(path);
        let db = db::Database::new(path).unwrap();
        with_worker(
            "db",
            move |sock_db_master_tx, chan_tx, chan_rx| worker_db(sock_db_master_tx, chan_tx, chan_rx, db).unwrap(),
            move |mut sock, tx, rx| {
                let ((key_a, value_a), (key_b, value_b), (key_c, value_c)) = (rnd_kv(), rnd_kv(), rnd_kv());
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Add { key: key_a.clone(), value: value_a.clone(), mode: AddMode::Tail, }),
                                  DbRep::Local(DbLocalRep::Added(key_a.clone(), AddMode::Tail)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Add { key: key_a.clone(), value: value_b.clone(), mode: AddMode::Tail, }),
                                  DbRep::Global(GlobalRep::Kept));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Add { key: key_b.clone(), value: value_b.clone(), mode: AddMode::Head, }),
                                  DbRep::Local(DbLocalRep::Added(key_b.clone(), AddMode::Head)));
                let now = pq::Instant::now();
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(177, key_a.clone(), 1000, now, LendMode::Poll)),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 177, key: key_a.clone(), value: value_a.clone(), }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(277, key_b.clone(), 1000, now, LendMode::Poll)),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 277, key: key_b.clone(), value: value_b.clone(), }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(377, key_c.clone(), 1000, now, LendMode::Poll)),
                                  DbRep::Local(DbLocalRep::LentNotFound(key_c.clone(), 1000, now, LendMode::Poll)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::RepayUpdate(key_a.clone(), value_c.clone())),
                                  DbRep::Global(GlobalRep::Repaid));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(177, key_a.clone(), 1000, now, LendMode::Poll)),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 177, key: key_a.clone(), value: value_c.clone(), }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Update(key_b.clone(), value_c.clone())),
                                  DbRep::Global(GlobalRep::Updated));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Local(DbLocalReq::LoadLent(277, key_b.clone(), 1000, now, LendMode::Poll)),
                                  DbRep::Global(GlobalRep::Lent { lend_key: 277, key: key_b.clone(), value: value_c.clone(), }));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  DbReq::Global(GlobalReq::Update(key_c.clone(), value_c.clone())),
                                  DbRep::Global(GlobalRep::NotFound));
                assert_worker_cmd(&mut sock, &tx, &rx, DbReq::Local(DbLocalReq::Stop), DbRep::Local(DbLocalRep::Stopped));
            });
    }

    #[test]
    fn pq_worker() {
        let path = "/tmp/spiderq_pq_main";
        let _ = fs::remove_dir_all(path);
        let pq = pq::PQueue::new(path).unwrap();
        with_worker(
            "pq",
            move |sock_pq_master_tx, chan_tx, chan_rx| worker_pq(sock_pq_master_tx, chan_tx, chan_rx, pq).unwrap(),
            move |mut sock, tx, rx| {
                let ((key_a, value_a), (key_b, value_b)) = (rnd_kv(), rnd_kv());
                let now = pq::Instant::now();
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Global(GlobalReq::Count), PqRep::Global(GlobalRep::Counted(0)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(1000, now + Duration::milliseconds(1000), LendMode::Poll)),
                                  PqRep::Global(GlobalRep::QueueEmpty));
                tx_chan(PqReq::Local(PqLocalReq::Enqueue(key_a.clone(), AddMode::Tail)), None, &tx);
                tx_chan(PqReq::Local(PqLocalReq::Enqueue(key_b.clone(), AddMode::Tail)), None, &tx);
                assert_worker_cmd(&mut sock, &tx, &rx, PqReq::Local(PqLocalReq::NextTrigger), PqRep::Local(PqLocalRep::TriggerGot(None)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(1000, now + Duration::milliseconds(1000), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::Lent(3, key_a.clone(), 1000, now + Duration::milliseconds(1000), LendMode::Block)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(1000)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::Lent(3, key_b.clone(), 500, now + Duration::milliseconds(500), LendMode::Block)));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(500)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(250, pq::Instant::now() + Duration::milliseconds(250), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::EmptyQueueHit { timeout: 250 }));
                tx_chan(PqReq::Local(PqLocalReq::RepayTimedOut), None, &tx);
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::NextTrigger),
                                  PqRep::Local(PqLocalRep::TriggerGot(Some(now + Duration::milliseconds(1000)))));
                assert_worker_cmd(&mut sock, &tx, &rx,
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::Lent(4, key_b.clone(), 500, now + Duration::milliseconds(500), LendMode::Block)));
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
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::Lent(6, key_b.clone(), 500, now + Duration::milliseconds(500), LendMode::Block)));
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
                                  PqReq::Local(PqLocalReq::LendUntil(500, now + Duration::milliseconds(500), LendMode::Block)),
                                  PqRep::Local(PqLocalRep::Lent(6, key_a.clone(), 500, now + Duration::milliseconds(500), LendMode::Block)));
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
        let (ctx, master_thread) = entrypoint(zmq_addr, path).unwrap();
        let mut sock_ftd = ctx.socket(zmq::REQ).unwrap();
        sock_ftd.connect(zmq_addr).unwrap();
        let sock = &mut sock_ftd;

        tx_sock(GlobalReq::Ping, sock); assert_eq!(rx_sock(sock), GlobalRep::Pong);
        let ((key_a, value_a), (key_b, value_b), (key_c, value_c)) = (rnd_kv(), rnd_kv(), rnd_kv());
        tx_sock(GlobalReq::Add { key: key_a.clone(), value: value_a.clone(), mode: AddMode::Tail, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Added);
        tx_sock(GlobalReq::Lookup(key_a.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::ValueFound(value_a.clone()));
        tx_sock(GlobalReq::Lookup(key_b.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::ValueNotFound);
        tx_sock(GlobalReq::Add { key: key_c.clone(), value: value_c.clone(), mode: AddMode::Tail, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Added);
        tx_sock(GlobalReq::Add { key: key_a.clone(), value: value_b.clone(), mode: AddMode::Tail, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Kept);
        tx_sock(GlobalReq::Add { key: key_b.clone(), value: value_b.clone(), mode: AddMode::Tail, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Added);
        tx_sock(GlobalReq::Lookup(key_b.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::ValueFound(value_b.clone()));
        tx_sock(GlobalReq::Lookup(key_c.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::ValueFound(value_c.clone()));
        tx_sock(GlobalReq::Remove(key_c.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::Removed);
        tx_sock(GlobalReq::Remove(key_c.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::NotRemoved);
        tx_sock(GlobalReq::Lookup(key_c.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::ValueNotFound);
        tx_sock(GlobalReq::Lend { timeout: 1000, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 4, key: key_a.clone(), value: value_a.clone(), });
        tx_sock(GlobalReq::Lend { timeout: 500, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 4, key: key_b.clone(), value: value_b.clone(), });
        tx_sock(GlobalReq::Count, sock); assert_eq!(rx_sock(sock), GlobalRep::Counted(0));
        tx_sock(GlobalReq::Lend { timeout: 500, mode: LendMode::Poll, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::QueueEmpty);
        tx_sock(GlobalReq::Repay { lend_key: 4, key: key_b.clone(), value: value_a.clone(), status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Repaid);
        tx_sock(GlobalReq::Repay { lend_key: 4, key: key_a.clone(), value: value_b.clone(), status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Repaid);
        tx_sock(GlobalReq::Count, sock); assert_eq!(rx_sock(sock), GlobalRep::Counted(2));
        tx_sock(GlobalReq::Lend { timeout: 10000, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 6, key: key_b.clone(), value: value_a.clone(), });
        tx_sock(GlobalReq::Lend { timeout: 1000, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 6, key: key_a.clone(), value: value_b.clone(), });
        tx_sock(GlobalReq::Count, sock); assert_eq!(rx_sock(sock), GlobalRep::Counted(0));

        fn round_ms(t: pq::Instant, expected: i64, variance: i64) -> i64 {
            let value = (pq::Instant::now() - t).num_milliseconds();
            ((value + variance) / expected) * expected
        }
        let t_start_a = pq::Instant::now();
        tx_sock(GlobalReq::Lend { timeout: 500, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 7, key: key_a.clone(), value: value_b.clone(), });
        assert_eq!(round_ms(t_start_a, 1000, 100), 1000);
        tx_sock(GlobalReq::Count, sock); assert_eq!(rx_sock(sock), GlobalRep::Counted(0));
        tx_sock(GlobalReq::Update(key_a.clone(), value_a.clone()), sock); assert_eq!(rx_sock(sock), GlobalRep::Updated);
        tx_sock(GlobalReq::Heartbeat { lend_key: 7, key: key_a.clone(), timeout: 1000, }, sock); assert_eq!(rx_sock(sock), GlobalRep::Heartbeaten);
        let t_start_b = pq::Instant::now();
        tx_sock(GlobalReq::Lend { timeout: 500, mode: LendMode::Block, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Lent { lend_key: 8, key: key_a.clone(), value: value_a.clone(), });
        assert_eq!(round_ms(t_start_b, 1000, 100), 1000);
        tx_sock(GlobalReq::Repay { lend_key: 8, key: key_a.clone(), value: value_a.clone(), status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::Repaid);
        tx_sock(GlobalReq::Repay { lend_key: 8, key: key_a.clone(), value: value_a.clone(), status: RepayStatus::Penalty, }, sock);
        assert_eq!(rx_sock(sock), GlobalRep::NotFound);
        tx_sock(GlobalReq::Stats, sock);
        assert_eq!(rx_sock(sock),
                   GlobalRep::StatsGot {
                       ping: 1,
                       count: 4,
                       add: 4,
                       update: 1,
                       lookup: 5,
                       remove: 2,
                       lend: 7,
                       repay: 4,
                       heartbeat: 1,
                       stats: 1, });
        tx_sock(GlobalReq::Terminate, sock); assert_eq!(rx_sock(sock), GlobalRep::Terminated);
        master_thread.join().unwrap();
    }
}
