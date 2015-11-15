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
    Proto(ProtoError),
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
    headers: Option<Headers>,
    load: R,
}

pub fn rx_sock(sock: &mut zmq::Socket) -> Result<(Option<Headers>, GlobalReq), Error> {
    let mut frames = Vec::new();
    loop {
        frames.push(try!(sock.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e)))));
        if !try!(sock.get_rcvmore().map_err(|e| Error::Zmq(ZmqError::GetSockOpt(e)))) {
            break
        }
    }

    let load_msg = frames.pop().unwrap();
    Ok((Some(frames), try!(GlobalReq::decode(&load_msg).map(|p| p.0).map_err(|e| Error::Proto(e)))))
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

fn tx_chan<R>(packet: R, maybe_headers: Option<Headers>, chan: &Sender<Message<R>>) {
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
        WaitingPqAndDb(Headers),
        WaitingPq(Headers),
        WaitingDb(Headers),
        Finished(Headers),
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
                try!(tx_sock(GlobalRep::Terminated, Some(headers), &mut sock_ext));
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
            pending_queue.push(try!(rx_sock(&mut sock_ext)));
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
        for (headers, req) in incoming_queue.drain(..) {
        }

        mem::swap(&mut incoming_queue, &mut pending_queue);
    }
}


// pub fn master(sock_ext: &mut zmq::Socket,
//               sock_db_tx: &mut zmq::Socket,
//               sock_db_rx: &mut zmq::Socket,
//               sock_pq_tx: &mut zmq::Socket,
//               sock_pq_rx: &mut zmq::Socket) -> Result<(), Error> {
//     enum StopState {
//         NotTriggered,
//         WaitingPqAndDb(Frames),
//         WaitingPq(Frames),
//         WaitingDb(Frames),
//         Finished(Frames),
//     }

//     let mut stats_count = 0;
//     let mut stats_add = 0;
//     let mut stats_lend = 0;
//     let mut stats_repay = 0;
//     let mut stats_stats = 0;

//     let mut stop_state = StopState::NotTriggered;
//     loop {
//         match stop_state {
//             StopState::Finished(frames) => {
//                 try!(proto_reply(sock_ext, frames, Rep::Local(LocalRep::Stopped)));
//                 return Ok(())
//             },
//             _ => (),
//         }

//         let avail_socks = {
//             let mut pollitems = [sock_ext.as_poll_item(zmq::POLLIN),
//                                  sock_db_rx.as_poll_item(zmq::POLLIN),
//                                  sock_pq_rx.as_poll_item(zmq::POLLIN)];
//             if try!(zmq::poll(&mut pollitems, -1).map_err(|e| Error::Zmq(ZmqError::Poll(e)))) == 0 {
//                 continue
//             }

//             [pollitems[0].get_revents() == zmq::POLLIN,
//              pollitems[1].get_revents() == zmq::POLLIN,
//              pollitems[2].get_revents() == zmq::POLLIN]
//         };

//         if avail_socks[0] {
//             // sock_ext is online        
//             enum Decision<'a> {
//                 TransmitPq,
//                 TransmitDb,
//                 BroadcastStop,
//                 JustReply(Rep<'a>),
//             }

//             let req_frames = try!(Frames::recv(sock_ext));
//             let decision = match Req::decode(&req_frames) {
//                 Ok(Req::Global(GlobalReq::Count)) =>
//                     Decision::TransmitPq,
//                 Ok(Req::Global(GlobalReq::Add(..))) =>
//                     Decision::TransmitDb,
//                 Ok(Req::Global(GlobalReq::Lend { .. })) =>
//                     Decision::TransmitPq,
//                 Ok(Req::Global(GlobalReq::Repay(..))) =>
//                     Decision::TransmitPq,
//                 Ok(Req::Local(LocalReq::Stop)) =>
//                     Decision::BroadcastStop,
//                 Ok(Req::Global(GlobalReq::Stats)) => {
//                     stats_stats += 1;
//                     Decision::JustReply(Rep::GlobalOk(GlobalRep::StatsGot {
//                         count: stats_count,
//                         add: stats_add,
//                         lend: stats_lend,
//                         repay: stats_repay,
//                         stats: stats_stats,
//                     }))
//                 },
//                 Ok(..) =>
//                     Decision::JustReply(Rep::GlobalErr(ProtoError::UnexpectedMasterRequest)),
//                 Err(proto_err) =>
//                     Decision::JustReply(Rep::GlobalErr(proto_err)),
//             };

//             match decision {
//                 Decision::TransmitPq =>
//                     try!(req_frames.transmit(sock_pq_tx)),
//                 Decision::TransmitDb =>
//                     try!(req_frames.transmit(sock_db_tx)),
//                 Decision::BroadcastStop => {
//                     try!(sock_pq_tx.send_msg(try!(encode_into_msg!(Req::Local(LocalReq::Stop))), 0).map_err(|e| Error::Zmq(ZmqError::Send(e))));
//                     try!(sock_db_tx.send_msg(try!(encode_into_msg!(Req::Local(LocalReq::Stop))), 0).map_err(|e| Error::Zmq(ZmqError::Send(e))));
//                     stop_state = StopState::WaitingPqAndDb(req_frames);
//                 },
//                 Decision::JustReply(rep) =>
//                     try!(proto_reply(sock_ext, req_frames, rep)),
//             }
//         }

//         if avail_socks[1] {
//             // sock_db is online
//             enum Decision {
//                 PassPqAddAndReply(u32),
//                 DoNothing,
//                 TransmitExt,
//             }

//             let frames = try!(Frames::recv(sock_db_rx));
//             let decision = match Rep::decode(&frames) {
//                 Ok(Rep::Local(LocalRep::Added(id))) =>
//                     Decision::PassPqAddAndReply(id),
//                 Ok(Rep::Local(LocalRep::Stopped)) => {
//                     stop_state = match stop_state {
//                         StopState::NotTriggered | StopState::WaitingPq(..) | StopState::Finished(..) => unreachable!(),
//                         StopState::WaitingPqAndDb(stop_frames) => StopState::WaitingPq(stop_frames),
//                         StopState::WaitingDb(stop_frames) => StopState::Finished(stop_frames),
//                     };
//                     Decision::DoNothing
//                 },
//                 Ok(Rep::GlobalOk(GlobalRep::Lent(..))) => {
//                     stats_lend += 1;
//                     Decision::TransmitExt
//                 },
//                 Ok(Rep::GlobalErr(..)) =>
//                     Decision::TransmitExt,
//                 Ok(Rep::GlobalOk(GlobalRep::Counted(..))) =>
//                     panic!("unexpected GlobalRep::Counted reply from worker_db"),
//                 Ok(Rep::GlobalOk(GlobalRep::Added(..))) =>
//                     panic!("unexpected GlobalRep::Added reply from worker_db"),
//                 Ok(Rep::GlobalOk(GlobalRep::Repaid(..))) =>
//                     panic!("unexpected GlobalRep::Repaid reply from worker_db"),
//                 Ok(Rep::GlobalOk(GlobalRep::StatsGot { .. })) =>
//                     panic!("unexpected GlobalRep::StatsGot reply from worker_db"),
//                 Ok(Rep::Local(LocalRep::Lent(..))) =>
//                     panic!("unexpected LocalRep::Lend reply from worker_db"),
//                 Ok(Rep::Local(LocalRep::Panicked(msg))) =>
//                     panic!("worker_db panic'd with message: {}", msg),
//                 Err(err) =>
//                     panic!("failed to decode worker_db reply: {:?}, error: {:?}", frames.deref(), err),
//             };

//             match decision {
//                 Decision::PassPqAddAndReply(id) => {
//                     stats_add += 1;
//                     try!(sock_pq_tx.send_msg(try!(encode_into_msg!(Req::Local(LocalReq::AddEnqueue(id)))), 0)
//                          .map_err(|e| Error::Zmq(ZmqError::Send(e))));
//                     try!(proto_reply(sock_ext, frames, Rep::GlobalOk(GlobalRep::Added(id))));
//                 },
//                 Decision::DoNothing =>
//                     (),
//                 Decision::TransmitExt =>
//                     try!(frames.transmit(sock_ext)),
//             }
//         }

//         if avail_socks[2] {
//             // sock_pq is online
//             enum Decision {
//                 PassDbLoad(u32),
//                 DoNothing,
//                 TransmitExt,
//             }

//             let frames = try!(Frames::recv(sock_pq_rx));
//             let decision = match Rep::decode(&frames) {
//                 Ok(Rep::Local(LocalRep::Lent(id))) =>
//                     Decision::PassDbLoad(id),
//                 Ok(Rep::Local(LocalRep::Stopped)) => {
//                     stop_state = match stop_state {
//                         StopState::NotTriggered | StopState::WaitingDb(..) | StopState::Finished(..) => unreachable!(),
//                         StopState::WaitingPqAndDb(stop_frames) => StopState::WaitingDb(stop_frames),
//                         StopState::WaitingPq(stop_frames) => StopState::Finished(stop_frames),
//                     };
//                     Decision::DoNothing
//                 },
//                 Ok(Rep::GlobalOk(GlobalRep::Counted(..))) => {
//                     stats_count += 1;
//                     Decision::TransmitExt
//                 },
//                 Ok(Rep::GlobalOk(GlobalRep::Repaid)) => {
//                     stats_repay += 1;
//                     Decision::TransmitExt
//                 },
//                 Ok(Rep::GlobalErr(..)) =>
//                     Decision::TransmitExt,
//                 Ok(Rep::GlobalOk(GlobalRep::Added(..))) =>
//                     panic!("unexpected GlobalRep::Added reply from worker_pq"),
//                 Ok(Rep::GlobalOk(GlobalRep::Lent(..))) =>
//                     panic!("unexpected GlobalRep::Lent reply from worker_pq"),
//                 Ok(Rep::GlobalOk(GlobalRep::StatsGot { .. })) =>
//                     panic!("unexpected GlobalRep::StatsGot reply from worker_pq"),
//                 Ok(Rep::Local(LocalRep::Added(..))) =>
//                     panic!("unexpected LocalRep::Added reply from worker_pq"),
//                 Ok(Rep::Local(LocalRep::Panicked(msg))) =>
//                     panic!("worker_pq panic'd with message: {}", msg),
//                 Err(err) =>
//                     panic!("failed to decode worker_pq reply: {:?}, error: {:?}", frames.deref(), err),
//             };

//             match decision {
//                 Decision::PassDbLoad(id) =>
//                     try!(proto_request(sock_db_tx, frames, Req::Local(LocalReq::Load(id)))),
//                 Decision::DoNothing =>
//                     (),
//                 Decision::TransmitExt =>
//                     try!(frames.transmit(sock_ext)),
//             }
//         }
//     }
// }

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

// #[cfg(test)]
// mod test {
//     extern crate zmq;

//     use std::fs;
//     use std::thread::spawn;
//     use time::SteadyTime;
//     use super::{db, pq, worker_db_entrypoint, worker_pq_entrypoint, master};
//     use super::proto::{RepayStatus, Req, LocalReq, GlobalReq, Rep, LocalRep, GlobalRep};

//     fn proto_request(sock: &mut zmq::Socket, req: Req) {
//         let bytes_required = req.encode_len();
//         let mut msg = zmq::Message::with_capacity(bytes_required).unwrap();
//         req.encode(&mut msg);
//         sock.send_msg(msg, 0).unwrap();
//     }

//     fn assert_request_reply(sock_tx: &mut zmq::Socket, sock_rx: &mut zmq::Socket, req: Req, rep: Rep) {
//         proto_request(sock_tx, req);
//         let rep_msg = sock_rx.recv_msg(0).unwrap();
//         assert_eq!(Rep::decode(&rep_msg), Ok(rep));
//     }

//     fn with_worker<WF, MF>(base_addr: &str, worker_fn: WF, master_fn: MF)
//         where WF: FnOnce(zmq::Socket, zmq::Socket) + Send + 'static, MF: FnOnce(&mut zmq::Socket, &mut zmq::Socket) + Send + 'static
//     {
//         let mut ctx = zmq::Context::new();
//         let mut sock_master_slave_tx = ctx.socket(zmq::PUSH).unwrap();
//         let mut sock_master_slave_rx = ctx.socket(zmq::PULL).unwrap();
//         let mut sock_slave_master_tx = ctx.socket(zmq::PUSH).unwrap();
//         let mut sock_slave_master_rx = ctx.socket(zmq::PULL).unwrap();

//         let tx_addr = format!("inproc://{}_txrx", base_addr);
//         sock_master_slave_tx.bind(&tx_addr).unwrap();
//         sock_slave_master_rx.connect(&tx_addr).unwrap();
//         let rx_addr = format!("inproc://{}_rxtx", base_addr);
//         sock_master_slave_rx.bind(&rx_addr).unwrap();
//         sock_slave_master_tx.connect(&rx_addr).unwrap();

//         let worker = spawn(move || worker_fn(sock_slave_master_tx, sock_slave_master_rx));
//         master_fn(&mut sock_master_slave_tx, &mut sock_master_slave_rx);
//         assert_request_reply(&mut sock_master_slave_tx, &mut sock_master_slave_rx,
//                              Req::Local(LocalReq::Stop),
//                              Rep::Local(LocalRep::Stopped));
//         worker.join().unwrap();
//     }

//     #[test]
//     fn pq_worker() {
//         let pq = pq::PQueue::new(3);
//         with_worker(
//             "pq",
//             move |sock_pq_master_tx, sock_pq_master_rx| worker_pq_entrypoint(sock_pq_master_tx, sock_pq_master_rx, pq),
//             move |sock_master_pq_tx, sock_master_pq_rx| {
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Count),
//                                      Rep::GlobalOk(GlobalRep::Counted(3)));
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Lend { timeout: 10000, }),
//                                      Rep::Local(LocalRep::Lent(0)));
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Count),
//                                      Rep::GlobalOk(GlobalRep::Counted(2)));
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Lend { timeout: 10000, }),
//                                      Rep::Local(LocalRep::Lent(1)));
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Count),
//                                      Rep::GlobalOk(GlobalRep::Counted(1)));
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Repay(1, RepayStatus::Front)),
//                                      Rep::GlobalOk(GlobalRep::Repaid));
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Count),
//                                      Rep::GlobalOk(GlobalRep::Counted(2)));
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Lend { timeout: 10000, }),
//                                      Rep::Local(LocalRep::Lent(1)));
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Count),
//                                      Rep::GlobalOk(GlobalRep::Counted(1)));
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Lend { timeout: 1000, }),
//                                      Rep::Local(LocalRep::Lent(2)));
//                 assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                      Req::Global(GlobalReq::Count),
//                                      Rep::GlobalOk(GlobalRep::Counted(0)));
//                 {
//                     let start = SteadyTime::now();
//                     assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
//                                          Req::Global(GlobalReq::Lend { timeout: 10000, }),
//                                          Rep::Local(LocalRep::Lent(2)));
//                     let interval = SteadyTime::now() - start;
//                     assert_eq!(interval.num_seconds(), 1);
//                 }
//             });
//     }

//     #[test]
//     fn db_worker() {
//         let path = "/tmp/spiderq_main";
//         let _ = fs::remove_dir_all(path);
//         let db = db::Database::new(path, false).unwrap();
//         with_worker(
//             "pq",
//             move |sock_db_master_tx, sock_db_master_rx| worker_db_entrypoint(sock_db_master_tx, sock_db_master_rx, db),
//             move |sock_master_db_tx, sock_master_db_rx| {
//                 let user_data_0 = "some user data #0".as_bytes();

//                 assert_request_reply(sock_master_db_tx, sock_master_db_rx,
//                                      Req::Global(GlobalReq::Add(None)),
//                                      Rep::Local(LocalRep::Added(0)));
//                 assert_request_reply(sock_master_db_tx, sock_master_db_rx,
//                                      Req::Global(GlobalReq::Add(Some(user_data_0))),
//                                      Rep::Local(LocalRep::Added(1)));
//                 assert_request_reply(sock_master_db_tx, sock_master_db_rx,
//                                      Req::Local(LocalReq::Load(0)),
//                                      Rep::GlobalOk(GlobalRep::Lent(0, None)));
//                 assert_request_reply(sock_master_db_tx, sock_master_db_rx,
//                                      Req::Local(LocalReq::Load(1)),
//                                      Rep::GlobalOk(GlobalRep::Lent(1, Some(user_data_0))));
//             });
//     }

//     fn assert_request_reply_ext(sock: &mut zmq::Socket, req: Req, rep: Rep) {
//         proto_request(sock, req);
//         let rep_msg = sock.recv_msg(0).unwrap();
//         assert_eq!(Rep::decode(&rep_msg), Ok(rep));
//     }

//     #[test]
//     fn full_server() {
//         let path = "/tmp/spiderq_master";
//         let _ = fs::remove_dir_all(path);
//         let db = db::Database::new(path, false).unwrap();
//         let pq = pq::PQueue::new(0);
//         let mut ctx = zmq::Context::new();
//         let mut sock_master_ext_peer = ctx.socket(zmq::REQ).unwrap();
//         let mut sock_master_ext = ctx.socket(zmq::ROUTER).unwrap();
//         let mut sock_master_db_tx = ctx.socket(zmq::PUSH).unwrap();
//         let mut sock_master_db_rx = ctx.socket(zmq::PULL).unwrap();
//         let mut sock_master_pq_tx = ctx.socket(zmq::PUSH).unwrap();
//         let mut sock_master_pq_rx = ctx.socket(zmq::PULL).unwrap();
//         let mut sock_db_master_tx = ctx.socket(zmq::PUSH).unwrap();
//         let mut sock_db_master_rx = ctx.socket(zmq::PULL).unwrap();
//         let mut sock_pq_master_tx = ctx.socket(zmq::PUSH).unwrap();
//         let mut sock_pq_master_rx = ctx.socket(zmq::PULL).unwrap();

//         sock_master_ext_peer.bind("inproc://test_master_ext_txrx").unwrap();
//         sock_master_ext.connect("inproc://test_master_ext_txrx").unwrap();

//         sock_master_db_tx.bind("inproc://test_master_db_txrx").unwrap();
//         sock_db_master_rx.connect("inproc://test_master_db_txrx").unwrap();
//         sock_master_db_rx.bind("inproc://test_master_db_rxtx").unwrap();
//         sock_db_master_tx.connect("inproc://test_master_db_rxtx").unwrap();

//         sock_master_pq_tx.bind("inproc://test_master_pq_txrx").unwrap();
//         sock_pq_master_rx.connect("inproc://test_master_pq_txrx").unwrap();
//         sock_master_pq_rx.bind("inproc://test_master_pq_rxtx").unwrap();
//         sock_pq_master_tx.connect("inproc://test_master_pq_rxtx").unwrap();

//         let worker_db = spawn(move || worker_db_entrypoint(sock_db_master_tx, sock_db_master_rx, db));
//         let worker_pq = spawn(move || worker_pq_entrypoint(sock_pq_master_tx, sock_pq_master_rx, pq));
//         let master_thread = spawn(move || master(&mut sock_master_ext,
//                                                  &mut sock_master_db_tx,
//                                                  &mut sock_master_db_rx,
//                                                  &mut sock_master_pq_tx,
//                                                  &mut sock_master_pq_rx).unwrap());

//         let entry_a = "Entry A".as_bytes();
//         let entry_b = "Entry B".as_bytes();
//         let entry_c = "Entry C".as_bytes();

//         // first fill
//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(0)));
//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Add(Some(entry_a))), Rep::GlobalOk(GlobalRep::Added(0)));
//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(1)));
//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Add(Some(entry_b))), Rep::GlobalOk(GlobalRep::Added(1)));
//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(2)));
//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Add(Some(entry_c))), Rep::GlobalOk(GlobalRep::Added(2)));
//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(3)));
//         // lend one and repay it back
//         assert_request_reply_ext(&mut sock_master_ext_peer,
//                                  Req::Global(GlobalReq::Lend { timeout: 10000, }),
//                                  Rep::GlobalOk(GlobalRep::Lent(0, Some(entry_a))));
//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(2)));
//         assert_request_reply_ext(&mut sock_master_ext_peer,
//                                  Req::Global(GlobalReq::Repay(0, RepayStatus::Front)),
//                                  Rep::GlobalOk(GlobalRep::Repaid));
//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(3)));
//         // lend all entries
//         assert_request_reply_ext(&mut sock_master_ext_peer,
//                                  Req::Global(GlobalReq::Lend { timeout: 10000, }),
//                                  Rep::GlobalOk(GlobalRep::Lent(0, Some(entry_a))));
//         assert_request_reply_ext(&mut sock_master_ext_peer,
//                                  Req::Global(GlobalReq::Lend { timeout: 1000, }),
//                                  Rep::GlobalOk(GlobalRep::Lent(1, Some(entry_b))));
//         assert_request_reply_ext(&mut sock_master_ext_peer,
//                                  Req::Global(GlobalReq::Lend { timeout: 10000, }),
//                                  Rep::GlobalOk(GlobalRep::Lent(2, Some(entry_c))));
//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(0)));
//         // check blocking
//         {
//             let start = SteadyTime::now();
//             assert_request_reply_ext(&mut sock_master_ext_peer,
//                                      Req::Global(GlobalReq::Lend { timeout: 10000, }),
//                                      Rep::GlobalOk(GlobalRep::Lent(1, Some(entry_b))));
//             let interval = SteadyTime::now() - start;
//             assert_eq!(interval.num_seconds(), 1);
//         }

//         assert_request_reply_ext(&mut sock_master_ext_peer,
//                                  Req::Global(GlobalReq::Stats),
//                                  Rep::GlobalOk(GlobalRep::StatsGot { 
//                                      count: 7,
//                                      add: 3,
//                                      lend: 5,
//                                      repay: 1,
//                                      stats: 1,
//                                  }));

//         assert_request_reply_ext(&mut sock_master_ext_peer, Req::Local(LocalReq::Stop), Rep::Local(LocalRep::Stopped));
//         master_thread.join().unwrap();
//         worker_pq.join().unwrap();
//         worker_db.join().unwrap();
//     }
// }

