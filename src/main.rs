#![feature(vec_resize, slice_bytes)]

extern crate zmq;
extern crate time;
extern crate getopts;
extern crate byteorder;

use std::{io, env, mem, process};
use std::io::Write;
use std::ops::Deref;
use std::convert::From;
use std::thread::spawn;
use std::collections::VecDeque;
use getopts::Options;
use time::{SteadyTime, Duration};

pub mod db;
pub mod pq;
pub mod proto;
use proto::{RepayStatus, Req, GlobalReq, LocalReq, Rep, GlobalRep, LocalRep, ProtoError};

#[derive(Debug)]
pub enum Error {
    Getopts(getopts::Fail),
    Db(db::Error),
    Zmq(ZmqError),
    EmptyFramesTransmit,
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

    let db = try!(db::Database::new(&database_dir).map_err(|e| Error::Db(e)));
    let pq = pq::PQueue::new(try!(db.count()));
    let mut ctx = zmq::Context::new();
    let mut sock_master_ext = try!(ctx.socket(zmq::ROUTER).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_master_db_tx = try!(ctx.socket(zmq::PUSH).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_master_db_rx = try!(ctx.socket(zmq::PULL).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_master_pq_tx = try!(ctx.socket(zmq::PUSH).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_master_pq_rx = try!(ctx.socket(zmq::PULL).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_db_master_tx = try!(ctx.socket(zmq::PUSH).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_db_master_rx = try!(ctx.socket(zmq::PULL).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_pq_master_tx = try!(ctx.socket(zmq::PUSH).map_err(|e| Error::Zmq(ZmqError::Socket(e))));
    let mut sock_pq_master_rx = try!(ctx.socket(zmq::PULL).map_err(|e| Error::Zmq(ZmqError::Socket(e))));

    try!(sock_master_ext.bind(&zmq_addr).map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_master_db_tx.bind("inproc://db_txrx").map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_db_master_rx.connect("inproc://db_txrx").map_err(|e| Error::Zmq(ZmqError::Connect(e))));
    try!(sock_master_db_rx.bind("inproc://db_rxtx").map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_db_master_tx.connect("inproc://db_rxtx").map_err(|e| Error::Zmq(ZmqError::Connect(e))));
    try!(sock_master_pq_tx.bind("inproc://pq_txrx").map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_pq_master_rx.connect("inproc://pq_txrx").map_err(|e| Error::Zmq(ZmqError::Connect(e))));
    try!(sock_master_pq_rx.bind("inproc://pq_rxtx").map_err(|e| Error::Zmq(ZmqError::Bind(e))));
    try!(sock_pq_master_tx.connect("inproc://pq_rxtx").map_err(|e| Error::Zmq(ZmqError::Connect(e))));

    spawn(move || worker_db_entrypoint(sock_db_master_tx, sock_db_master_rx, db));
    spawn(move || worker_pq_entrypoint(sock_pq_master_tx, sock_pq_master_rx, pq));
    master(&mut sock_master_ext,
           &mut sock_master_db_tx,
           &mut sock_master_db_rx,
           &mut sock_master_pq_tx,
           &mut sock_master_pq_rx)
}

struct Frames(Vec<zmq::Message>);

impl Frames {
    fn recv(sock: &mut zmq::Socket) -> Result<Frames, Error> {
        let mut frames = Vec::new();
        loop {
            frames.push(try!(sock.recv_msg(0).map_err(|e| Error::Zmq(ZmqError::Recv(e)))));
            if !try!(sock.get_rcvmore().map_err(|e| Error::Zmq(ZmqError::GetSockOpt(e)))) {
                break
            }
        }

        Ok(Frames(frames))
    }

    fn send(mut self, sock: &mut zmq::Socket, workload: zmq::Message) -> Result<(), Error> {
        self.0.last_mut().map(|m| mem::replace(m, workload));
        self.transmit(sock)
    }

    fn transmit(mut self, sock: &mut zmq::Socket) -> Result<(), Error> {
        if let Some(workload) = self.0.pop() {
            for frame in self.0 {
                try!(sock.send_msg(frame, zmq::SNDMORE).map_err(|e| Error::Zmq(ZmqError::Send(e))));
            }
            sock.send_msg(workload, 0).map_err(|e| Error::Zmq(ZmqError::Send(e)))
        } else {
            Err(Error::EmptyFramesTransmit)
        }
    }
}

impl Deref for Frames {
    type Target = [u8];

    fn deref<'a>(&'a self) -> &'a [u8] {
        &self.0.last().unwrap()
    }
}

macro_rules! encode_into_msg {
    ($packet:expr) => ({
        let packet = $packet;
        let bytes_required = packet.encode_len();
        zmq::Message::with_capacity(bytes_required)
            .map_err(|e| Error::Zmq(ZmqError::Message(e)))
            .map(|mut msg| {
                packet.encode(&mut msg);
                msg
            })
    })
}

fn proto_request(sock: &mut zmq::Socket, frames: Frames, req: Req) -> Result<(), Error> {
    frames.send(sock, try!(encode_into_msg!(req)))
}

fn proto_reply(sock: &mut zmq::Socket, frames: Frames, rep: Rep) -> Result<(), Error> {
    frames.send(sock, try!(encode_into_msg!(rep)))
}

pub fn worker_db_entrypoint(mut sock_tx: zmq::Socket, mut sock_rx: zmq::Socket, db: db::Database)  {
    match worker_db(&mut sock_tx, &mut sock_rx, db) {
        Ok(()) => (),
        Err(e) => {
            let panic_str = format!("{:?}", e);
            sock_tx.send_msg(encode_into_msg!(Rep::Local(LocalRep::Panicked(&panic_str))).unwrap(), 0).unwrap();
        },
    }
}

fn worker_db(sock_tx: &mut zmq::Socket, sock_rx: &mut zmq::Socket, mut db: db::Database) -> Result<(), Error> {
    loop {
        enum Decision<'a> {
            JustReply(Rep<'a>),
            Stop,
            ReplyMsg(zmq::Message),
        }
        
        let req_frames = try!(Frames::recv(sock_rx));
        let decision = match Req::decode(&req_frames) {
            Ok(Req::Global(GlobalReq::Add(maybe_data))) => {
                let id = try!(db.add(maybe_data.unwrap_or(&[])));
                Decision::JustReply(Rep::Local(LocalRep::Added(id)))
            },
            Ok(Req::Local(LocalReq::Load(id))) => {
                struct MsgLoader {
                    id: u32,
                    msg: Option<(zmq::Message, usize)>,
                }

                impl db::Loader for MsgLoader {
                    type Error = ZmqError;

                    fn set_len(&mut self, len: usize) -> Result<(), ZmqError> {
                        let base_rep = Rep::GlobalOk(GlobalRep::Lent(self.id, None));
                        let base_len = base_rep.encode_len();
                        let mut msg = try!(zmq::Message::with_capacity(base_len + len).map_err(|e| ZmqError::Message(e)));
                        base_rep.encode(&mut msg);
                        self.msg = Some((msg, base_len));
                        Ok(())
                    }

                    fn contents(&mut self) -> &mut [u8] {
                        self.msg.as_mut().map(|&mut (ref mut msg, offset)| &mut msg[offset ..]).unwrap()
                    }
                }

                let mut msg_loader = MsgLoader { id: id, msg: None };
                match db.load(id, &mut msg_loader) {
                    Ok(()) => Decision::ReplyMsg(msg_loader.msg.unwrap().0),
                    Err(db::LoadError::Db(e)) => return Err(Error::Db(e)),
                    Err(db::LoadError::Load(e)) => return Err(Error::Zmq(e)),
                }
            },
            Ok(Req::Local(LocalReq::Stop)) =>
                Decision::Stop,
            Ok(..) =>
                Decision::JustReply(Rep::GlobalErr(ProtoError::UnexpectedWorkerDbRequest)),
            Err(proto_err) =>
                Decision::JustReply(Rep::GlobalErr(proto_err)),
        };

        match decision {
            Decision::JustReply(rep) => 
                try!(proto_reply(sock_tx, req_frames, rep)),
            Decision::Stop => {
                try!(proto_reply(sock_tx, req_frames, Rep::Local(LocalRep::Stopped)));
                return Ok(())
            },
            Decision::ReplyMsg(msg) =>
                try!(req_frames.send(sock_tx, msg)),
        }
    }
}

pub fn worker_pq_entrypoint(mut sock_tx: zmq::Socket, mut sock_rx: zmq::Socket, pq: pq::PQueue)  {
    match worker_pq(&mut sock_tx, &mut sock_rx, pq) {
        Ok(()) => (),
        Err(e) => {
            let panic_str = format!("{:?}", e);
            sock_tx.send_msg(encode_into_msg!(Rep::Local(LocalRep::Panicked(&panic_str))).unwrap(), 0).unwrap();
        },
    }
}

fn worker_pq(sock_tx: &mut zmq::Socket, sock_rx: &mut zmq::Socket, mut pq: pq::PQueue) -> Result<(), Error> {
    let mut pending_queue = VecDeque::new();
    loop {
        let timeout = if let Some(next_timeout) = pq.next_timeout() {
            let interval = next_timeout - SteadyTime::now();
            let timeout = interval.num_milliseconds();
            if timeout < 0 {
                pq.repay_timed_out();
                0
            } else {
                timeout
            }
        } else {
            -1
        };

        let mut pollitems = [sock_rx.as_poll_item(zmq::POLLIN)];
        let avail = try!(zmq::poll(&mut pollitems, timeout).map_err(|e| Error::Zmq(ZmqError::Poll(e))));
        if (avail == 1) && (pollitems[0].get_revents() == zmq::POLLIN) {
            pending_queue.push_front(try!(Frames::recv(sock_rx)));
        }

        let mut tmp_queue = VecDeque::new();
        enum Decision<'a> {
            JustReply(Rep<'a>),
            Reenqueue,
            ProceedLend(u32, u64),
            ProceedRepay(u32, RepayStatus),
            DoNothing,
            Stop,
        }

        while let Some(req_frames) = pending_queue.pop_front() {
            let decision = match Req::decode(&req_frames) {
                Ok(Req::Local(LocalReq::Stop)) =>
                    Decision::Stop,
                Ok(Req::Global(GlobalReq::Count)) =>
                    Decision::JustReply(Rep::GlobalOk(GlobalRep::Counted(pq.len()))),
                Ok(Req::Global(GlobalReq::Lend { timeout: t, })) => {
                    if let Some(id) = pq.top() {
                        Decision::ProceedLend(id, t)
                    } else {
                        Decision::Reenqueue
                    }
                },
                Ok(Req::Global(GlobalReq::Repay(id, status))) =>
                    Decision::ProceedRepay(id, status),
                Ok(Req::Local(LocalReq::AddEnqueue(expected_id))) => {
                    let new_id = pq.add();
                    assert_eq!(expected_id, new_id);
                    Decision::DoNothing
                },
                Ok(..) =>
                    Decision::JustReply(Rep::GlobalErr(ProtoError::UnexpectedWorkerPqRequest)),
                Err(proto_err) =>
                    Decision::JustReply(Rep::GlobalErr(proto_err)),
            };

            match decision {
                Decision::JustReply(rep) => 
                    try!(proto_reply(sock_tx, req_frames, rep)),
                Decision::Stop => {
                    try!(proto_reply(sock_tx, req_frames, Rep::Local(LocalRep::Stopped)));
                    return Ok(())
                },
                Decision::ProceedLend(id, t) => {
                    try!(proto_reply(sock_tx, req_frames, Rep::Local(LocalRep::Lent(id))));
                    let trigger_at = SteadyTime::now() + Duration::milliseconds(t as i64);
                    assert_eq!(pq.lend(trigger_at), Some(id));
                },
                Decision::ProceedRepay(id, status) => {
                    try!(proto_reply(sock_tx, req_frames, Rep::GlobalOk(GlobalRep::Repaid)));
                    pq.repay(id, status);
                },
                Decision::DoNothing =>
                    (),
                Decision::Reenqueue => {
                    tmp_queue.push_back(req_frames);
                },
            }
        }

        mem::replace(&mut pending_queue, tmp_queue);
    }
}

pub fn master(sock_ext: &mut zmq::Socket,
              sock_db_tx: &mut zmq::Socket,
              sock_db_rx: &mut zmq::Socket,
              sock_pq_tx: &mut zmq::Socket,
              sock_pq_rx: &mut zmq::Socket) -> Result<(), Error> {
    enum StopState {
        NotTriggered,
        WaitingPqAndDb(Frames),
        WaitingPq(Frames),
        WaitingDb(Frames),
        Finished(Frames),
    }

    let mut stop_state = StopState::NotTriggered;
    loop {
        match stop_state {
            StopState::Finished(frames) => {
                try!(proto_reply(sock_ext, frames, Rep::Local(LocalRep::Stopped)));
                return Ok(())
            },
            _ => (),
        }

        let mut pollitems = [sock_ext.as_poll_item(zmq::POLLIN),
                             sock_db_rx.as_poll_item(zmq::POLLIN),
                             sock_pq_rx.as_poll_item(zmq::POLLIN)];
        if try!(zmq::poll(&mut pollitems, -1).map_err(|e| Error::Zmq(ZmqError::Poll(e)))) == 0 {
            continue
        }

        if pollitems[0].get_revents() == zmq::POLLIN {
            // sock_ext is online        
            enum Decision<'a> {
                TransmitPq,
                TransmitDb,
                BroadcastStop,
                JustReply(Rep<'a>),
            }

            let req_frames = try!(Frames::recv(sock_ext));
            let decision = match Req::decode(&req_frames) {
                Ok(Req::Global(GlobalReq::Count)) =>
                    Decision::TransmitPq,
                Ok(Req::Global(GlobalReq::Add(..))) =>
                    Decision::TransmitDb,
                Ok(Req::Global(GlobalReq::Lend { .. })) =>
                    Decision::TransmitPq,
                Ok(Req::Global(GlobalReq::Repay(..))) =>
                    Decision::TransmitPq,
                Ok(Req::Local(LocalReq::Stop)) =>
                    Decision::BroadcastStop,
                Ok(..) =>
                    Decision::JustReply(Rep::GlobalErr(ProtoError::UnexpectedMasterRequest)),
                Err(proto_err) =>
                    Decision::JustReply(Rep::GlobalErr(proto_err)),
            };

            match decision {
                Decision::TransmitPq =>
                    try!(req_frames.transmit(sock_pq_tx)),
                Decision::TransmitDb =>
                    try!(req_frames.transmit(sock_db_tx)),
                Decision::BroadcastStop => {
                    try!(sock_pq_tx.send_msg(try!(encode_into_msg!(Req::Local(LocalReq::Stop))), 0).map_err(|e| Error::Zmq(ZmqError::Send(e))));
                    try!(sock_db_tx.send_msg(try!(encode_into_msg!(Req::Local(LocalReq::Stop))), 0).map_err(|e| Error::Zmq(ZmqError::Send(e))));
                    stop_state = StopState::WaitingPqAndDb(req_frames);
                },
                Decision::JustReply(rep) =>
                    try!(proto_reply(sock_ext, req_frames, rep)),
            }
        }

        if pollitems[1].get_revents() == zmq::POLLIN {
            // sock_db is online
            enum Decision {
                PassPqAddAndReply(u32),
                DoNothing,
                TransmitExt,
            }

            let frames = try!(Frames::recv(sock_db_rx));
            let decision = match Rep::decode(&frames) {
                Ok(Rep::Local(LocalRep::Added(id))) =>
                    Decision::PassPqAddAndReply(id),
                Ok(Rep::Local(LocalRep::Stopped)) => {
                    stop_state = match stop_state {
                        StopState::NotTriggered | StopState::WaitingPq(..) | StopState::Finished(..) => unreachable!(),
                        StopState::WaitingPqAndDb(stop_frames) => StopState::WaitingPq(stop_frames),
                        StopState::WaitingDb(stop_frames) => StopState::Finished(stop_frames),
                    };
                    Decision::DoNothing
                },
                Ok(Rep::Local(LocalRep::Lent(..))) =>
                    panic!("unexpected LocalRep::Lend reply from worker_db"),
                Ok(Rep::Local(LocalRep::Panicked(msg))) =>
                    panic!("worker_db panic'd with message: {}", msg),
                Ok(Rep::GlobalOk(..)) | Ok(Rep::GlobalErr(..)) =>
                    Decision::TransmitExt,
                Err(err) =>
                    panic!("failed to decode worker_db reply: {:?}, error: {:?}", frames.deref(), err),
            };

            match decision {
                Decision::PassPqAddAndReply(id) => {
                    try!(sock_pq_tx.send_msg(try!(encode_into_msg!(Req::Local(LocalReq::AddEnqueue(id)))), 0)
                         .map_err(|e| Error::Zmq(ZmqError::Send(e))));
                    try!(proto_reply(sock_ext, frames, Rep::GlobalOk(GlobalRep::Added(id))));
                },
                Decision::DoNothing =>
                    (),
                Decision::TransmitExt =>
                    try!(frames.transmit(sock_ext)),
            }
        }

        if pollitems[2].get_revents() == zmq::POLLIN {
            // sock_pq is online
            enum Decision {
                PassDbLoad(u32),
                DoNothing,
                TransmitExt,
            }

            let frames = try!(Frames::recv(sock_pq_rx));
            let decision = match Rep::decode(&frames) {
                Ok(Rep::Local(LocalRep::Lent(id))) =>
                    Decision::PassDbLoad(id),
                Ok(Rep::Local(LocalRep::Stopped)) => {
                    stop_state = match stop_state {
                        StopState::NotTriggered | StopState::WaitingDb(..) | StopState::Finished(..) => unreachable!(),
                        StopState::WaitingPqAndDb(stop_frames) => StopState::WaitingDb(stop_frames),
                        StopState::WaitingPq(stop_frames) => StopState::Finished(stop_frames),
                    };
                    Decision::DoNothing
                },
                Ok(Rep::Local(LocalRep::Added(..))) =>
                    panic!("unexpected LocalRep::Add reply from worker_pq"),
                Ok(Rep::Local(LocalRep::Panicked(msg))) =>
                    panic!("worker_pq panic'd with message: {}", msg),
                Ok(Rep::GlobalOk(..)) | Ok(Rep::GlobalErr(..)) =>
                    Decision::TransmitExt,
                Err(err) =>
                    panic!("failed to decode worker_pq reply: {:?}, error: {:?}", frames.deref(), err),
            };

            match decision {
                Decision::PassDbLoad(id) =>
                    try!(proto_request(sock_db_tx, frames, Req::Local(LocalReq::Load(id)))),
                Decision::DoNothing =>
                    (),
                Decision::TransmitExt =>
                    try!(frames.transmit(sock_ext)),
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
    extern crate zmq;

    use std::fs;
    use std::thread::spawn;
    use time::SteadyTime;
    use super::{db, pq, worker_db_entrypoint, worker_pq_entrypoint, master};
    use super::proto::{RepayStatus, Req, LocalReq, GlobalReq, Rep, LocalRep, GlobalRep};

    fn proto_request(sock: &mut zmq::Socket, req: Req) {
        let bytes_required = req.encode_len();
        let mut msg = zmq::Message::with_capacity(bytes_required).unwrap();
        req.encode(&mut msg);
        sock.send_msg(msg, 0).unwrap();
    }

    fn assert_request_reply(sock_tx: &mut zmq::Socket, sock_rx: &mut zmq::Socket, req: Req, rep: Rep) {
        proto_request(sock_tx, req);
        let rep_msg = sock_rx.recv_msg(0).unwrap();
        assert_eq!(Rep::decode(&rep_msg), Ok(rep));
    }

    fn with_worker<WF, MF>(base_addr: &str, worker_fn: WF, master_fn: MF)
        where WF: FnOnce(zmq::Socket, zmq::Socket) + Send + 'static, MF: FnOnce(&mut zmq::Socket, &mut zmq::Socket) + Send + 'static
    {
        let mut ctx = zmq::Context::new();
        let mut sock_master_slave_tx = ctx.socket(zmq::PUSH).unwrap();
        let mut sock_master_slave_rx = ctx.socket(zmq::PULL).unwrap();
        let mut sock_slave_master_tx = ctx.socket(zmq::PUSH).unwrap();
        let mut sock_slave_master_rx = ctx.socket(zmq::PULL).unwrap();

        let tx_addr = format!("inproc://{}_txrx", base_addr);
        sock_master_slave_tx.bind(&tx_addr).unwrap();
        sock_slave_master_rx.connect(&tx_addr).unwrap();
        let rx_addr = format!("inproc://{}_rxtx", base_addr);
        sock_master_slave_rx.bind(&rx_addr).unwrap();
        sock_slave_master_tx.connect(&rx_addr).unwrap();

        let worker = spawn(move || worker_fn(sock_slave_master_tx, sock_slave_master_rx));
        master_fn(&mut sock_master_slave_tx, &mut sock_master_slave_rx);
        assert_request_reply(&mut sock_master_slave_tx, &mut sock_master_slave_rx,
                             Req::Local(LocalReq::Stop),
                             Rep::Local(LocalRep::Stopped));
        worker.join().unwrap();
    }

    #[test]
    fn pq_worker() {
        let pq = pq::PQueue::new(3);
        with_worker(
            "pq",
            move |sock_pq_master_tx, sock_pq_master_rx| worker_pq_entrypoint(sock_pq_master_tx, sock_pq_master_rx, pq),
            move |sock_master_pq_tx, sock_master_pq_rx| {
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Count),
                                     Rep::GlobalOk(GlobalRep::Counted(3)));
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Lend { timeout: 10000, }),
                                     Rep::Local(LocalRep::Lent(0)));
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Count),
                                     Rep::GlobalOk(GlobalRep::Counted(2)));
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Lend { timeout: 10000, }),
                                     Rep::Local(LocalRep::Lent(1)));
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Count),
                                     Rep::GlobalOk(GlobalRep::Counted(1)));
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Repay(1, RepayStatus::Front)),
                                     Rep::GlobalOk(GlobalRep::Repaid));
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Count),
                                     Rep::GlobalOk(GlobalRep::Counted(2)));
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Lend { timeout: 10000, }),
                                     Rep::Local(LocalRep::Lent(1)));
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Count),
                                     Rep::GlobalOk(GlobalRep::Counted(1)));
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Lend { timeout: 1000, }),
                                     Rep::Local(LocalRep::Lent(2)));
                assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                     Req::Global(GlobalReq::Count),
                                     Rep::GlobalOk(GlobalRep::Counted(0)));
                {
                    let start = SteadyTime::now();
                    assert_request_reply(sock_master_pq_tx, sock_master_pq_rx,
                                         Req::Global(GlobalReq::Lend { timeout: 10000, }),
                                         Rep::Local(LocalRep::Lent(2)));
                    let interval = SteadyTime::now() - start;
                    assert_eq!(interval.num_seconds(), 1);
                }
            });
    }

    #[test]
    fn db_worker() {
        let path = "/tmp/spiderq_main";
        let _ = fs::remove_dir_all(path);
        let db = db::Database::new(path).unwrap();
        with_worker(
            "pq",
            move |sock_db_master_tx, sock_db_master_rx| worker_db_entrypoint(sock_db_master_tx, sock_db_master_rx, db),
            move |sock_master_db_tx, sock_master_db_rx| {
                let user_data_0 = "some user data #0".as_bytes();

                assert_request_reply(sock_master_db_tx, sock_master_db_rx,
                                     Req::Global(GlobalReq::Add(None)),
                                     Rep::Local(LocalRep::Added(0)));
                assert_request_reply(sock_master_db_tx, sock_master_db_rx,
                                     Req::Global(GlobalReq::Add(Some(user_data_0))),
                                     Rep::Local(LocalRep::Added(1)));
                assert_request_reply(sock_master_db_tx, sock_master_db_rx,
                                     Req::Local(LocalReq::Load(0)),
                                     Rep::GlobalOk(GlobalRep::Lent(0, None)));
                assert_request_reply(sock_master_db_tx, sock_master_db_rx,
                                     Req::Local(LocalReq::Load(1)),
                                     Rep::GlobalOk(GlobalRep::Lent(1, Some(user_data_0))));
            });
    }

    fn assert_request_reply_ext(sock: &mut zmq::Socket, req: Req, rep: Rep) {
        proto_request(sock, req);
        let rep_msg = sock.recv_msg(0).unwrap();
        assert_eq!(Rep::decode(&rep_msg), Ok(rep));
    }

    #[test]
    fn full_server() {
        let path = "/tmp/spiderq_master";
        let _ = fs::remove_dir_all(path);
        let db = db::Database::new(path).unwrap();
        let pq = pq::PQueue::new(0);
        let mut ctx = zmq::Context::new();
        let mut sock_master_ext_peer = ctx.socket(zmq::REQ).unwrap();
        let mut sock_master_ext = ctx.socket(zmq::ROUTER).unwrap();
        let mut sock_master_db_tx = ctx.socket(zmq::PUSH).unwrap();
        let mut sock_master_db_rx = ctx.socket(zmq::PULL).unwrap();
        let mut sock_master_pq_tx = ctx.socket(zmq::PUSH).unwrap();
        let mut sock_master_pq_rx = ctx.socket(zmq::PULL).unwrap();
        let mut sock_db_master_tx = ctx.socket(zmq::PUSH).unwrap();
        let mut sock_db_master_rx = ctx.socket(zmq::PULL).unwrap();
        let mut sock_pq_master_tx = ctx.socket(zmq::PUSH).unwrap();
        let mut sock_pq_master_rx = ctx.socket(zmq::PULL).unwrap();

        sock_master_ext_peer.bind("inproc://test_master_ext_txrx").unwrap();
        sock_master_ext.connect("inproc://test_master_ext_txrx").unwrap();

        sock_master_db_tx.bind("inproc://test_master_db_txrx").unwrap();
        sock_db_master_rx.connect("inproc://test_master_db_txrx").unwrap();
        sock_master_db_rx.bind("inproc://test_master_db_rxtx").unwrap();
        sock_db_master_tx.connect("inproc://test_master_db_rxtx").unwrap();

        sock_master_pq_tx.bind("inproc://test_master_pq_txrx").unwrap();
        sock_pq_master_rx.connect("inproc://test_master_pq_txrx").unwrap();
        sock_master_pq_rx.bind("inproc://test_master_pq_rxtx").unwrap();
        sock_pq_master_tx.connect("inproc://test_master_pq_rxtx").unwrap();

        let worker_db = spawn(move || worker_db_entrypoint(sock_db_master_tx, sock_db_master_rx, db));
        let worker_pq = spawn(move || worker_pq_entrypoint(sock_pq_master_tx, sock_pq_master_rx, pq));
        let master_thread = spawn(move || master(&mut sock_master_ext,
                                                 &mut sock_master_db_tx,
                                                 &mut sock_master_db_rx,
                                                 &mut sock_master_pq_tx,
                                                 &mut sock_master_pq_rx).unwrap());

        let entry_a = "Entry A".as_bytes();
        let entry_b = "Entry B".as_bytes();
        let entry_c = "Entry C".as_bytes();

        // first fill
        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(0)));
        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Add(Some(entry_a))), Rep::GlobalOk(GlobalRep::Added(0)));
        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(1)));
        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Add(Some(entry_b))), Rep::GlobalOk(GlobalRep::Added(1)));
        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(2)));
        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Add(Some(entry_c))), Rep::GlobalOk(GlobalRep::Added(2)));
        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(3)));
        // lend one and repay it back
        assert_request_reply_ext(&mut sock_master_ext_peer,
                                 Req::Global(GlobalReq::Lend { timeout: 10000, }),
                                 Rep::GlobalOk(GlobalRep::Lent(0, Some(entry_a))));
        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(2)));
        assert_request_reply_ext(&mut sock_master_ext_peer,
                                 Req::Global(GlobalReq::Repay(0, RepayStatus::Front)),
                                 Rep::GlobalOk(GlobalRep::Repaid));
        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(3)));
        // lend all entries
        assert_request_reply_ext(&mut sock_master_ext_peer,
                                 Req::Global(GlobalReq::Lend { timeout: 10000, }),
                                 Rep::GlobalOk(GlobalRep::Lent(0, Some(entry_a))));
        assert_request_reply_ext(&mut sock_master_ext_peer,
                                 Req::Global(GlobalReq::Lend { timeout: 1000, }),
                                 Rep::GlobalOk(GlobalRep::Lent(1, Some(entry_b))));
        assert_request_reply_ext(&mut sock_master_ext_peer,
                                 Req::Global(GlobalReq::Lend { timeout: 10000, }),
                                 Rep::GlobalOk(GlobalRep::Lent(2, Some(entry_c))));
        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Global(GlobalReq::Count), Rep::GlobalOk(GlobalRep::Counted(0)));
        // check blocking
        {
            let start = SteadyTime::now();
            assert_request_reply_ext(&mut sock_master_ext_peer,
                                     Req::Global(GlobalReq::Lend { timeout: 10000, }),
                                     Rep::GlobalOk(GlobalRep::Lent(1, Some(entry_b))));
            let interval = SteadyTime::now() - start;
            assert_eq!(interval.num_seconds(), 1);
        }

        assert_request_reply_ext(&mut sock_master_ext_peer, Req::Local(LocalReq::Stop), Rep::Local(LocalRep::Stopped));
        master_thread.join().unwrap();
        worker_pq.join().unwrap();
        worker_db.join().unwrap();
    }
}

