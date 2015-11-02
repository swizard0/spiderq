#![feature(vec_resize, slice_bytes)]

extern crate zmq;
extern crate time;
extern crate getopts;
extern crate byteorder;

use std::{io, env, mem, process};
use std::io::Write;
use std::convert::From;
use std::thread::spawn;
use std::collections::VecDeque;
use getopts::Options;
use time::{SteadyTime, Duration};

pub mod db;
pub mod pq;
pub mod proto;
use proto::{Req, GlobalReq, LocalReq, Rep, GlobalRep, LocalRep, ProtoError};

#[derive(Debug)]
pub enum Error {
    Getopts(getopts::Fail),
    Db(db::Error),
    Zmq(zmq::Error),
}

impl From<db::Error> for Error {
    fn from(err: db::Error) -> Error {
        Error::Db(err)
    }
}

impl From<zmq::Error> for Error {
    fn from(err: zmq::Error) -> Error {
        Error::Zmq(err)
    }
}

fn entrypoint(maybe_matches: getopts::Result) -> Result<(), Error> {
    let matches = try!(maybe_matches.map_err(|e| Error::Getopts(e)));
    let database_dir = matches.opt_str("database").unwrap_or("./spiderq".to_owned());
    let zmq_addr = matches.opt_str("zmq-addr").unwrap_or("ipc://./spiderq.ipc".to_owned());

    let db = try!(db::Database::new(&database_dir).map_err(|e| Error::Db(e)));
    let pq = pq::PQueue::new(try!(db.count()));
    let mut ctx = zmq::Context::new();
    let mut sock_master_ext = try!(ctx.socket(zmq::ROUTER));
    let mut sock_master_db_tx = try!(ctx.socket(zmq::PUSH));
    let mut sock_master_db_rx = try!(ctx.socket(zmq::PULL));
    let mut sock_master_pq_tx = try!(ctx.socket(zmq::PUSH));
    let mut sock_master_pq_rx = try!(ctx.socket(zmq::PULL));
    let mut sock_db_master_tx = try!(ctx.socket(zmq::PUSH));
    let mut sock_db_master_rx = try!(ctx.socket(zmq::PULL));
    let mut sock_pq_master_tx = try!(ctx.socket(zmq::PUSH));
    let mut sock_pq_master_rx = try!(ctx.socket(zmq::PULL));

    try!(sock_master_ext.bind(&zmq_addr));
    try!(sock_master_db_tx.bind("inproc://db_txrx"));
    try!(sock_db_master_rx.connect("inproc://db_txrx"));
    try!(sock_master_db_rx.bind("inproc://db_rxtx"));
    try!(sock_db_master_tx.connect("inproc://db_rxtx"));
    try!(sock_master_pq_tx.bind("inproc://pq_txrx"));
    try!(sock_pq_master_rx.connect("inproc://pq_txrx"));
    try!(sock_master_pq_rx.bind("inproc://pq_rxtx"));
    try!(sock_pq_master_tx.connect("inproc://pq_rxtx"));

    spawn(move || worker_db_entrypoint(sock_db_master_tx, sock_db_master_rx, db));
    spawn(move || worker_pq_entrypoint(sock_pq_master_tx, sock_pq_master_rx, pq));
    try!(master(sock_master_ext, sock_master_db_tx, sock_master_db_rx, sock_master_pq_tx, sock_master_pq_rx));
    
    Ok(())
}

pub fn proto_request(sock: &mut zmq::Socket, req: Req) -> Result<(), Error> {
    let bytes_required = req.encode_len();
    let mut msg = try!(zmq::Message::with_capacity(bytes_required));
    req.encode(&mut msg);
    try!(sock.send_msg(msg, 0));
    Ok(())
}

pub fn proto_reply(sock: &mut zmq::Socket, rep: Rep) -> Result<(), Error> {
    let bytes_required = rep.encode_len();
    let mut msg = try!(zmq::Message::with_capacity(bytes_required));
    rep.encode(&mut msg);
    try!(sock.send_msg(msg, 0));
    Ok(())
}

pub fn worker_db_entrypoint(mut sock_tx: zmq::Socket, mut sock_rx: zmq::Socket, db: db::Database)  {
    match worker_db(&mut sock_tx, &mut sock_rx, db) {
        Ok(()) => (),
        Err(e) => {
            let panic_str = format!("{:?}", e);
            proto_reply(&mut sock_tx, Rep::Local(LocalRep::Panic(&panic_str))).unwrap();
        },
    }
}

#[allow(unused_variables, unused_mut)]
fn worker_db(sock_tx: &mut zmq::Socket, sock_rx: &mut zmq::Socket, mut db: db::Database) -> Result<(), Error> {
    loop {
        let req_msg = try!(sock_rx.recv_msg(0));
        match Req::decode(&req_msg) {
            Ok(Req::Local(LocalReq::Stop)) => {
                try!(proto_reply(sock_tx, Rep::Local(LocalRep::StopAck)));
                return Ok(())
            },
            Ok(..) =>
                try!(proto_reply(sock_tx, Rep::GlobalErr(ProtoError::UnexpectedWorkerDbRequest))),
            Err(proto_err) =>
                try!(proto_reply(sock_tx, Rep::GlobalErr(proto_err))),
        }
    }
}

pub fn worker_pq_entrypoint(mut sock_tx: zmq::Socket, mut sock_rx: zmq::Socket, pq: pq::PQueue)  {
    match worker_pq(&mut sock_tx, &mut sock_rx, pq) {
        Ok(()) => (),
        Err(e) => {
            let panic_str = format!("{:?}", e);
            proto_reply(&mut sock_tx, Rep::Local(LocalRep::Panic(&panic_str))).unwrap();
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
        let avail = try!(zmq::poll(&mut pollitems, timeout));
        if (avail == 1) && (pollitems[0].get_revents() == zmq::POLLIN) {
            let req_msg = try!(sock_rx.recv_msg(0));
            pending_queue.push_front(req_msg);
        }

        let mut tmp_queue = VecDeque::new();
        enum Action<'a> {
            Reply(Rep<'a>),
            DoNothing,
            Reenqueue,
            Stop,
        }

        while let Some(msg) = pending_queue.pop_front() {
            let action = match Req::decode(&msg) {
                Ok(Req::Local(LocalReq::Stop)) =>
                    Action::Stop,
                Ok(Req::Global(GlobalReq::Count)) => {
                    let count = pq.len();
                    Action::Reply(Rep::GlobalOk(GlobalRep::Count(count)))
                },
                Ok(Req::Global(GlobalReq::Lend { timeout: t, })) => {
                    if let Some(id) = pq.top() {
                        try!(proto_reply(sock_tx, Rep::Local(LocalRep::Lend(id))));
                        let trigger_at = SteadyTime::now() + Duration::milliseconds(t as i64);
                        assert_eq!(pq.lend(trigger_at), Some(id));
                        Action::DoNothing
                    } else {
                        Action::Reenqueue
                    }
                },
                Ok(Req::Global(GlobalReq::Repay(id, status))) => {
                    try!(proto_reply(sock_tx, Rep::GlobalOk(GlobalRep::Repaid)));
                    pq.repay(id, status);
                    Action::DoNothing
                },
                Ok(Req::Local(LocalReq::AddEnqueue(expected_id))) => {
                    let new_id = pq.add();
                    assert_eq!(expected_id, new_id);
                    Action::DoNothing
                },
                Ok(..) =>
                    Action::Reply(Rep::GlobalErr(ProtoError::UnexpectedWorkerPqRequest)),
                Err(proto_err) =>
                    Action::Reply(Rep::GlobalErr(proto_err)),
            };

            match action {
                Action::Reply(reply) => {
                    try!(proto_reply(sock_tx, reply));
                },
                Action::DoNothing =>
                    (),
                Action::Reenqueue => {
                    tmp_queue.push_back(msg);
                },
                Action::Stop => {
                    try!(proto_reply(sock_tx, Rep::Local(LocalRep::StopAck)));
                    return Ok(());
                }
            }
        }

        mem::replace(&mut pending_queue, tmp_queue);
    }
}

#[allow(unused_variables, unused_mut)]
fn master(mut sock_ext: zmq::Socket,
          mut sock_db_tx: zmq::Socket,
          mut sock_db_rx: zmq::Socket,
          mut sock_pq_tx: zmq::Socket,
          mut sock_pq_rx: zmq::Socket) -> Result<(), Error> {

    Ok(())
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
    use super::{db, pq, worker_db_entrypoint, worker_pq_entrypoint, proto_request};
    use super::proto::{RepayStatus, Req, LocalReq, GlobalReq, Rep, LocalRep, GlobalRep};

    fn assert_request_reply(sock_tx: &mut zmq::Socket, sock_rx: &mut zmq::Socket, req: Req, rep: Rep) {
        proto_request(sock_tx, req).unwrap();
        let rep_msg = sock_rx.recv_msg(0).unwrap();
        assert_eq!(Rep::decode(&rep_msg), Ok(rep));
    }

    #[test]
    fn pq_worker() {
        let pq = pq::PQueue::new(3);
        let mut ctx = zmq::Context::new();
        let mut sock_master_pq_tx = ctx.socket(zmq::PUSH).unwrap();
        let mut sock_master_pq_rx = ctx.socket(zmq::PULL).unwrap();
        let mut sock_pq_master_tx = ctx.socket(zmq::PUSH).unwrap();
        let mut sock_pq_master_rx = ctx.socket(zmq::PULL).unwrap();

        sock_master_pq_tx.bind("inproc://pq_txrx").unwrap();
        sock_pq_master_rx.connect("inproc://pq_txrx").unwrap();
        sock_master_pq_rx.bind("inproc://pq_rxtx").unwrap();
        sock_pq_master_tx.connect("inproc://pq_rxtx").unwrap();

        let worker = spawn(move || worker_pq_entrypoint(sock_pq_master_tx, sock_pq_master_rx, pq));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Count),
                             Rep::GlobalOk(GlobalRep::Count(3)));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Lend { timeout: 10000, }),
                             Rep::Local(LocalRep::Lend(0)));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Count),
                             Rep::GlobalOk(GlobalRep::Count(2)));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Lend { timeout: 10000, }),
                             Rep::Local(LocalRep::Lend(1)));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Count),
                             Rep::GlobalOk(GlobalRep::Count(1)));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Repay(1, RepayStatus::Front)),
                             Rep::GlobalOk(GlobalRep::Repaid));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Count),
                             Rep::GlobalOk(GlobalRep::Count(2)));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Lend { timeout: 10000, }),
                             Rep::Local(LocalRep::Lend(1)));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Count),
                             Rep::GlobalOk(GlobalRep::Count(1)));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Lend { timeout: 1000, }),
                             Rep::Local(LocalRep::Lend(2)));
        assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                             Req::Global(GlobalReq::Count),
                             Rep::GlobalOk(GlobalRep::Count(0)));
        {
            let start = SteadyTime::now();
            assert_request_reply(&mut sock_master_pq_tx, &mut sock_master_pq_rx,
                                 Req::Global(GlobalReq::Lend { timeout: 10000, }),
                                 Rep::Local(LocalRep::Lend(2)));
            let interval = SteadyTime::now() - start;
            assert_eq!(interval.num_seconds(), 1);
        }

        proto_request(&mut sock_master_pq_tx, Req::Local(LocalReq::Stop)).unwrap();
        worker.join().unwrap();
    }

    #[test]
    fn db_worker() {
        let path = "/tmp/spiderq_main";
        let _ = fs::remove_dir_all(path);
        let db = db::Database::new(path).unwrap();
        let mut ctx = zmq::Context::new();
        let mut sock_master_db_tx = ctx.socket(zmq::PUSH).unwrap();
        let mut sock_master_db_rx = ctx.socket(zmq::PULL).unwrap();
        let mut sock_db_master_tx = ctx.socket(zmq::PUSH).unwrap();
        let mut sock_db_master_rx = ctx.socket(zmq::PULL).unwrap();

        sock_master_db_tx.bind("inproc://db_txrx").unwrap();
        sock_db_master_rx.connect("inproc://db_txrx").unwrap();
        sock_master_db_rx.bind("inproc://db_rxtx").unwrap();
        sock_db_master_tx.connect("inproc://db_rxtx").unwrap();

        let worker = spawn(move || worker_db_entrypoint(sock_db_master_tx, sock_db_master_rx, db));

        proto_request(&mut sock_master_db_tx, Req::Local(LocalReq::Stop)).unwrap();
        worker.join().unwrap();
    }
}

