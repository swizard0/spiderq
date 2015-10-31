#![feature(vec_resize, slice_bytes)]

extern crate zmq;
extern crate getopts;
extern crate byteorder;

use std::{io, env, process};
use std::io::Write;
use std::convert::From;
use std::thread::spawn;
use getopts::Options;

pub mod db;
pub mod pq;
pub mod proto;
use proto::{RepayStatus, Req, GlobalReq, LocalReq, Rep, GlobalRep, LocalRep, ProtoError};

#[derive(Debug)]
enum Error {
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
    let mut sock_master_db = try!(ctx.socket(zmq::REQ));
    let mut sock_master_pq = try!(ctx.socket(zmq::REQ));
    let mut sock_db_master = try!(ctx.socket(zmq::REP));
    let mut sock_pq_master = try!(ctx.socket(zmq::REP));

    try!(sock_master_ext.bind(&zmq_addr));
    try!(sock_master_db.bind("inproc://db"));
    try!(sock_db_master.connect("inproc://db"));
    try!(sock_master_pq.bind("inproc://pq"));
    try!(sock_pq_master.connect("inproc://pq"));

    spawn(move || worker_db(sock_db_master, db).unwrap());
    spawn(move || worker_pq(sock_pq_master, pq).unwrap());
    try!(master(sock_master_ext, sock_master_db, sock_master_pq));
    
    Ok(())
}

fn proto_reply(sock: &mut zmq::Socket, rep: Rep) -> Result<(), Error> {
    let bytes_required = rep.encode_len();
    let mut msg = try!(zmq::Message::with_capacity(bytes_required));
    rep.encode(&mut msg);
    try!(sock.send_msg(msg, 0));
    Ok(())
}

fn worker_db(mut sock: zmq::Socket, mut db: db::Database) -> Result<(), Error> {
    loop {
        // let req_msg = try!(sock.recv_msg(0));
        // match Req::decode(&req_msg) {
        //     Ok(Req::Local(LocalReq::Stop)) => {
        //         try!(proto_reply(&mut sock, Rep::Local(LocalRep::StopAck)));
        //         break;
        //     },
            
        // }
    }
}

fn worker_pq(sock: zmq::Socket, pq: pq::PQueue) -> Result<(), Error> {
    loop {
        
    }
}

fn master(sock_ext: zmq::Socket, sock_db: zmq::Socket, sock_pq: zmq::Socket) -> Result<(), Error> {

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
    use std::fs;
    use std::time::Duration;
    use super::{db, pq};
    use super::proto::RepayStatus;

    fn mkdb(path: &str) -> db::Database {
        let _ = fs::remove_dir_all(path);
        db::Database::new(path).unwrap()
    }

    #[test]
    fn make_database() {
        let db = mkdb("/tmp/spiderq_a");
        assert_eq!(db.count().unwrap(), 0);
    }

    #[test]
    fn reopen_database() {
        {
            let db = mkdb("/tmp/spiderq_b");
            assert_eq!(db.count().unwrap(), 0);
        }
        {
            let db = db::Database::new("/tmp/spiderq_b").unwrap();
            assert_eq!(db.count().unwrap(), 0);
        }
    }

    #[test]
    fn open_database_fail() {
        match db::Database::new("/qwe") {
            Ok(..) => panic!("expected fail"),
            Err(..) => (),
        }
    }

    fn mkfill(path: &str) -> db::Database {
        let mut db = mkdb(path);
        assert_eq!(db.add(&[1, 2, 3]).unwrap(), 0);
        assert_eq!(db.add(&[4, 5, 6, 7]).unwrap(), 1);
        assert_eq!(db.add(&[8, 9]).unwrap(), 2);
        assert_eq!(db.count().unwrap(), 3);
        db
    }

    #[test]
    fn open_database_fill() {
        let _ = mkfill("/tmp/spiderq_c");
    }

    #[test]
    fn open_database_check() {
        let mut db = mkfill("/tmp/spiderq_d");
        let mut data = Vec::new();
        assert_eq!(db.load(0, &mut data).unwrap(), &[1, 2, 3]);
        assert_eq!(db.load(1, &mut data).unwrap(), &[4, 5, 6, 7]);
        assert_eq!(db.load(2, &mut data).unwrap(), &[8, 9]);
        match db.load(3, &mut data) {
            Err(db::Error::IndexIsTooBig { given: 3, total: 3, }) => (),
            other => panic!("unexpected Database::load return value: {:?}", other),
        }
    }

    #[test]
    fn pqueue_basic() {
        let mut pq = pq::PQueue::new(10);
        assert_eq!(pq.top(), Some(0));
        assert_eq!(pq.lend(Duration::new(10, 0)), Some(0));
        assert_eq!(pq.top(), Some(1));
        assert_eq!(pq.next_timeout(), Some(Duration::new(10, 0)));
        assert_eq!(pq.lend(Duration::new(5, 0)), Some(1));
        assert_eq!(pq.top(), Some(2));
        assert_eq!(pq.next_timeout(), Some(Duration::new(5, 0)));
        pq.repay(1, RepayStatus::Reward);
        assert_eq!(pq.top(), Some(2));
        assert_eq!(pq.next_timeout(), Some(Duration::new(10, 0)));
        pq.repay_timed_out();
        assert_eq!(pq.next_timeout(), None);
        assert_eq!(pq.lend(Duration::new(5, 0)), Some(0));
        assert_eq!(pq.lend(Duration::new(6, 0)), Some(2));
        assert_eq!(pq.lend(Duration::new(7, 0)), Some(3));
        assert_eq!(pq.lend(Duration::new(8, 0)), Some(4));
        assert_eq!(pq.lend(Duration::new(9, 0)), Some(5));
        assert_eq!(pq.lend(Duration::new(10, 0)), Some(6));
        assert_eq!(pq.lend(Duration::new(11, 0)), Some(7));
        assert_eq!(pq.lend(Duration::new(12, 0)), Some(8));
        assert_eq!(pq.lend(Duration::new(13, 0)), Some(1));
        assert_eq!(pq.lend(Duration::new(14, 0)), Some(9));
    }

    #[test]
    fn pqueue_double_lend() {
        let mut pq = pq::PQueue::new(2);
        assert_eq!(pq.lend(Duration::new(10, 0)), Some(0));
        assert_eq!(pq.lend(Duration::new(15, 0)), Some(1));
        pq.repay(1, RepayStatus::Penalty);
        assert_eq!(pq.lend(Duration::new(20, 0)), Some(1));
        assert_eq!(pq.next_timeout(), Some(Duration::new(10, 0)));
        pq.repay_timed_out();
        assert_eq!(pq.next_timeout(), Some(Duration::new(20, 0)));
    }
}

