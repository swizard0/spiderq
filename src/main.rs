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
}

