#![feature(vec_resize, slice_bytes)]

extern crate zmq;
extern crate getopts;
extern crate byteorder;

use std::{io, env, process};
use std::io::Write;
use getopts::Options;

pub mod db;
pub mod pq;
pub mod proto;

#[derive(Debug)]
enum Error {
    Getopts(getopts::Fail),
    Db(db::Error),
}

fn entrypoint(maybe_matches: getopts::Result) -> Result<(), Error> {
    let matches = try!(maybe_matches.map_err(|e| Error::Getopts(e)));
    let database_dir = matches.opt_str("database").unwrap_or("./spiderq".to_owned());

    let _db = try!(db::Database::new(&database_dir).map_err(|e| Error::Db(e)));

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
        pq.repay(1, pq::RepayStatus::Reward);
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
        pq.repay(1, pq::RepayStatus::Penalty);
        assert_eq!(pq.lend(Duration::new(20, 0)), Some(1));
        assert_eq!(pq.next_timeout(), Some(Duration::new(10, 0)));
        pq.repay_timed_out();
        assert_eq!(pq.next_timeout(), Some(Duration::new(20, 0)));
    }
}

