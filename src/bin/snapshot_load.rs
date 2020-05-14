use std::{io, env, process, fs::File};
use std::io::{Write, BufRead};
use getopts::Options;

use spiderq::{db, pq};
use spiderq_proto::{Key, Value, AddMode};

#[derive(Debug)]
pub enum Error {
    Getopts(getopts::Fail),
    Db(db::Error),
    Io(io::Error),
    Pq(pq::Error)
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

pub fn entrypoint(maybe_matches: getopts::Result) -> Result<(), Error> {
    let matches = maybe_matches.map_err(Error::Getopts)?;

    let database_dir = matches.opt_str("database").unwrap_or("./spiderq".to_owned());
    let mut db = db::Database::new(&database_dir).map_err(Error::Db)?;
    let mut queue = pq::PQueue::new(&database_dir).map_err(Error::Pq)?;

    let snapshot_path = matches.opt_str("snapshot").unwrap();
    println!("loading from: {}", snapshot_path);

    let lines_count: Option<usize> = matches.opt_get("number").unwrap();

    let file = File::open(snapshot_path)?;
    let lines = io::BufReader::new(file).lines();

    fn maybe_take<B>(lines: io::Lines<B>, count: Option<usize>) -> Box<dyn Iterator<Item = io::Result<String>>>
        where
            B: BufRead + 'static
    {
        match count {
            Some(n) => Box::new(lines.take(n)),
            None => Box::new(lines)
        }
    }

    for line in maybe_take(lines, lines_count) {
        if let Ok(line) = line {
            let mut split_iter = line.splitn(2, '\t');

            let key = split_iter.next().unwrap().to_owned();
            let key = key.into_boxed_str();
            let key: Key = key.into_boxed_bytes().into();

            let value = split_iter.next().unwrap().to_owned();
            let value = value.into_boxed_str();
            let value: Value = value.into_boxed_bytes().into();

            db.insert(key.clone(), value).unwrap();
            queue.add(key, AddMode::Tail).unwrap();
        }
    }

    Ok(())
}

fn main() {
    let mut args = env::args();
    let cmd_proc = args.next().unwrap();
    let mut opts = Options::new();

    opts.reqopt("s", "snapshot", "snapshot file path", "snapshot.dump");
    opts.optopt("d", "database", "database directory path (optional, default: ./spiderq)", "");
    opts.optopt("n", "number", "load this number of rows only (optional)", "");

    match entrypoint(opts.parse(args)) {
        Ok(()) =>
            (),
        Err(cause) => {
            let _ = writeln!(&mut io::stderr(), "Error: {:?}", cause);
            let usage = format!("Usage: {}", cmd_proc);
            let _ = writeln!(&mut io::stderr(), "{}", opts.usage(&usage[..]));
            process::exit(1);
        }
    }
}
