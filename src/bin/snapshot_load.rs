use std::{io, env, process, fs::File};
use std::io::{Write, BufRead};
use getopts::Options;

use spiderq::{db, pq};
use spiderq_proto::{Key, AddMode};

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

    let file = File::open(snapshot_path)?;
    let lines = io::BufReader::new(file).lines();

    for line in lines {
        if let Ok(line) = line {
            let mut split_iter = line.splitn(2, '\t');
            let key = split_iter.next().unwrap().to_owned();
            let key = key.into_boxed_str();
            let key: Key = key.into_boxed_bytes().into();

            let value = split_iter.next().unwrap().to_owned();
            let value = value.into_boxed_str();
            let value = value.into_boxed_bytes().into();

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
