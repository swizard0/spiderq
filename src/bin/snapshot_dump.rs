extern crate spiderq;
extern crate getopts;

use std::{io, env, str, process};
use std::io::Write;
use getopts::Options;

use spiderq::db;

#[derive(Debug)]
pub enum Error {
    Getopts(getopts::Fail),
    Db(db::Error),
}

pub fn entrypoint(maybe_matches: getopts::Result) -> Result<(), Error> {
    let matches = maybe_matches.map_err(Error::Getopts)?;
    let database_dir = matches.opt_str("database").unwrap_or("./spiderq".to_owned());
    let db = db::Database::new(&database_dir, 131072).map_err(Error::Db)?;

    for (key, value) in db.iter() {
        print_vec_u8(&key);
        print!("\t");
        print_vec_u8(&value);
        println!("");
    }

    Ok(())
}

fn print_vec_u8(data: &[u8]) {
    match str::from_utf8(data) {
        Ok(valid_string) =>
            print!("{}", valid_string),
        Err(..) =>
            print!("{:?}", data),
    }
}

fn main() {
    let mut args = env::args();
    let cmd_proc = args.next().unwrap();
    let mut opts = Options::new();

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
