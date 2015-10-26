use std::{io, fs, mem};
use std::path::PathBuf;

#[derive(Debug)]
pub enum Error {
    DatabaseIsNotADir(String),
    DatabaseStat(io::Error),
    DatabaseMkdir(io::Error),
    DatabaseFile(String, String, io::Error),
    Metadata(String, io::Error),
}

pub struct Database {
    filename_idx: PathBuf,
    filename_db: PathBuf,
    fd_idx: fs::File,
    fd_db: fs::File,
}

fn open_rw(database_dir: &str, filename: &str) -> Result<(PathBuf, fs::File), Error> {
    let mut full_filename = PathBuf::new();
    full_filename.push(database_dir);
    full_filename.push(filename);
    let file = 
        try!(fs::OpenOptions::new()
             .read(true)
             .write(true)
             .append(true)
             .create(true)
             .open(&full_filename)
             .map_err(|e| Error::DatabaseFile(database_dir.to_owned(), filename.to_owned(), e)));
    Ok((full_filename, file))
}

impl Database {
    pub fn new(database_dir: String) -> Result<Database, Error> {
        match fs::metadata(&database_dir) {
            Ok(ref metadata) if metadata.is_dir() => (),
            Ok(_) => return Err(Error::DatabaseIsNotADir(database_dir)),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound =>
                try!(fs::create_dir(&database_dir).map_err(|e| Error::DatabaseMkdir(e))),
            Err(e) => return Err(Error::DatabaseStat(e)),
        }

        let (filename_idx, fd_idx) = try!(open_rw(&database_dir, "spiderq.idx"));
        let (filename_db, fd_db) = try!(open_rw(&database_dir, "spiderq.db"));
        Ok(Database {
            filename_idx: filename_idx,
            filename_db: filename_db,
            fd_idx: fd_idx,
            fd_db: fd_db,
        })
    }

    pub fn count(&mut self) -> Result<usize, Error> {
        let md = try!(self.fd_idx.metadata().map_err(|e| Error::Metadata(self.filename_idx.to_string_lossy().into_owned(), e)));
        Ok(md.len() as usize / mem::size_of::<u64>())
    }

}

