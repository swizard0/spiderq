use std::{io, fs, mem};
use std::ops::DerefMut;
use std::io::{Seek, SeekFrom, Read, Write};
use std::path::PathBuf;
use byteorder::{ReadBytesExt, WriteBytesExt, NativeEndian};

#[derive(Debug)]
pub enum Error {
    DatabaseIsNotADir(String),
    DatabaseStat(io::Error),
    DatabaseMkdir(io::Error),
    DatabaseFile(String, String, io::Error),
    Metadata(String, io::Error),
    Seek(String, io::Error),
    Read(String, io::Error),
    Write(String, io::Error),
    IndexIsTooBig { given: u32, total: u32, },
    EofReadingData { index: u32, len: usize, read: usize },
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

fn filename_as_string(filename: &PathBuf) -> String {
    filename.to_string_lossy().into_owned()
}

fn file_size(fd: &fs::File, filename: &PathBuf) -> Result<u64, Error> {
    let md = try!(fd.metadata().map_err(|e| Error::Metadata(filename_as_string(filename), e)));
    Ok(md.len())
}

pub trait Loader {
    fn set_len(&mut self, len: usize);
    fn contents(&mut self) -> &mut [u8];
}

impl Loader for Vec<u8> {
    fn set_len(&mut self, len: usize) {
        self.resize(len, 0)
    }

    fn contents(&mut self) -> &mut [u8] {
        self.deref_mut()
    }
}

impl Database {
    pub fn new(database_dir: &str) -> Result<Database, Error> {
        match fs::metadata(database_dir) {
            Ok(ref metadata) if metadata.is_dir() => (),
            Ok(_) => return Err(Error::DatabaseIsNotADir(database_dir.to_owned())),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound =>
                try!(fs::create_dir(database_dir).map_err(|e| Error::DatabaseMkdir(e))),
            Err(e) => return Err(Error::DatabaseStat(e)),
        }

        let (filename_idx, fd_idx) = try!(open_rw(database_dir, "spiderq.idx"));
        let (filename_db, fd_db) = try!(open_rw(database_dir, "spiderq.db"));
        Ok(Database {
            filename_idx: filename_idx,
            filename_db: filename_db,
            fd_idx: fd_idx,
            fd_db: fd_db,
        })
    }

    pub fn count(&self) -> Result<usize, Error> {
        Ok(try!(file_size(&self.fd_idx, &self.filename_idx)) as usize / mem::size_of::<u64>())
    }

    pub fn add(&mut self, data: &[u8]) -> Result<u32, Error> {
        let last_offset = try!(file_size(&self.fd_db, &self.filename_db));
        try!(self.fd_idx.seek(SeekFrom::End(0)).map_err(|e| Error::Seek(filename_as_string(&self.filename_idx), e)));
        try!(self.fd_idx.write_u64::<NativeEndian>(last_offset)
             .map_err(|e| Error::Write(filename_as_string(&self.filename_idx), From::from(e))));
        
        try!(self.fd_db.seek(SeekFrom::End(0)).map_err(|e| Error::Seek(filename_as_string(&self.filename_db), e)));
        try!(self.fd_db.write_u32::<NativeEndian>(data.len() as u32)
             .map_err(|e| Error::Write(filename_as_string(&self.filename_db), From::from(e))));
        try!(self.fd_db.write(data).map_err(|e| Error::Write(filename_as_string(&self.filename_db), e)));
        Ok(try!(self.count()) as u32 - 1)
    }

    pub fn load<'a, 'b, L>(&'a mut self, index: u32, loader: &'b mut L) -> Result<&'b L, Error> where L: Loader {
        let total = try!(self.count()) as u32;
        if index >= total {
            return Err(Error::IndexIsTooBig { given: index, total: total, })
        }

        try!(self.fd_idx.seek(SeekFrom::Start((index as usize * mem::size_of::<u64>()) as u64))
             .map_err(|e| Error::Seek(filename_as_string(&self.filename_idx), e)));
        let offset = try!(self.fd_idx.read_u64::<NativeEndian>()
                          .map_err(|e| Error::Read(filename_as_string(&self.filename_idx), From::from(e))));
        try!(self.fd_db.seek(SeekFrom::Start(offset))
             .map_err(|e| Error::Seek(filename_as_string(&self.filename_db), e)));
        let data_len = try!(self.fd_db.read_u32::<NativeEndian>()
                            .map_err(|e| Error::Read(filename_as_string(&self.filename_db), From::from(e)))) as usize;
        loader.set_len(data_len);

        {
            let mut target = loader.contents();
            while !target.is_empty() {
                match self.fd_db.read(target) {
                    Ok(0) if target.is_empty() => break,
                    Ok(0) => return Err(Error::EofReadingData { index: index, len: data_len, read: data_len - target.len(), }),
                    Ok(n) => { let tmp = target; target = &mut tmp[n ..]; },
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => { },
                    Err(e) => return Err(Error::Read(filename_as_string(&self.filename_db), e)),
                }
            }
        }

        Ok(loader)
    }
}

#[cfg(test)]
mod test {
    use std::fs;
    use super::{Database, Error};

    fn mkdb(path: &str) -> Database {
        let _ = fs::remove_dir_all(path);
        Database::new(path).unwrap()
    }

    #[test]
    fn make() {
        let db = mkdb("/tmp/spiderq_a");
        assert_eq!(db.count().unwrap(), 0);
    }

    #[test]
    fn reopen() {
        {
            let db = mkdb("/tmp/spiderq_b");
            assert_eq!(db.count().unwrap(), 0);
        }
        {
            let db = Database::new("/tmp/spiderq_b").unwrap();
            assert_eq!(db.count().unwrap(), 0);
        }
    }

    #[test]
    fn open_database_fail() {
        match Database::new("/qwe") {
            Ok(..) => panic!("expected fail"),
            Err(..) => (),
        }
    }

    fn mkfill(path: &str) -> Database {
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
            Err(Error::IndexIsTooBig { given: 3, total: 3, }) => (),
            other => panic!("unexpected Database::load return value: {:?}", other),
        }
    }
}

