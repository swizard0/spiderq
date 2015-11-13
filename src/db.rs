use std::{io, fs, mem};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{sync_channel, Receiver, TryRecvError};
use std::thread::{spawn, JoinHandle};
use std::collections::HashMap;
use tempdir::TempDir;
use byteorder::{ReadBytesExt, WriteBytesExt, ByteOrder, NativeEndian};
use super::proto::{Key, Value};

type Index = HashMap<Key, Value>;

#[derive(Debug)]
pub enum Error {
    DatabaseIsNotADir(String),
    DatabaseStat(io::Error),
    DatabaseMkdir(io::Error),
    DatabaseTmpFile(String, io::Error),
    DatabaseFileOpen(String, io::Error),
    DatabaseWrite(io::Error),
    DatabaseMove(String, String, io::Error),
    DatabaseRead(io::Error),
    DatabaseUnexpectedEof,
}

enum Snapshot {
    Memory(Index),
    Frozen(Arc<Index>),
    Persisting { index: Arc<Index>,
                 chan: Receiver<Result<(), Error>>,
                 slave: JoinHandle<()>, },
    Persisted(Arc<Index>),
    Merging { indices: Arc<Vec<Arc<Index>>>,
              chan: Receiver<Index>,
              slave: JoinHandle<()>, },
}

impl Snapshot {
    fn count(&self) -> usize {
        match self {
            &Snapshot::Memory(ref idx) => idx.len(),
            &Snapshot::Frozen(ref idx) => idx.len(),
            &Snapshot::Persisting { index: ref idx, .. } => idx.len(),
            &Snapshot::Persisted(ref idx) => idx.len(),
            &Snapshot::Merging { indices: ref idxs, .. } => idxs.iter().fold(0, |total, idx| total + idx.len()),
        }
    }

    fn lookup(&self, key: &Key) -> Option<&Value> {
        match self {
            &Snapshot::Memory(ref idx) => idx.get(key),
            &Snapshot::Frozen(ref idx) => idx.get(key),
            &Snapshot::Persisting { index: ref idx, .. } => idx.get(key),
            &Snapshot::Persisted(ref idx) => idx.get(key),
            &Snapshot::Merging { indices: ref idxs, .. } => {
                for idx in idxs.iter() {
                    if let Some(value) = idx.get(key) {
                        return Some(value)
                    }
                }

                None
            }
        }
    }
}

pub struct Database {
    database_dir: Arc<PathBuf>,
    flush_limit: usize,
    snapshots: Vec<Snapshot>,
}

impl Database {
    pub fn new(database_dir: &str, flush_limit: usize) -> Result<Database, Error> {
        match fs::metadata(database_dir) {
            Ok(ref metadata) if metadata.is_dir() => (),
            Ok(_) => return Err(Error::DatabaseIsNotADir(database_dir.to_owned())),
            Err(ref e) if e.kind() == io::ErrorKind::NotFound =>
                try!(fs::create_dir(database_dir).map_err(|e| Error::DatabaseMkdir(e))),
            Err(e) => return Err(Error::DatabaseStat(e)),
        }

        let mut snapshots = vec![Snapshot::Memory(Index::new())];
        if let Some(persisted_index) = try!(load_index(database_dir)) {
            snapshots.push(Snapshot::Persisted(Arc::new(persisted_index)));
        }

        Ok(Database {
            database_dir: Arc::new(PathBuf::from(database_dir)),
            flush_limit: flush_limit,
            snapshots: snapshots,
        })
    }

    pub fn count(&self) -> usize {
        self.snapshots.iter().fold(0, |total, snapshot| total + snapshot.count())
    }

    pub fn lookup(&self, key: &Key) -> Option<&Value> {
        for snapshot in self.snapshots.iter() {
            if let Some(value) = snapshot.lookup(key) {
                return Some(value)
            }
        }

        None
    }

    pub fn insert(&mut self, key: Key, value: Value) {
        if let Some(&mut Snapshot::Memory(ref mut idx)) = self.snapshots.first_mut() {
            idx.insert(key, value);
        } else {
            panic!("unexpected snapshots layout");
        }
        self.update_snapshots(false);
    }

    fn update_snapshots(&mut self, flush_mode: bool) {
        loop {
            // Check if memory part overflowed
            if let Some(index_to_freeze) = match self.snapshots.first_mut() {
                Some(&mut Snapshot::Memory(ref mut idx)) if (idx.len() >= self.flush_limit) || (idx.len() != 0 && flush_mode) =>
                    Some(mem::replace(idx, Index::new())),
                _ =>
                    None,
            } {
                self.snapshots.insert(1, Snapshot::Frozen(Arc::new(index_to_freeze)));
                continue;
            }

            // Check if last snapshot is not persisted
            if let Some(last_snapshot) = self.snapshots.last_mut() {
                if let Some(index_to_persist) = match last_snapshot {
                    &mut Snapshot::Frozen(ref idx) => Some(idx.clone()),
                    _ => None,
                } {
                    let (tx, rx) = sync_channel(0);
                    let slave_dir = self.database_dir.clone();
                    let slave_index = index_to_persist.clone();
                    let slave = spawn(move || tx.send(persist(slave_dir, slave_index)).unwrap());
                    mem::replace(last_snapshot, Snapshot::Persisting { 
                        index: index_to_persist,
                        chan: rx,
                        slave: slave,
                    });
                    continue;
                }
            }

            // Check if several snapshots should be merged
            enum MergeLayout { FirstMemory, SomeFrozen, LastPersisted, }
            let merge_decision = 
                self.snapshots.iter().fold(Some(MergeLayout::FirstMemory), |state, snapshot| match (state, snapshot) {
                    (Some(MergeLayout::FirstMemory), &Snapshot::Memory(..)) => Some(MergeLayout::SomeFrozen),
                    (Some(MergeLayout::SomeFrozen), &Snapshot::Frozen(..)) => Some(MergeLayout::SomeFrozen),
                    (Some(MergeLayout::SomeFrozen), &Snapshot::Persisted(..)) => Some(MergeLayout::LastPersisted),
                    _ => None,
                });
            if let Some(MergeLayout::LastPersisted) = merge_decision {
                let indices: Vec<_> = self.snapshots.drain(1 ..)
                    .map(|snapshot| match snapshot {
                        Snapshot::Frozen(idx) => idx,
                        Snapshot::Persisted(idx) => idx,
                        _ => unreachable!(),
                    })
                    .collect();
                let master_indices = Arc::new(indices);
                let slave_indices = master_indices.clone();
                let (tx, rx) = sync_channel(0);
                let slave = spawn(move || tx.send(merge(slave_indices)).unwrap());
                self.snapshots.push(Snapshot::Merging {
                    indices: master_indices,
                    chan: rx,
                    slave: slave,
                });

                continue;
            }

            // Check if persisting is finished
            if let Some(persisting_snapshot) = self.snapshots.iter_mut().find(|snapshot| match snapshot {
                &&mut Snapshot::Persisting { .. } => true,
                _ => false,
            }) {
                let done_index =
                    if let &mut Snapshot::Persisting { index: ref idx, chan: ref rx, .. } = persisting_snapshot {
                        if !flush_mode {
                            match rx.try_recv() {
                                Ok(Ok(())) => Some(idx.clone()),
                                Ok(Err(e)) => panic!("persisting thread failed: {:?}", e),
                                Err(TryRecvError::Empty) => None,
                                Err(TryRecvError::Disconnected) => panic!("persisting thread is down"),
                            }
                        } else {
                            match rx.recv().unwrap() {
                                Ok(()) => Some(idx.clone()),
                                Err(e) => panic!("persisting thread failed: {:?}", e),
                            }
                        }
                    } else {
                        unreachable!()
                    };

                if let Some(persisted_index) = done_index {
                    if let Snapshot::Persisting { slave: thread, .. } =
                        mem::replace(persisting_snapshot, Snapshot::Persisted(persisted_index)) {
                            thread.join().unwrap();
                        } else {
                            unreachable!()
                        }

                    continue;
                }
            }

            // Check if merging is finished
            if let Some(merging_snapshot) = self.snapshots.iter_mut().find(|snapshot| match snapshot {
                &&mut Snapshot::Merging { .. } => true,
                _ => false,
            }) {
                let done_index =
                    if let &mut Snapshot::Merging { chan: ref rx, .. } = merging_snapshot {
                        if !flush_mode {
                            match rx.try_recv() {
                                Ok(merged_index) => Some(merged_index),
                                Err(TryRecvError::Empty) => None,
                                Err(TryRecvError::Disconnected) => panic!("merging thread is down"),
                            }
                        } else {
                            Some(rx.recv().unwrap())
                        }
                    } else {
                        unreachable!()
                    };

                if let Some(merged_index) = done_index {
                    if let Snapshot::Merging { slave: thread, .. } =
                        mem::replace(merging_snapshot, Snapshot::Persisted(Arc::new(merged_index))) {
                            thread.join().unwrap();
                        } else {
                            unreachable!()
                        }

                    continue;
                }
            }

            break;
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.update_snapshots(true);
    }
}

fn filename_as_string(filename: &PathBuf) -> String {
    filename.to_string_lossy().into_owned()
}

fn read_vec<R>(source: &mut R) -> Result<Option<Vec<u8>>, Error> where R: io::Read {
    let len = try!(source.read_u32::<NativeEndian>().map_err(|e| Error::DatabaseRead(From::from(e))));
    let source_ref = source.by_ref();
    let mut buffer = Vec::with_capacity(len as usize);
    match source_ref.take(len as u64).read_to_end(&mut buffer) {
        Ok(0) => Ok(None),
        Ok(_) => Ok(Some(buffer)),
        Err(e) => Err(Error::DatabaseRead(e)),
    }
}

fn load_index(database_dir: &str) -> Result<Option<Index>, Error> {
    let mut db_file = PathBuf::new();
    db_file.push(database_dir);
    db_file.push("snapshot");

    match fs::File::open(&db_file) {
        Ok(file) => {
            let mut index = Index::new();
            let mut source = io::BufReader::new(file);
            while let Some(key) = try!(read_vec(&mut source)) {
                if let Some(value) = try!(read_vec(&mut source)) {
                    index.insert(Arc::new(key), Arc::new(value));
                } else {
                    return Err(Error::DatabaseUnexpectedEof)
                }
            }
            Ok(Some(index))
        },
        Err(ref e) if e.kind() == io::ErrorKind::NotFound =>
            Ok(None),
        Err(e) =>
            return Err(Error::DatabaseFileOpen(filename_as_string(&db_file), e)),
    }
}

fn write_vec<W>(value: &Arc<Vec<u8>>, target: &mut W) -> Result<(), Error> where W: io::Write {
    try!(target.write_u32::<NativeEndian>(value.len() as u32).map_err(|e| Error::DatabaseWrite(From::from(e))));
    try!(target.write_all(&value[..]).map_err(|e| Error::DatabaseWrite(e)));
    Ok(())
}

fn persist(dir: Arc<PathBuf>, index: Arc<Index>) -> Result<(), Error> {
    let db_filename = "snapshot";
    let tmp_dir = try!(TempDir::new_in(&*dir, "snapshot").map_err(|e| Error::DatabaseMkdir(e)));
    let mut tmp_db_file = PathBuf::new();
    tmp_db_file.push(tmp_dir.path());
    tmp_db_file.push(db_filename);

    {
        let mut file = io::BufWriter::new(
            try!(fs::File::create(&tmp_db_file).map_err(|e| Error::DatabaseTmpFile(filename_as_string(&tmp_db_file), e))));
        for (key, value) in &*index {
            try!(write_vec(key, &mut file));
            try!(write_vec(value, &mut file));
        }
    }

    let mut db_file = PathBuf::new();
    db_file.push(&*dir);
    db_file.push(db_filename);

    fs::rename(&tmp_db_file, &db_file).map_err(|e| Error::DatabaseMove(filename_as_string(&tmp_db_file), filename_as_string(&db_file), e))
}

fn merge(indices: Arc<Vec<Arc<Index>>>) -> Index {
    let mut iter = indices.iter();
    let mut base_index = (**(iter.next().unwrap())).clone();
    for index in iter {
        for (key, value) in &**index {
            if !base_index.contains_key(key) {
                base_index.insert(key.clone(), value.clone());
            }
        }
    }
    
    base_index
}

#[cfg(test)]
mod test {
    use std::fs;
    use super::{Database, Error};
    
    fn mkdb(path: &str, flush_limit: usize) -> Database {
        let _ = fs::remove_dir_all(path);
        Database::new(path, flush_limit).unwrap()
    }

    #[test]
    fn make() {
        let db = mkdb("/tmp/spiderq_a", 10);
        assert_eq!(db.count(), 0);
    }

//     #[test]
//     fn reopen() {
//         {
//             let db = mkdb("/tmp/spiderq_b", false);
//             assert_eq!(db.count().unwrap(), 0);
//         }
//         {
//             let db = Database::new("/tmp/spiderq_b", false).unwrap();
//             assert_eq!(db.count().unwrap(), 0);
//         }
//     }

//     #[test]
//     fn open_database_fail() {
//         match Database::new("/qwe", false) {
//             Ok(..) => panic!("expected fail"),
//             Err(..) => (),
//         }
//     }

//     fn mkfill(path: &str, use_mem_cache: bool) -> Database {
//         let mut db = mkdb(path, use_mem_cache);
//         assert_eq!(db.add(&[1, 2, 3]).unwrap(), 0);
//         assert_eq!(db.add(&[4, 5, 6, 7]).unwrap(), 1);
//         assert_eq!(db.add(&[8, 9]).unwrap(), 2);
//         assert_eq!(db.count().unwrap(), 3);
//         db
//     }

//     #[test]
//     fn open_database_fill() {
//         let _ = mkfill("/tmp/spiderq_c", false);
//     }

//     #[test]
//     fn open_database_fill_cache() {
//         let _ = mkfill("/tmp/spiderq_d", true);
//     }

//     fn open_database_check_db(mut db: Database) {
//         let mut data = Vec::new();
//         db.load(0, &mut data).unwrap(); assert_eq!(&data, &[1, 2, 3]);
//         db.load(1, &mut data).unwrap(); assert_eq!(&data, &[4, 5, 6, 7]);
//         db.load(2, &mut data).unwrap(); assert_eq!(&data, &[8, 9]);
//         match db.load(3, &mut data) {
//             Err(LoadError::Db(Error::IndexIsTooBig { given: 3, total: 3, })) => (),
//             other => panic!("unexpected Database::load return value: {:?}", other),
//         }
//     }

//     #[test]
//     fn open_database_check() {
//         open_database_check_db(mkfill("/tmp/spiderq_e", false));
//     }

//     #[test]
//     fn open_database_check_cache() {
//         open_database_check_db(mkfill("/tmp/spiderq_f", true));
//     }
}

