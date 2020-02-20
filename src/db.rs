use std::{io, fs, mem};
use std::io::Read;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{sync_channel, Receiver, TryRecvError};
use std::thread::{spawn, JoinHandle};
use std::collections::BTreeMap;
use std::collections::btree_map;
use std::iter::Iterator;
use tempdir::TempDir;
use byteorder::{ReadBytesExt, WriteBytesExt, NativeEndian};
use super::proto::{Key, Value};

#[derive(Clone)]
enum ValueSlot {
    Value(Value),
    Tombstone,
}

type Index = BTreeMap<Key, ValueSlot>;

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

    fn lookup(&self, key: &Key) -> Option<&ValueSlot> {
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

    fn indices_refs<'a, 'b>(&'a self, refs: &'b mut Vec<&'a Index>) {
        match self {
            &Snapshot::Memory(ref idx) => refs.push(&*idx),
            &Snapshot::Frozen(ref idx) => refs.push(&*idx),
            &Snapshot::Persisting { index: ref idx, .. } => refs.push(&*idx),
            &Snapshot::Persisted(ref idx) => refs.push(&*idx),
            &Snapshot::Merging { indices: ref idxs, .. } => {
                for idx in idxs.iter() {
                    refs.push(&*idx)
                }
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
                fs::create_dir(database_dir).map_err(Error::DatabaseMkdir)?,
            Err(e) => return Err(Error::DatabaseStat(e)),
        }

        let mut snapshots = vec![Snapshot::Memory(Index::new())];
        if let Some(persisted_index) = load_index(database_dir)? {
            snapshots.push(Snapshot::Persisted(Arc::new(persisted_index)));
        }

        Ok(Database {
            database_dir: Arc::new(PathBuf::from(database_dir)),
            flush_limit: flush_limit,
            snapshots: snapshots,
        })
    }

    pub fn approx_count(&self) -> usize {
        self.snapshots.iter().fold(0, |total, snapshot| total + snapshot.count())
    }

    pub fn lookup(&self, key: &Key) -> Option<&Value> {
        for snapshot in self.snapshots.iter() {
            match snapshot.lookup(key) {
                Some(&ValueSlot::Value(ref value)) =>
                    return Some(value),
                Some(&ValueSlot::Tombstone) =>
                    return None,
                None =>
                    (),
            }
        }

        None
    }

    pub fn insert(&mut self, key: Key, value: Value) {
        self.insert_slot(key, ValueSlot::Value(value))
    }

    pub fn remove(&mut self, key: Key) {
        self.insert_slot(key, ValueSlot::Tombstone)
    }

    fn insert_slot(&mut self, key: Key, slot: ValueSlot) {
        if let Some(&mut Snapshot::Memory(ref mut idx)) = self.snapshots.first_mut() {
            idx.insert(key, slot);
        } else {
            panic!("unexpected snapshots layout");
        }

        self.update_snapshots(false);
    }

    pub fn flush(&mut self) {
        self.update_snapshots(true);
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
            enum MergeLayout { FirstMemory, AtLeastOneFrozen, MaybeMoreFrozen, LastPersisted, }
            let merge_decision =
                self.snapshots.iter().fold(Some(MergeLayout::FirstMemory), |state, snapshot| match (state, snapshot) {
                    (Some(MergeLayout::FirstMemory), &Snapshot::Memory(..)) => Some(MergeLayout::AtLeastOneFrozen),
                    (Some(MergeLayout::AtLeastOneFrozen), &Snapshot::Frozen(..)) => Some(MergeLayout::MaybeMoreFrozen),
                    (Some(MergeLayout::MaybeMoreFrozen), &Snapshot::Frozen(..)) => Some(MergeLayout::MaybeMoreFrozen),
                    (Some(MergeLayout::MaybeMoreFrozen), &Snapshot::Persisted(..)) => Some(MergeLayout::LastPersisted),
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
                        mem::replace(merging_snapshot, Snapshot::Frozen(Arc::new(merged_index))) {
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

    pub fn iter<'a>(&'a self) -> Iter<'a> {
        let mut indices = Vec::new();
        for snapshot in self.snapshots.iter() {
            snapshot.indices_refs(&mut indices);
        }

        Iter {
            indices: indices,
            index: 0,
            iter: None,
        }
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        self.flush();
    }
}

pub struct Iter<'a> {
    indices: Vec<&'a Index>,
    index: usize,
    iter: Option<btree_map::Iter<'a, Key, ValueSlot>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a Key, &'a Value);

    fn next(&mut self) -> Option<(&'a Key, &'a Value)> {
        while self.index < self.indices.len() {
            if let Some(ref mut map_iter) = self.iter {
                while let Some((key, slot)) = map_iter.next() {
                    if self.indices[0 .. self.index]
                        .iter()
                        .rev()
                        .any(|older_index| older_index.contains_key(key)) {
                            continue
                        }

                    match slot {
                        &ValueSlot::Value(ref value) =>
                            return Some((key, value)),
                        &ValueSlot::Tombstone =>
                            continue,
                    }
                }
            } else {
                self.iter = Some(self.indices[self.index].iter());
                continue;
            }

            self.iter = None;
            self.index += 1;
        }

        None
    }
}


fn filename_as_string(filename: &PathBuf) -> String {
    filename.to_string_lossy().into_owned()
}

fn read_vec<R>(source: &mut R) -> Result<Vec<u8>, Error> where R: io::Read {
    let len = source.read_u32::<NativeEndian>().map_err(|e| Error::DatabaseRead(From::from(e)))? as usize;
    let mut buffer = Vec::with_capacity(len);
    let source_ref = source.by_ref();
    match source_ref.take(len as u64).read_to_end(&mut buffer) {
        Ok(bytes_read) if bytes_read == len => Ok(buffer),
        Ok(_) => Err(Error::DatabaseUnexpectedEof),
        Err(e) => Err(Error::DatabaseRead(e)),
    }
}

fn load_index(database_dir: &str) -> Result<Option<Index>, Error> {
    let mut db_file = PathBuf::new();
    db_file.push(database_dir);
    db_file.push("snapshot");

    match fs::File::open(&db_file) {
        Ok(file) => {
            let mut source = io::BufReader::new(file);
            let total = source.read_u64::<NativeEndian>().map_err(|e| Error::DatabaseRead(From::from(e)))? as usize;
            let mut index = Index::new();
            for _ in 0 .. total {
                let (key, value) = (read_vec(&mut source)?, read_vec(&mut source)?);
                index.insert(Arc::new(key), ValueSlot::Value(Arc::new(value)));
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
    target.write_u32::<NativeEndian>(value.len() as u32).map_err(|e| Error::DatabaseWrite(From::from(e)))?;
    target.write_all(&value[..]).map_err(Error::DatabaseWrite)?;
    Ok(())
}

fn persist(dir: Arc<PathBuf>, index: Arc<Index>) -> Result<(), Error> {
    let db_filename = "snapshot";
    let tmp_dir = TempDir::new_in(&*dir, "snapshot").map_err(Error::DatabaseMkdir)?;
    let mut tmp_db_file = PathBuf::new();
    tmp_db_file.push(tmp_dir.path());
    tmp_db_file.push(db_filename);

    let approx_len = index.len();
    let mut actual_len = 0;

    {
        let mut file = io::BufWriter::new(
            fs::File::create(&tmp_db_file).map_err(|e| Error::DatabaseTmpFile(filename_as_string(&tmp_db_file), e))?);
        file.write_u64::<NativeEndian>(approx_len as u64).map_err(|e| Error::DatabaseWrite(From::from(e)))?;
        for (key, slot) in &*index {
            if let &ValueSlot::Value(ref value) = slot {
                write_vec(key, &mut file)?;
                write_vec(value, &mut file)?;
                actual_len += 1;
            }
        }
    }

    if actual_len != approx_len {
        let mut file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&tmp_db_file)
            .map_err(|e| Error::DatabaseTmpFile(filename_as_string(&tmp_db_file), e))?;
        file.write_u64::<NativeEndian>(actual_len as u64).map_err(|e| Error::DatabaseWrite(From::from(e)))?;
    }

    let mut db_file = PathBuf::new();
    db_file.push(&*dir);
    db_file.push(db_filename);

    fs::rename(&tmp_db_file, &db_file).map_err(|e| Error::DatabaseMove(filename_as_string(&tmp_db_file), filename_as_string(&db_file), e))
}

fn merge(indices: Arc<Vec<Arc<Index>>>) -> Index {
    let mut base_index: Option<Index> = None;
    for index in indices.iter().rev() {
        if let Some(ref mut base) = base_index {
            for (key, slot) in &**index {
                base.insert(key.clone(), slot.clone());
            }
        } else {
            base_index = Some((**index).clone());
        }
    }

    base_index.take().unwrap()
}

#[cfg(test)]
mod test {
    use std::fs;
    use std::sync::Arc;
    use std::collections::HashMap;
    use rand::{thread_rng, sample, Rng};
    use super::{Database};
    use super::super::proto::{Key, Value};

    fn mkdb(path: &str, flush_limit: usize) -> Database {
        let _ = fs::remove_dir_all(path);
        Database::new(path, flush_limit).unwrap()
    }

    fn rnd_kv() -> (Key, Value) {
        let mut rng = thread_rng();
        let key_len = rng.gen_range(1, 64);
        let value_len = rng.gen_range(1, 64);
        (Arc::new(sample(&mut rng, 0 .. 255, key_len)),
         Arc::new(sample(&mut rng, 0 .. 255, value_len)))
    }

    fn rnd_fill_check(db: &mut Database, check_table: &mut HashMap<Key, Value>, count: usize) {
        let mut to_remove = Vec::new();
        let remove_count = count / 2;
        for _ in 0 .. count + remove_count {
            let (k, v) = rnd_kv();
            check_table.insert(k.clone(), v.clone());
            db.insert(k.clone(), v.clone());
            if to_remove.len() < remove_count {
                to_remove.push(k.clone());
            }
        }

        for k in to_remove {
            check_table.remove(&k);
            db.remove(k);
        }

        check_against(db, check_table);
    }

    fn check_against(db: &Database, check_table: &HashMap<Key, Value>) {
        if db.approx_count() < check_table.len() {
            panic!("db.approx_count() == {} < check_table.len() == {}", db.approx_count(), check_table.len());
        }

        for (k, v) in check_table {
            assert_eq!(db.lookup(k), Some(v));
        }
    }

    #[test]
    fn make() {
        let db = mkdb("/tmp/spiderq_a", 10);
        assert_eq!(db.approx_count(), 0);
    }

    #[test]
    fn insert_lookup() {
        let mut db = mkdb("/tmp/spiderq_b", 16);
        assert_eq!(db.approx_count(), 0);
        let mut check_table = HashMap::new();
        rnd_fill_check(&mut db, &mut check_table, 10);
    }

    #[test]
    fn save_load() {
        let mut check_table = HashMap::new();
        {
            let mut db = mkdb("/tmp/spiderq_c", 16);
            assert_eq!(db.approx_count(), 0);
            rnd_fill_check(&mut db, &mut check_table, 10);
        }
        {
            let db = Database::new("/tmp/spiderq_c", 16).unwrap();
            assert_eq!(db.approx_count(), 10);
            check_against(&db, &check_table);
        }
    }

    #[test]
    fn stress() {
        let mut check_table = HashMap::new();
        {
            let mut db = mkdb("/tmp/spiderq_d", 160);
            rnd_fill_check(&mut db, &mut check_table, 2560);
        }
        {
            let db = Database::new("/tmp/spiderq_d", 160).unwrap();
            assert!(db.approx_count() <= 2560);
            check_against(&db, &check_table);
        }
    }

    #[test]
    fn iter() {
        let mut check_table = HashMap::new();
        {
            let mut db = mkdb("/tmp/spiderq_e", 64);
            rnd_fill_check(&mut db, &mut check_table, 1024);
            for (k, v) in db.iter() {
                assert_eq!(check_table.get(k), Some(v));
            }
        }
        {
            let db = Database::new("/tmp/spiderq_e", 64).unwrap();
            for (k, v) in db.iter() {
                assert_eq!(check_table.get(k), Some(v));
            }
        }
    }
}
