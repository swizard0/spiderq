use std::io::Write;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::collections::hash_map::Entry;
use serde::{Serialize, Deserialize};
use super::proto::{Key, RepayStatus, AddMode};

#[derive(Debug)]
pub enum Error {
    DatabaseIsNotADir(String),
    DatabaseStat(std::io::Error),
    DatabaseMkdir(std::io::Error),
    LoadSnapshotError(std::io::Error),
    FlushError(std::io::Error),
    EncodingError(bincode::Error)
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Error::EncodingError(err)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
struct PQueueEntry {
    key: Key,
    priority: u64,
    boost: u32,
}

impl Ord for PQueueEntry {
    fn cmp(&self, other: &PQueueEntry) -> Ordering {
        other.priority.cmp(&self.priority)
    }
}

impl PartialOrd for PQueueEntry {
    fn partial_cmp(&self, other: &PQueueEntry) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Instant(u64);

use time::{OffsetDateTime, Duration};

impl Instant {
    pub fn to_be_bytes(&self) -> [u8; 8] {
        self.0.to_be_bytes()
    }

    pub fn now() -> Self {
        let d = OffsetDateTime::now_utc() - OffsetDateTime::unix_epoch();
        Self(d.whole_nanoseconds() as u64)
    }
}

impl PartialOrd for Instant {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Instant {
    fn cmp(&self, other: &Self) -> Ordering {
        other.0.cmp(&self.0)
    }
}

impl std::ops::Sub for Instant {
    type Output = Duration;

    fn sub(self, other: Self) -> Self::Output {
        Duration::nanoseconds(self.0 as i64) - Duration::nanoseconds(other.0 as i64)
    }
}

impl std::ops::Add<Duration> for Instant {
    type Output = Self;

    fn add(self, d: Duration) -> Self {
        let d = Duration::nanoseconds(self.0 as i64) + d;
        Self(d.whole_nanoseconds() as u64)
    }
}

#[derive(PartialEq, Eq, Serialize, Deserialize)]
struct LentEntry {
    trigger_at: Instant,
    key: Key,
    snapshot: u64,
}

#[derive(Serialize, Deserialize, Clone)]
struct LendSnapshot {
    entry: PQueueEntry,
    serial: u64,
    recycle: Option<Instant>,
}

impl Ord for LentEntry {
    fn cmp(&self, other: &LentEntry) -> Ordering {
        self.trigger_at.cmp(&other.trigger_at)
    }
}

impl PartialOrd for LentEntry {
    fn partial_cmp(&self, other: &LentEntry) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

use std::sync::{Arc, Mutex};

pub struct PQueue {
    serial: u64,
    queue: Arc<Mutex<BinaryHeap<PQueueEntry>>>,
    lentm: HashMap<Key, LendSnapshot>,
    lentq: BinaryHeap<LentEntry>,
    db_path: std::path::PathBuf,
    snapshot_counter: usize,
    snapshot_in_progress: Arc<Mutex<bool>>,
    count_before_snapshot: usize,
    last_snapshot_at: std::time::Instant
}

const QUEUE_DUMP_FILENAME: &str = "queue.dump";

impl PQueue {
    pub fn new(database_dir: &str) -> Result<PQueue, Error> {
        let mut db_path = std::path::PathBuf::from(database_dir);
        db_path.push("queue");

        match std::fs::metadata(&db_path) {
            Ok(ref metadata) if metadata.is_dir() => (),
            Ok(_) => return Err(Error::DatabaseIsNotADir(db_path.to_string_lossy().to_owned().to_string())),
            Err(ref e) if e.kind() == std::io::ErrorKind::NotFound =>
                std::fs::create_dir_all(&db_path).map_err(Error::DatabaseMkdir)?,
            Err(e) => return Err(Error::DatabaseStat(e)),
        }

        let snapshot_filename = db_path.join(QUEUE_DUMP_FILENAME);
        let queue = Arc::new(Mutex::new(BinaryHeap::new()));

        let serial = match std::fs::File::open(snapshot_filename) {
            Ok(file) => {
                let queue_copy = queue.clone();

                let reader = std::io::BufReader::new(file);
                let mut reader = snap::read::FrameDecoder::new(reader);
                let serial = bincode::deserialize_from::<_, u64>(&mut reader)?;

                std::thread::spawn(move || {
                    let mut buf = Vec::new();

                    loop {
                        match bincode::deserialize_from::<_, PQueueEntry>(&mut reader) {
                            Ok(next) => {
                                buf.push(next);

                                if buf.len() == 100000 {
                                    let mut q = match queue_copy.lock() {
                                        Ok(g) => g,
                                        Err(p) => p.into_inner()
                                    };
                                    for v in buf.drain(..) {
                                        q.push(v);
                                    }
                                }
                            }
                            Err(err) => {
                                match *err {
                                    bincode::ErrorKind::Io(_) => {}
                                    _ => {
                                        log::error!("failed to load snapshot: {}", err);
                                    }
                                }
                                break;
                            }
                        }
                    }

                    let mut q = match queue_copy.lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner()
                    };
                    for v in buf.drain(..) {
                        q.push(v);
                    }
                });

                serial
            }
            Err(_) => 1
        };

        eprintln!("serial = {}", serial);

        Ok(PQueue {
            serial,
            queue,
            lentm: HashMap::new(),
            lentq: BinaryHeap::new(),
            db_path,
            snapshot_counter: 0,
            snapshot_in_progress: Arc::new(Mutex::new(false)),
            count_before_snapshot: 0,
            last_snapshot_at: std::time::Instant::now()
        })
    }

    pub fn len(&self) -> usize {
        let in_progress = match self.snapshot_in_progress.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner()
        };

        if *in_progress {
            self.count_before_snapshot
        } else {
            let q = match self.queue.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner()
            };
            q.len()
        }
    }

    pub fn add(&mut self, key: Key, mode: AddMode, dump: bool) {
        self.serial += 1;

        {
            let mut q = match self.queue.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner()
            };

            q.push(PQueueEntry {
                key: key,
                priority: match mode { AddMode::Head => 0, AddMode::Tail => self.serial, },
                boost: 0,
            });
        }

        if dump {
            self.dump();
        }
    }

    pub fn top(&self) -> Option<(Key, u64)> {
        let q = match self.queue.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner()
        };

        q.peek().map(|e| (e.key.clone(), self.serial))
    }

    pub fn lend(&mut self, trigger_at: Instant) -> Option<u64> {
        let r = {
            let mut q = match self.queue.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner()
            };

            if let Some(entry) = q.pop() {
                self.lentq.push(LentEntry { trigger_at: trigger_at, key: entry.key.clone(), snapshot: self.serial, });
                self.lentm.insert(entry.key.clone(), LendSnapshot { serial: self.serial, recycle: None, entry: entry, });
                Some(self.serial)
            } else {
                None
            }
        };

        self.dump();

        r
    }

    pub fn next_timeout(&mut self) -> Option<Instant> {
        let r = self.do_next_timeout();

        self.dump();

        r
    }

    fn do_next_timeout(&mut self) -> Option<Instant> {
        loop {
            let do_recycle = if let Some(&LentEntry { trigger_at: trigger, key: ref k, snapshot: qshot, .. }) = self.lentq.peek() {
                match self.lentm.get_mut(k) {
                    Some(ref mut snapshot) if snapshot.recycle.is_some() =>
                        snapshot.recycle.take(),
                    Some(&mut LendSnapshot { serial: mshot, .. }) if mshot == qshot =>
                        return Some(trigger),
                    _ =>
                        None,
                }
            } else {
                break
            };

            self.lentq.pop().map(|mut entry| if let Some(reschedule) = do_recycle {
                entry.trigger_at = reschedule;
                self.lentq.push(entry);
            });
        }
        None
    }

    pub fn repay_timed_out(&mut self) {
        if let Some(LentEntry { key: k, snapshot: s, .. }) = self.lentq.pop() {
            self.repay(s, k, RepayStatus::Front);
        }
    }

    pub fn repay(&mut self, lend_key: u64, key: Key, status: RepayStatus) -> bool {
        let r = self.do_repay(lend_key, key, status);

        self.dump();

        r
    }

    fn do_repay(&mut self, lend_key: u64, key: Key, status: RepayStatus) -> bool {
        if let Entry::Occupied(map_entry) = self.lentm.entry(key) {
            if lend_key != map_entry.get().serial {
                return false
            }
            let LendSnapshot { mut entry, .. } = map_entry.remove();
            let mut q = match self.queue.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner()
            };
            let min_priority = if let Some(&PQueueEntry { priority: p, .. }) = q.peek() { p } else { 0 };
            let region = self.serial + 1 - min_priority;
            let current_boost = match status {
                RepayStatus::Penalty if entry.boost == 0 => 0,
                RepayStatus::Penalty => { entry.boost -= 1; entry.boost },
                RepayStatus::Reward if region >> (entry.boost + 1) == 0 => entry.boost,
                RepayStatus::Reward => { entry.boost += 1; entry.boost },
                RepayStatus::Front => 0,
                RepayStatus::Drop => return true,
            };
            self.serial += 1;
            entry.priority = match status {
                RepayStatus::Front => 0,
                _ => self.serial - (region - (region >> current_boost)),
            };

            q.push(entry);

            true
        } else {
            false
        }
    }

    pub fn heartbeat(&mut self, lend_key: u64, key: &Key, trigger_at: Instant) -> bool {
        if let Some(snapshot) = self.lentm.get_mut(key) {
            if lend_key == snapshot.serial {
                snapshot.recycle = Some(trigger_at);
                true
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn remove(&mut self, key: Key) {
        self.lentm.remove(&key);
        self.dump();
    }

    fn dump(&mut self) {
        self.snapshot_counter += 1;

        let time_to_snapshot = self.last_snapshot_at.elapsed() >= std::time::Duration::from_secs(30 * 60);
        let too_much_ops_to_snapshot = self.snapshot_counter == 1_000_000;
        let should_do_snapshot = time_to_snapshot || too_much_ops_to_snapshot;

        if should_do_snapshot {
            let mut in_progress = self.snapshot_in_progress.lock().unwrap();

            if !*in_progress {

                *in_progress = true;
                self.count_before_snapshot = {
                    let q = match self.queue.lock() {
                        Ok(g) => g,
                        Err(p) => p.into_inner()
                    };
                    q.len()
                };

                match self.flush() {
                    Ok(_handle) => {
                        self.snapshot_counter = 0;
                        self.last_snapshot_at = std::time::Instant::now();
                    }
                    Err(err) => {
                        *in_progress = false;
                        log::error!("failed to flush: {:?}", err);
                    }
                }
            }
        }
    }

    pub fn flush(&self) -> Result<std::thread::JoinHandle<()>, Error> {
        let proper_filename = self.db_path.join(QUEUE_DUMP_FILENAME);

        let cur_time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap();
        let new_queue_filename = format!("{}.{}", QUEUE_DUMP_FILENAME, cur_time.as_nanos());
        let new_queue_filename = self.db_path.join(new_queue_filename);

        let lentm_copy = self.lentm.clone();

        let old_heap = {
            let mut q = match self.queue.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner()
            };
            let queue_len = q.len();

            let new_heap = BinaryHeap::with_capacity(queue_len);
            std::mem::replace(&mut *q, new_heap)
        };

        let queue = self.queue.clone();
        let snapshot_in_progress = self.snapshot_in_progress.clone();
        let serial = self.serial;

        let snapshot_thread_handle = std::thread::spawn(move || {
            if let Err(err) = Self::mk_snapshot(
                old_heap, queue, serial, lentm_copy,
                &proper_filename, &new_queue_filename
            ) {
                log::error!("failed to create snapshot: {:?}", err);
            }

            let mut in_progress = match snapshot_in_progress.lock() {
                Ok(g) => g,
                Err(p) => p.into_inner()
            };

            *in_progress = false;
        });

        Ok(snapshot_thread_handle)
    }

    fn mk_snapshot(
        mut old_heap: BinaryHeap<PQueueEntry>,
        queue: Arc<Mutex<BinaryHeap<PQueueEntry>>>,
        serial: u64,
        mut lentm_copy: HashMap<Key, LendSnapshot>,
        proper_filename: &std::path::Path,
        new_queue_filename: &std::path::Path
    ) -> Result<(), Error> {
        let file = std::fs::File::create(&new_queue_filename).map_err(Error::FlushError)?;

        let writer = std::io::BufWriter::new(file);
        let mut writer = snap::write::FrameEncoder::new(writer);

        bincode::serialize_into::<_, u64>(&mut writer, &serial)?;

        for (_, v) in lentm_copy.drain() {
            bincode::serialize_into(&mut writer, &v.entry)?;
        }

        let mut buf = Vec::new();

        while let Some(next) = old_heap.pop() {
            bincode::serialize_into(&mut writer, &next)?;

            buf.push(next);

            if buf.len() == 100000 {
                let mut q = match queue.lock() {
                    Ok(g) => g,
                    Err(p) => p.into_inner()
                };
                for v in buf.drain(..) {
                    q.push(v);
                }
            }
        }

        let mut q = match queue.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner()
        };
        for v in buf.drain(..) {
            q.push(v);
        }

        writer.flush().map_err(Error::FlushError)?;
        std::fs::rename(new_queue_filename, proper_filename).map_err(Error::FlushError)?;

        Ok(())
    }
}

impl Drop for PQueue {
    fn drop(&mut self) {
        let start = std::time::Instant::now();

        let h = self.flush().unwrap();
        h.join().unwrap();

        eprintln!("flush took {} ms", start.elapsed().as_millis());
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use time::Duration;
    use super::{PQueue, Instant};
    use super::super::proto::{Key, RepayStatus, AddMode};

    fn as_key(value: usize) -> Key {
        Arc::from(format!("{}", value).as_bytes())
    }

    fn make_pq(len: usize, path: &str) -> PQueue {
        let _ = std::fs::remove_dir_all(path);
        let mut pq = PQueue::new(path).unwrap();
        for i in 0 .. len {
            pq.add(as_key(i), AddMode::Tail, false);
        }
        pq
    }

    fn assert_top(pq: &PQueue, sample: Key) {
        let (peek, _) = pq.top().unwrap();
        assert_eq!(peek, sample);
    }

    #[test]
    fn basic_api() {
        let time = Instant::now();
        let mut pq = make_pq(10, "/tmp/spider_pq_a"); // 0 1 2 3 4 5 6 7 8 9 |
        assert_top(&pq, as_key(0));
        let lend_key_0 = pq.lend(time + Duration::seconds(10)).unwrap(); // 1 2 3 4 5 6 7 8 9 | 0
        assert_top(&pq, as_key(1));
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(10)));
        let lend_key_1 = pq.lend(time + Duration::seconds(5)).unwrap(); // 2 3 4 5 6 7 8 9 | <1/5> <0/10>
        assert_top(&pq, as_key(2));
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(5)));
        assert!(pq.heartbeat(lend_key_0, &as_key(0), time + Duration::seconds(13))); // 2 3 4 5 6 7 8 9 | <1/5> <0/13>
        assert!(pq.heartbeat(lend_key_1, &as_key(1), time + Duration::seconds(7))); // 2 3 4 5 6 7 8 9 | <1/7> <0/13>
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(7)));
        assert!(pq.repay(lend_key_1, as_key(1), RepayStatus::Reward)); // 2 3 4 5 (1 6) 7 8 9 | <0/13>
        assert_top(&pq, as_key(2));
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(13)));
        pq.repay_timed_out(); // 0 2 3 4 5 (1 6) 7 8 9 |
        assert_eq!(pq.next_timeout(), None);
        assert_top(&pq, as_key(0));
        pq.lend(time + Duration::seconds(5)).unwrap(); // 2 3 4 5 (1 6) 7 8 9 | <0/5>
        assert_top(&pq, as_key(2));
        pq.lend(time + Duration::seconds(6)).unwrap(); // 3 4 5 (1 6) 7 8 9 | <0/10> <2/6>
        assert_top(&pq, as_key(3));
        pq.lend(time + Duration::seconds(7)).unwrap(); // 4 5 (1 6) 7 8 9 | <0/10> <2/6> <3/7>
        assert_top(&pq, as_key(4));
        pq.lend(time + Duration::seconds(8)).unwrap(); // 5 (1 6) 7 8 9 | <0/10> <2/6> <3/7> <4/8>
        assert_top(&pq, as_key(5));
        pq.lend(time + Duration::seconds(9)).unwrap(); // (1 6) 7 8 9 | <0/10> <2/6> <3/7> <4/8> <5/9>
        assert_top(&pq, as_key(6));
        pq.lend(time + Duration::seconds(10)).unwrap(); // 1 7 8 9 | <0/10> <2/6> <3/7> <4/8> <5/9> <6/10>
        assert_top(&pq, as_key(1));
        pq.lend(time + Duration::seconds(11)).unwrap(); // 7 8 9 | <0/10> <2/6> <3/7> <4/8> <5/9> <6/10> <1/11>
        assert_top(&pq, as_key(7));
        pq.lend(time + Duration::seconds(12)).unwrap(); // 8 9 | <0/10> <2/6> <3/7> <4/8> <5/9> <6/10> <1/11> <7/12>
        assert_top(&pq, as_key(8));
        pq.lend(time + Duration::seconds(13)).unwrap(); // 9 | <0/10> <2/6> <3/7> <4/8> <5/9> <6/10> <1/11> <7/12> <8/13>
        assert_top(&pq, as_key(9));
        pq.lend(time + Duration::seconds(14)).unwrap(); // | <0/10> <2/6> <3/7> <4/8> <5/9> <6/10> <1/11> <7/12> <8/13> <9/13>
        assert_eq!(pq.len(), 0);
    }

    #[test]
    fn double_lend() {
        let time = Instant::now();
        let mut pq = make_pq(2, "/tmp/spider_pq_b"); // 0 1 |
        assert_top(&pq, as_key(0));
        pq.lend(time + Duration::seconds(10)).unwrap(); // 1 | <0/10>
        assert_top(&pq, as_key(1));
        let lend_key_1 = pq.lend(time + Duration::seconds(15)).unwrap(); // | <0/10> <1/15>
        assert_eq!(pq.len(), 0);
        assert!(pq.repay(lend_key_1, as_key(1), RepayStatus::Penalty)); // 1 | <0/10>
        assert_top(&pq, as_key(1));
        pq.lend(time + Duration::seconds(20)).unwrap(); // | <0/10> <1/20>
        assert_eq!(pq.len(), 0);
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(10)));
        pq.repay_timed_out(); // 0 | <1/20>
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(20)));
        assert_top(&pq, as_key(0));
    }

    #[test]
    fn several_heartbeats() {
        let time = Instant::now();
        let mut pq = make_pq(3, "/tmp/spider_pq_c"); // 0 1 2 |
        assert_top(&pq, as_key(0));
        let lend_key_0 = pq.lend(time + Duration::seconds(10)).unwrap(); // 1 2 | <0/10>
        assert_top(&pq, as_key(1));
        let lend_key_1 = pq.lend(time + Duration::seconds(15)).unwrap(); // 2 | <0/10> <1/15>
        assert_top(&pq, as_key(2));
        pq.lend(time + Duration::seconds(20)).unwrap(); // | <0/10> <1/15> <2/20>
        assert_eq!(pq.len(), 0);
        assert!(pq.heartbeat(lend_key_1, &as_key(1), time + Duration::seconds(17))); // | <0/10> <1/17> <2/20>
        assert!(pq.heartbeat(lend_key_1, &as_key(1), time + Duration::seconds(18))); // | <0/10> <1/18> <2/20>
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(10)));
        assert!(pq.heartbeat(lend_key_0, &as_key(0), time + Duration::seconds(19))); // | <1/18> <0/19> <2/20>
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(18)));
        assert!(pq.heartbeat(lend_key_1, &as_key(1), time + Duration::seconds(21))); // | <0/19> <2/20> <1/21>
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(19)));
        assert!(pq.heartbeat(lend_key_0, &as_key(0), time + Duration::seconds(22))); // | <2/20> <1/21> <0/22>
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(20)));
        pq.repay_timed_out(); // 2 | <1/21> <0/22>
        assert_top(&pq, as_key(2));
        pq.lend(time + Duration::seconds(100)).unwrap(); // | <1/21> <0/22> <2/100>
        pq.repay_timed_out(); // 1 | <0/22> <2/100>
        assert_top(&pq, as_key(1));
        pq.lend(time + Duration::seconds(101)).unwrap(); // | <0/22> <2/100> <1/101>
        pq.repay_timed_out(); // 0 | <2/100> <1/101>
        assert_top(&pq, as_key(0));
    }
}
