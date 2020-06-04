use serde::{Serialize, Deserialize, de::DeserializeOwned};
use super::proto::{Key, RepayStatus, AddMode};
use crate::system::{SledTree, System};

#[derive(Debug)]
pub enum Error {
    DatabaseDriverError(sled::Error),
    EncodingError(bincode::Error)
}

impl From<sled::Error> for Error {
    fn from(err: sled::Error) -> Self {
        Error::DatabaseDriverError(err)
    }
}

impl From<bincode::Error> for Error {
    fn from(err: bincode::Error) -> Self {
        Error::EncodingError(err)
    }
}

trait OrdKey {
    type Output: Serialize;

    fn key(&self) -> Self::Output;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PQueueEntry {
    key: Key,
    priority: u64,
    boost: u32,
}

impl OrdKey for PQueueEntry {
    type Output = ([u8; 8], Key);

    fn key(&self) -> Self::Output {
        (self.priority.to_be_bytes(), self.key.clone())
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

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
struct LentEntry {
    trigger_at: Instant,
    key: Key,
    snapshot: u64,
}

impl OrdKey for LentEntry {
    type Output = ([u8; 8], Key);

    fn key(&self) -> Self::Output {
        (self.trigger_at.to_be_bytes(), self.key.clone())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct LendSnapshot {
    entry: PQueueEntry,
    serial: u64,
    recycle: Option<Instant>,
}

struct Tree<K, V> {
    inner: sled::Tree,
    buf: Vec<u8>,
    key_marker: std::marker::PhantomData<K>,
    value_marker: std::marker::PhantomData<V>
}

impl<K, V> Tree<K, V> {
    fn new(inner: sled::Tree) -> Self {
        Tree {
            inner,
            buf: Vec::new(),
            key_marker: std::marker::PhantomData {},
            value_marker: std::marker::PhantomData {}
        }
    }
}

impl<K, V> Tree<K, V>
where
    K: AsRef<[u8]>,
    V: Serialize + DeserializeOwned
{
    fn insert(&mut self, key: K, value: V) -> Result<(), Error> {
        let start = std::time::Instant::now();

        bincode::serialize_into(&mut self.buf, &value)?;

        self.inner.insert(key, self.buf.as_slice())?;
        self.buf.clear();

        let end = std::time::Instant::now();
        metrics::timing!("pq.tree.insert_time", start, end);
        metrics::counter!("pq.tree.insert", 1);

        Ok(())
    }

    fn get(&self, key: &K) -> Result<Option<V>, Error> {
        let start = std::time::Instant::now();

        let r = self.inner
            .get(key)?
            .map(|v| bincode::deserialize(&v).map_err(Error::EncodingError))
            .transpose();

        let end = std::time::Instant::now();
        metrics::timing!("pq.tree.get_time", start, end);
        metrics::counter!("pq.tree.get", 1);

        r
    }

    fn remove(&self, key: &K) -> Result<Option<V>, Error> {
        let start = std::time::Instant::now();

        let r = self.inner
            .remove(key)?
            .map(|v| bincode::deserialize(&v).map_err(Error::EncodingError))
            .transpose();

        let end = std::time::Instant::now();
        metrics::timing!("pq.tree.remove_time", start, end);
        metrics::counter!("pq.tree.remove", 1);

        r
    }
}

struct TreeBag<V>
{
    inner: std::sync::Arc<sled::Tree>,
    marker: std::marker::PhantomData<V>,
    key_buf: Vec<u8>
}

impl<V> TreeBag<V>
where
    V: OrdKey + Clone + DeserializeOwned + Serialize
{
    fn new(tree: sled::Tree) -> Self {
        let inner = std::sync::Arc::new(tree);

        TreeBag {
            inner,
            marker: std::marker::PhantomData {},
            key_buf: Vec::new(),
        }
    }

    fn insert(&mut self, value: V) -> Result<Option<V>, Error> {
        let start = std::time::Instant::now();

        let k = value.key();
        bincode::serialize_into(&mut self.key_buf, &k)?;
        let value = bincode::serialize(&value)?;

        let r = self.inner.insert(&self.key_buf, value)?;
        let r = match r {
            Some(v) => Some(bincode::deserialize(&v)?),
            None => None
        };

        self.key_buf.clear();

        let end = std::time::Instant::now();
        metrics::timing!("pq.tree_bag.insert_time", start, end);
        metrics::counter!("pq.tree_bag.insert", 1);

        Ok(r)
    }

    fn top(&self) -> Result<Option<V>, Error> {
        let start = std::time::Instant::now();

        let maybe_top =
            self.inner
                .iter()
                .next();

        let r = match maybe_top {
            Some(r) => {
                let (_, v) = r?;
                let v: V = bincode::deserialize(&v)?;

                Ok(Some(v))
            }
            None => Ok(None)
        };

        let end = std::time::Instant::now();
        metrics::timing!("pq.tree_bag.top_time", start, end);
        metrics::counter!("pq.tree_bag.top", 1);

        r
    }

    fn pop(&mut self) -> Result<Option<V>, Error> {
        let start = std::time::Instant::now();
        let maybe_v = self.inner.pop_min()?;

        let r = match maybe_v {
            Some((_, v)) => {
                let v: V = bincode::deserialize(&v)?;

                Ok(Some(v))
            }
            None => Ok(None)
        };

        let end = std::time::Instant::now();
        metrics::timing!("pq.tree_bag.pop_time", start, end);
        metrics::counter!("pq.tree_bag.pop", 1);

        r
    }
}

pub struct PQueue {
    serial: u64,
    db_cfg: sled::Config,
    db: sled::Db,
    queue: TreeBag<PQueueEntry>,
    lentm: Tree<Key, LendSnapshot>,
    lentq: TreeBag<LentEntry>,
    system: System
}

impl PQueue {
    pub fn new(database_dir: &str) -> Result<PQueue, Error> {
        let mut db_path = std::path::PathBuf::from(database_dir);
        db_path.push("queue");

        let db_cfg = sled::Config::new()
            .path(db_path)
            .mode(sled::Mode::HighThroughput)
            .flush_every_ms(Some(10 * 60 * 1000));

        let start = std::time::Instant::now();

        let db = db_cfg.open()?;

        let end = std::time::Instant::now();
        metrics::timing!("pq.init_time", start, end);

        let queue_tree = db.open_tree("queue")?;
        let queue = TreeBag::new(queue_tree.clone());

        let lentm = db.open_tree("lentm")?;
        let lentm = Tree::new(lentm);

        let lentq = db.open_tree("lentq")?;
        let lentq = TreeBag::new(lentq);

        let system = db.open_tree("system")?;
        let system = System::new(system, SledTree::Tree(queue_tree), false);

        Ok(PQueue {
            serial: 1,
            lentm,
            queue,
            lentq,
            db,
            db_cfg,
            system
        })
    }

    pub fn len(&mut self) -> usize {
        self.system.count()
    }

    pub fn add(&mut self, key: Key, mode: AddMode) -> Result<(), Error> {
        self.serial += 1;

        let priority = match mode {
            AddMode::Head => 0,
            AddMode::Tail => self.serial
        };

        let value = PQueueEntry {
            key,
            priority,
            boost: 0,
        };

        match self.queue.insert(value)? {
            Some(_) => {}
            None => {
                self.system.incr();
            }
        }

        Ok(())
    }

    pub fn top(&self) -> Result<Option<(Key, u64)>, Error> {
        let maybe_top =
            self.queue
                .top()?
                .map(|e: PQueueEntry| (e.key.clone(), self.serial));

        Ok(maybe_top)
    }

    pub fn lend(&mut self, trigger_at: Instant) -> Result<Option<u64>, Error> {
        let start = std::time::Instant::now();

        let r = if let Some(entry) = self.queue.pop()? {
            // TODO: transaction
            let snapshot = LendSnapshot { serial: self.serial, recycle: None, entry: entry.clone() };
            self.lentm.insert(entry.key.clone(), snapshot)?;

            let lent_entry = LentEntry { trigger_at, key: entry.key, snapshot: self.serial };
            self.lentq.insert(lent_entry)?;

            Ok(Some(self.serial))
        } else {
            Ok(None)
        };

        self.system.decr();

        let end = std::time::Instant::now();
        metrics::timing!("pq.lend_time", start, end);
        metrics::counter!("pq.lend", 1);

        r
    }

    pub fn next_timeout(&mut self) -> Result<Option<Instant>, Error> {
        let start = std::time::Instant::now();

        let r = self.do_next_timeout();

        let end = std::time::Instant::now();
        metrics::timing!("pq.next_timeout_time", start, end);
        metrics::counter!("pq.next_timeout", 1);

        r
    }

    fn do_next_timeout(&mut self) -> Result<Option<Instant>, Error> {
        loop {
            let do_recycle =
                if let Some(LentEntry {
                    trigger_at,
                    key,
                    snapshot: qshot, ..
                }) = self.lentq.top()? {
                    let snapshot = self.lentm.get(&key);

                    match snapshot {
                        Ok(Some(mut snapshot)) if snapshot.recycle.is_some() => {
                            let v = snapshot.recycle.take();
                            self.lentm.insert(key, snapshot)?;

                            v
                        },
                        Ok(Some(LendSnapshot { serial: mshot, .. })) if mshot == qshot =>
                            return Ok(Some(trigger_at)),
                        _ =>
                            None,
                    }
                } else {
                    break
                };

            if let Some(mut entry) = self.lentq.pop()? {
                if let Some(reschedule) = do_recycle {
                    entry.trigger_at = reschedule;
                    self.lentq.insert(entry)?;
                }
            }
        }

        Ok(None)
    }

    pub fn repay_timed_out(&mut self) -> Result<(), Error> {
        let start = std::time::Instant::now();

        let entry = self.lentq.pop()?;
        if let Some(LentEntry { key, snapshot, .. }) = entry {
            self.repay(snapshot, key, RepayStatus::Front)?;
        }

        let end = std::time::Instant::now();
        metrics::timing!("pq.repay_timed_out_time", start, end);
        metrics::counter!("pq.repay_timed_out", 1);

        Ok(())
    }

    pub fn repay(&mut self, lend_key: u64, key: Key, status: RepayStatus) -> Result<bool, Error> {
        let start = std::time::Instant::now();

        let r = self.do_repay(lend_key, key, status);

        let end = std::time::Instant::now();
        metrics::timing!("pq.repay_timed_out_time", start, end);
        metrics::counter!("pq.repay_timed_out", 1);

        r
    }

    fn do_repay(&mut self, lend_key: u64, key: Key, status: RepayStatus) -> Result<bool, Error> {
        let entry = self.lentm.get(&key);

        if let Ok(Some(map_entry)) = entry {
            if lend_key != map_entry.serial {
                return Ok(false)
            }

            let LendSnapshot { mut entry, .. } = map_entry;
            self.lentm.remove(&key)?;

            let min_priority =
                self.queue
                    .top()?
                    .map(|e| e.priority)
                    .unwrap_or(0);

            let region = self.serial + 1 - min_priority;
            let current_boost = match status {
                RepayStatus::Penalty if entry.boost == 0 => 0,
                RepayStatus::Penalty => { entry.boost -= 1; entry.boost },
                RepayStatus::Reward if region >> (entry.boost + 1) == 0 => entry.boost,
                RepayStatus::Reward => { entry.boost += 1; entry.boost },
                RepayStatus::Front => 0,
                RepayStatus::Drop => return Ok(true),
            };
            self.serial += 1;
            self.system.incr();
            entry.priority = match status {
                RepayStatus::Front => 0,
                _ => self.serial - (region - (region >> current_boost)),
            };

            self.queue.insert(entry)?;

            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn heartbeat(&mut self, lend_key: u64, key: &Key, trigger_at: Instant) -> Result<bool, Error> {
        let start = std::time::Instant::now();

        let entry = self.lentm.remove(key);

        let r =
            if let Ok(Some(mut snapshot)) = entry {
                if lend_key == snapshot.serial {
                    snapshot.recycle = Some(trigger_at);
                    self.lentm.insert(key.clone(), snapshot)?;

                    true
                } else {
                    false
                }
            } else {
                false
            };

        let end = std::time::Instant::now();
        metrics::timing!("pq.heartbeat_time", start, end);
        metrics::counter!("pq.heartbeat", 1);

        Ok(r)
    }

    pub fn remove(&mut self, key: Key) -> Result<(), Error> {
        self.lentm.remove(&key)?;
        Ok(())
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
            pq.add(as_key(i), AddMode::Tail).unwrap();
        }
        pq
    }

    fn assert_top(pq: &PQueue, sample: Key) {
        let (peek, _) = pq.top().unwrap().unwrap();
        assert_eq!(peek, sample);
    }

    #[test]
    fn basic_api() {
        let time = Instant::now();
        let mut pq = make_pq(10, "/tmp/spider_pq_a"); // 0 1 2 3 4 5 6 7 8 9 |
        assert_top(&pq, as_key(0));
        let lend_key_0 = pq.lend(time + Duration::seconds(10)).unwrap().unwrap(); // 1 2 3 4 5 6 7 8 9 | <0/10>
        assert_top(&pq, as_key(1));
        assert_eq!(pq.next_timeout().unwrap(), Some(time + Duration::seconds(10)));
        let lend_key_1 = pq.lend(time + Duration::seconds(5)).unwrap().unwrap(); // 2 3 4 5 6 7 8 9 | <1/5> <0/10>
        assert_top(&pq, as_key(2));
        assert_eq!(pq.next_timeout().unwrap(), Some(time + Duration::seconds(5)));
        assert!(pq.heartbeat(lend_key_0, &as_key(0), time + Duration::seconds(13)).unwrap()); // 2 3 4 5 6 7 8 9 | <1/5> <0/13>
        assert!(pq.heartbeat(lend_key_1, &as_key(1), time + Duration::seconds(7)).unwrap()); // 2 3 4 5 6 7 8 9 | <1/7> <0/13>
        assert_eq!(pq.next_timeout().unwrap(), Some(time + Duration::seconds(7)));
        assert!(pq.repay(lend_key_1, as_key(1), RepayStatus::Reward).unwrap()); // 2 3 4 5 (1 6) 7 8 9 | <0/13>
        assert_top(&pq, as_key(2));
        assert_eq!(pq.next_timeout().unwrap(), Some(time + Duration::seconds(13)));
        pq.repay_timed_out(); // 0 2 3 4 5 (1 6) 7 8 9 |
        assert_eq!(pq.next_timeout().unwrap(), None);
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
        assert_top(&pq, as_key(1));
        pq.lend(time + Duration::seconds(10)).unwrap(); // 6 7 8 9 | <0/10> <2/6> <3/7> <4/8> <5/9> <1/10>
        assert_top(&pq, as_key(6));
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
        let lend_key_1 = pq.lend(time + Duration::seconds(15)).unwrap().unwrap(); // | <0/10> <1/15>
        assert_eq!(pq.len(), 0);
        assert!(pq.repay(lend_key_1, as_key(1), RepayStatus::Penalty).unwrap()); // 1 | <0/10>
        assert_top(&pq, as_key(1));
        pq.lend(time + Duration::seconds(20)).unwrap(); // | <0/10> <1/20>
        assert_eq!(pq.len(), 0);
        assert_eq!(pq.next_timeout().unwrap(), Some(time + Duration::seconds(10)));
        pq.repay_timed_out(); // 0 | <1/20>
        assert_eq!(pq.next_timeout().unwrap(), Some(time + Duration::seconds(20)));
        assert_top(&pq, as_key(0));
    }

    #[test]
    fn several_heartbeats() {
        let time = Instant::now();
        let mut pq = make_pq(3, "/tmp/spider_pq_c"); // 0 1 2 |
        assert_top(&pq, as_key(0));
        let lend_key_0 = pq.lend(time + Duration::seconds(10)).unwrap().unwrap(); // 1 2 | <0/10>
        assert_top(&pq, as_key(1));
        let lend_key_1 = pq.lend(time + Duration::seconds(15)).unwrap().unwrap(); // 2 | <0/10> <1/15>
        assert_top(&pq, as_key(2));
        pq.lend(time + Duration::seconds(20)).unwrap(); // | <0/10> <1/15> <2/20>
        assert_eq!(pq.len(), 0);
        assert!(pq.heartbeat(lend_key_1, &as_key(1), time + Duration::seconds(17)).unwrap()); // | <0/10> <1/17> <2/20>
        assert!(pq.heartbeat(lend_key_1, &as_key(1), time + Duration::seconds(18)).unwrap()); // | <0/10> <1/18> <2/20>
        assert_eq!(pq.next_timeout().unwrap(), Some(time + Duration::seconds(10)));
        assert!(pq.heartbeat(lend_key_0, &as_key(0), time + Duration::seconds(19)).unwrap()); // | <1/18> <0/19> <2/20>
        assert_eq!(pq.next_timeout().unwrap(), Some(time + Duration::seconds(18)));
        assert!(pq.heartbeat(lend_key_1, &as_key(1), time + Duration::seconds(21)).unwrap()); // | <0/19> <2/20> <1/21>
        assert_eq!(pq.next_timeout().unwrap(), Some(time + Duration::seconds(19)));
        assert!(pq.heartbeat(lend_key_0, &as_key(0), time + Duration::seconds(22)).unwrap()); // | <2/20> <1/21> <0/22>
        assert_eq!(pq.next_timeout().unwrap(), Some(time + Duration::seconds(20)));
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
