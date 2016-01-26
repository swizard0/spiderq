use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::collections::hash_map::Entry;
use time::SteadyTime;
use super::proto::{Key, RepayStatus, AddMode};

#[derive(Debug, PartialEq, Eq)]
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

#[derive(PartialEq, Eq)]
struct LentEntry {
    trigger_at: SteadyTime,
    key: Key,
    snapshot: u64,
}

struct LendSnapshot {
    entry: PQueueEntry,
    serial: u64,
    recycle: Option<SteadyTime>,
}

impl Ord for LentEntry {
    fn cmp(&self, other: &LentEntry) -> Ordering {
        other.trigger_at.cmp(&self.trigger_at)
    }
}

impl PartialOrd for LentEntry {
    fn partial_cmp(&self, other: &LentEntry) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct PQueue {
    serial: u64,
    queue: BinaryHeap<PQueueEntry>,
    lentm: HashMap<Key, LendSnapshot>,
    lentq: BinaryHeap<LentEntry>,
}

impl PQueue {
    pub fn new() -> PQueue {
        PQueue {
            serial: 1,
            queue: BinaryHeap::new(),
            lentm: HashMap::new(),
            lentq: BinaryHeap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn add(&mut self, key: Key, mode: AddMode) {
        self.serial += 1;
        self.queue.push(PQueueEntry {
            key: key,
            priority: match mode { AddMode::Head => 0, AddMode::Tail => self.serial, },
            boost: 0,
        });
    }

    pub fn top(&self) -> Option<(Key, u64)> {
        self.queue.peek().map(|e| (e.key.clone(), self.serial))
    }

    pub fn lend(&mut self, trigger_at: SteadyTime) -> Option<u64> {
        if let Some(entry) = self.queue.pop() {
            self.lentq.push(LentEntry { trigger_at: trigger_at, key: entry.key.clone(), snapshot: self.serial, });
            self.lentm.insert(entry.key.clone(), LendSnapshot { serial: self.serial, recycle: None, entry: entry, });
            Some(self.serial)
        } else {
            None
        }
    }

    pub fn next_timeout(&mut self) -> Option<SteadyTime> {
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
        if let Entry::Occupied(map_entry) = self.lentm.entry(key) {
            if lend_key != map_entry.get().serial {
                return false
            }
            let LendSnapshot { mut entry, .. } = map_entry.remove();
            let min_priority = if let Some(&PQueueEntry { priority: p, .. }) = self.queue.peek() { p } else { 0 };
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
            self.queue.push(entry);
            true
        } else {
            false
        }
    }

    pub fn heartbeat(&mut self, lend_key: u64, key: &Key, trigger_at: SteadyTime) -> bool {
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
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use time::{SteadyTime, Duration};
    use super::PQueue;
    use super::super::proto::{Key, RepayStatus, AddMode};

    fn as_key(value: usize) -> Key {
        Arc::new(format!("{}", value).into_bytes())
    }

    fn make_pq(len: usize) -> PQueue {
        let mut pq = PQueue::new();
        for i in 0 .. len {
            pq.add(as_key(i), AddMode::Tail);
        }
        pq
    }

    fn assert_top(pq: &PQueue, sample: Key) {
        let (peek, _) = pq.top().unwrap();
        assert_eq!(peek, sample);
    }

    #[test]
    fn basic_api() {
        let time = SteadyTime::now();
        let mut pq = make_pq(10); // 0 1 2 3 4 5 6 7 8 9 |
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
        let time = SteadyTime::now();
        let mut pq = make_pq(2); // 0 1 |
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
        let time = SteadyTime::now();
        let mut pq = make_pq(3); // 0 1 2 |
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
