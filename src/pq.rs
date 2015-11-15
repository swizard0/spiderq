use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use time::SteadyTime;
use super::proto::{Key, RepayStatus};

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
    lentm: HashMap<Key, (u64, PQueueEntry)>,
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

    pub fn add(&mut self, key: Key) {
        self.serial += 1;
        self.queue.push(PQueueEntry { key: key, priority: self.serial, boost: 0, });
    }

    pub fn top(&self) -> Option<Key> {
        self.queue.peek().map(|e| e.key.clone())
    }

    pub fn lend(&mut self, trigger_at: SteadyTime) {
        if let Some(entry) = self.queue.pop() {
            self.lentq.push(LentEntry { trigger_at: trigger_at, key: entry.key.clone(), snapshot: self.serial, });
            self.lentm.insert(entry.key.clone(), (self.serial, entry));
        }
    }

    pub fn next_timeout(&mut self) -> Option<SteadyTime> {
        loop {
            if let Some(&LentEntry { trigger_at: trigger, key: ref k, snapshot: qshot, .. }) = self.lentq.peek() {
                match self.lentm.get(k) {
                    Some(&(mshot, _)) if mshot == qshot => return Some(trigger),
                    _ => (),
                }
            } else {
                break
            }

            self.lentq.pop();
        }
        None
    }
    
    pub fn repay_timed_out(&mut self) {
        if let Some(LentEntry { key: k, .. }) = self.lentq.pop() {
            self.repay(k, &RepayStatus::Front)
        }
    }

    pub fn repay(&mut self, key: Key, status: &RepayStatus) {
        if let Some((_, mut entry)) = self.lentm.remove(&key) {
            let min_priority = if let Some(&PQueueEntry { priority: p, .. }) = self.queue.peek() { p } else { 0 };
            let region = self.serial + 1 - min_priority;
            let current_boost = match status {
                &RepayStatus::Penalty if entry.boost == 0 => 0,
                &RepayStatus::Penalty => { entry.boost -= 1; entry.boost },
                &RepayStatus::Reward if region >> (entry.boost + 1) == 0 => entry.boost,
                &RepayStatus::Reward => { entry.boost += 1; entry.boost },
                &RepayStatus::Front => 0,
                &RepayStatus::Drop => return,
            };
            self.serial += 1;
            entry.priority = match status {
                &RepayStatus::Front => 0,
                _ => self.serial - (region - (region >> current_boost)),
            };
            self.queue.push(entry)
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use time::{SteadyTime, Duration};
    use super::PQueue;
    use super::super::proto::{Key, RepayStatus};

    fn as_key(value: usize) -> Key {
        Arc::new(format!("{}", value).into_bytes())
    }

    fn make_pq(len: usize) -> PQueue {
        let mut pq = PQueue::new();
        for i in 0 .. len {
            pq.add(as_key(i));
        }
        pq
    }

    #[test]
    fn basic_api() {
        let time = SteadyTime::now();
        let mut pq = make_pq(10); // 0 1 2 3 4 5 6 7 8 9 |
        assert_eq!(pq.top(), Some(as_key(0)));
        pq.lend(time + Duration::seconds(10)); // 1 2 3 4 5 6 7 8 9 | 0
        assert_eq!(pq.top(), Some(as_key(1)));
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(10)));
        pq.lend(time + Duration::seconds(5)); // 2 3 4 5 6 7 8 9 | <1/5> <0/10>
        assert_eq!(pq.top(), Some(as_key(2)));
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(5)));
        pq.repay(as_key(1), &RepayStatus::Reward); // 2 3 4 5 (1 6) 7 8 9 | <0/10>
        assert_eq!(pq.top(), Some(as_key(2)));
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(10)));
        pq.repay_timed_out(); // 0 2 3 4 5 (1 6) 7 8 9 |
        assert_eq!(pq.next_timeout(), None);
        assert_eq!(pq.top(), Some(as_key(0)));
        pq.lend(time + Duration::seconds(5)); // 2 3 4 5 (1 6) 7 8 9 | <0/5>
        assert_eq!(pq.top(), Some(as_key(2))); 
        pq.lend(time + Duration::seconds(6)); // 3 4 5 (1 6) 7 8 9 | <0/10> <2/6>
        assert_eq!(pq.top(), Some(as_key(3)));
        pq.lend(time + Duration::seconds(7)); // 4 5 (1 6) 7 8 9 | <0/10> <2/6> <3/7>
        assert_eq!(pq.top(), Some(as_key(4)));
        pq.lend(time + Duration::seconds(8)); // 5 (1 6) 7 8 9 | <0/10> <2/6> <3/7> <4/8>
        assert_eq!(pq.top(), Some(as_key(5)));
        pq.lend(time + Duration::seconds(9)); // (1 6) 7 8 9 | <0/10> <2/6> <3/7> <4/8> <5/9>
        assert_eq!(pq.top(), Some(as_key(6)));
        pq.lend(time + Duration::seconds(10)); // 1 7 8 9 | <0/10> <2/6> <3/7> <4/8> <5/9> <6/10>
        assert_eq!(pq.top(), Some(as_key(1)));
        pq.lend(time + Duration::seconds(11)); // 7 8 9 | <0/10> <2/6> <3/7> <4/8> <5/9> <6/10> <1/11>
        assert_eq!(pq.top(), Some(as_key(7)));
        pq.lend(time + Duration::seconds(12)); // 8 9 | <0/10> <2/6> <3/7> <4/8> <5/9> <6/10> <1/11> <7/12>
        assert_eq!(pq.top(), Some(as_key(8)));
        pq.lend(time + Duration::seconds(13)); // 9 | <0/10> <2/6> <3/7> <4/8> <5/9> <6/10> <1/11> <7/12> <8/13>
        assert_eq!(pq.top(), Some(as_key(9)));
        pq.lend(time + Duration::seconds(14)); // | <0/10> <2/6> <3/7> <4/8> <5/9> <6/10> <1/11> <7/12> <8/13> <9/13>
        assert_eq!(pq.len(), 0);
    }

    #[test]
    fn double_lend() {
        let time = SteadyTime::now();
        let mut pq = make_pq(2); // 0 1 |
        assert_eq!(pq.top(), Some(as_key(0)));
        pq.lend(time + Duration::seconds(10)); // 1 | <0/10>
        assert_eq!(pq.top(), Some(as_key(1)));
        pq.lend(time + Duration::seconds(15)); // | <0/10> <1/15>
        assert_eq!(pq.len(), 0);
        pq.repay(as_key(1), &RepayStatus::Penalty); // 1 | <0/10>
        assert_eq!(pq.top(), Some(as_key(1)));
        pq.lend(time + Duration::seconds(20)); // | <0/10> <1/20>
        assert_eq!(pq.len(), 0);
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(10)));
        pq.repay_timed_out(); // 0 | <1/20>
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(20)));
        assert_eq!(pq.top(), Some(as_key(0)));
    }
}

