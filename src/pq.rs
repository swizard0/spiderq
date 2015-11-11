use std::rc::Rc;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use time::SteadyTime;
use super::proto::RepayStatus;

type Key = Rc<Vec<u8>>;

#[derive(PartialEq, Eq)]
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
    snapshot: u64,
    key: Key,
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
            serial: count as u64,
            queue: BinaryHeap::new(),
            lentm: HashMap::new(),
            lentq: BinaryHeap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn add(&mut self) -> u32 {
        self.serial += 1;
        let last_index = self.len() as u32;
        self.queue.push(PQueueEntry { priority: self.serial, index: last_index, boost: 0, });
        last_index
    }

    pub fn top(&self) -> Option<u32> {
        self.queue.peek().map(|e| e.index)
    }

    pub fn lend(&mut self, trigger_at: SteadyTime) -> Option<u32> {
        if let Some(entry) = self.queue.pop() {
            let index = entry.index;
            self.lentm.insert(index, (self.serial, entry));
            self.lentq.push(LentEntry { trigger_at: trigger_at, snapshot: self.serial, index: index, });
            Some(index)
        } else {
            None
        }
    }

    pub fn next_timeout(&mut self) -> Option<SteadyTime> {
        while let Some(&LentEntry { trigger_at: trigger, snapshot: qshot, index: i, .. }) = self.lentq.peek() {
            match self.lentm.get(&i) {
                Some(&(mshot, _)) if mshot == qshot => 
                    return Some(trigger),
                _ => 
                    self.lentq.pop(),
            };
        }
        None
    }
    
    pub fn repay_timed_out(&mut self) {
        if let Some(LentEntry { index: qi, .. }) = self.lentq.pop() {
            self.repay(qi, RepayStatus::Front)
        }
    }

    pub fn repay(&mut self, index: u32, status: RepayStatus) {
        if let Some((_, mut entry)) = self.lentm.remove(&index) {
            self.serial += 1;
            let min_priority = if let Some(&PQueueEntry { priority: p, .. }) = self.queue.peek() { p } else { 0 };
            let region = self.serial - min_priority;
            let current_boost = match status {
                RepayStatus::Penalty if entry.boost == 0 => 0,
                RepayStatus::Penalty => { entry.boost -= 1; entry.boost },
                RepayStatus::Reward if region >> (entry.boost + 1) == 0 => entry.boost,
                RepayStatus::Reward => { entry.boost += 1; entry.boost },
                RepayStatus::Front => 0,
            };
            entry.priority = match status {
                RepayStatus::Front => 0,
                _ => self.serial - (region - (region >> current_boost)),
            };
            self.queue.push(entry)
        }
    }
}

#[cfg(test)]
mod test {
    use time::{SteadyTime, Duration};
    use super::PQueue;
    use super::super::proto::RepayStatus;

    #[test]
    fn basic_api() {
        let time = SteadyTime::now();
        let mut pq = PQueue::new(10);
        assert_eq!(pq.top(), Some(0));
        assert_eq!(pq.lend(time + Duration::seconds(10)), Some(0));
        assert_eq!(pq.top(), Some(1));
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(10)));
        assert_eq!(pq.lend(time + Duration::seconds(5)), Some(1));
        assert_eq!(pq.top(), Some(2));
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(5)));
        pq.repay(1, RepayStatus::Reward);
        assert_eq!(pq.top(), Some(2));
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(10)));
        pq.repay_timed_out();
        assert_eq!(pq.next_timeout(), None);
        assert_eq!(pq.lend(time + Duration::seconds(5)), Some(0));
        assert_eq!(pq.lend(time + Duration::seconds(6)), Some(2));
        assert_eq!(pq.lend(time + Duration::seconds(7)), Some(3));
        assert_eq!(pq.lend(time + Duration::seconds(8)), Some(4));
        assert_eq!(pq.lend(time + Duration::seconds(9)), Some(5));
        assert_eq!(pq.lend(time + Duration::seconds(10)), Some(6));
        assert_eq!(pq.lend(time + Duration::seconds(11)), Some(1));
        assert_eq!(pq.lend(time + Duration::seconds(12)), Some(7));
        assert_eq!(pq.lend(time + Duration::seconds(13)), Some(8));
        assert_eq!(pq.lend(time + Duration::seconds(14)), Some(9));
    }

    #[test]
    fn double_lend() {
        let time = SteadyTime::now();
        let mut pq = PQueue::new(2);
        assert_eq!(pq.lend(time + Duration::seconds(10)), Some(0));
        assert_eq!(pq.lend(time + Duration::seconds(15)), Some(1));
        pq.repay(1, RepayStatus::Penalty);
        assert_eq!(pq.lend(time + Duration::seconds(20)), Some(1));
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(10)));
        pq.repay_timed_out();
        assert_eq!(pq.next_timeout(), Some(time + Duration::seconds(20)));
    }

    #[test]
    fn add() {
        let mut pq = PQueue::new(2);
        assert_eq!(pq.len(), 2);
        assert_eq!(pq.add(), 2);
        assert_eq!(pq.len(), 3);
        assert_eq!(pq.add(), 3);
        assert_eq!(pq.len(), 4);
    }
}

