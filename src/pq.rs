use std::cmp::Ordering;
use std::time::Duration;
use std::collections::{BinaryHeap, HashMap};

#[derive(PartialEq, Eq)]
struct PQueueEntry {
    priority: u64,
    index: u32,
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
    trigger_at: Duration,
    snapshot: u64,
    index: u32,
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
    lentm: HashMap<u32, (u64, PQueueEntry)>,
    lentq: BinaryHeap<LentEntry>,
}

#[derive(Debug, PartialEq)]
pub enum RepayStatus {
    Penalty,
    Reward,
    Requeue,
}

impl PQueue {
    pub fn new(count: usize) -> PQueue {
        PQueue {
            serial: count as u64,
            queue: (0 .. count).map(|i| PQueueEntry { priority: i as u64, index: i as u32, boost: 0, }).collect(),
            lentm: HashMap::new(),
            lentq: BinaryHeap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn add(&mut self) {
        let last_index = self.len() as u32;
        self.queue.push(PQueueEntry { priority: 0, index: last_index, boost: 0, });
    }

    pub fn top(&self) -> Option<u32> {
        self.queue.peek().map(|e| e.index)
    }

    pub fn lend(&mut self, trigger_at: Duration) -> Option<u32> {
        if let Some(entry) = self.queue.pop() {
            let index = entry.index;
            self.lentm.insert(index, (self.serial, entry));
            self.lentq.push(LentEntry { trigger_at: trigger_at, snapshot: self.serial, index: index, });
            Some(index)
        } else {
            None
        }
    }

    pub fn next_timeout(&mut self) -> Option<Duration> {
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
            self.repay(qi, RepayStatus::Requeue)
        }
    }

    pub fn repay(&mut self, index: u32, status: RepayStatus) {
        if let Some((_, mut entry)) = self.lentm.remove(&index) {
            self.serial += 1;
            let min_priority = if let Some(&PQueueEntry { priority: p, .. }) = self.queue.peek() { p } else { 0 };
            let total = self.queue.len() as u64;
            let region = total - min_priority;
            let current_boost = match status {
                RepayStatus::Penalty if entry.boost == 0 => 0,
                RepayStatus::Penalty => { entry.boost -= 1; entry.boost },
                RepayStatus::Reward if region >> (entry.boost + 1) == 0 => entry.boost,
                RepayStatus::Reward => { entry.boost += 1; entry.boost },
                RepayStatus::Requeue => 0,
            };
            entry.priority = match status {
                RepayStatus::Requeue => 0,
                _ => self.serial - (region - (region >> current_boost)),
            };
            self.queue.push(entry)
        }
    }
}
