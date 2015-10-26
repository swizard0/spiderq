use std::cmp::Ordering;
use std::time::Duration;
use std::collections::{BinaryHeap, HashMap};

#[derive(PartialEq, Eq)]
struct PQueueEntry {
    priority: u32,
    index: u32,
}

impl Ord for PQueueEntry {
    fn cmp(&self, other: &PQueueEntry) -> Ordering {
        self.priority.cmp(&other.priority)
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
    index: u32,
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

pub struct PQueue {
    queue: BinaryHeap<PQueueEntry>,
    lentm: HashMap<u32, PQueueEntry>,
    lentq: BinaryHeap<LentEntry>,
}

impl PQueue {
    pub fn new(count: usize) -> PQueue {
        PQueue {
            queue: (0 .. count).map(|i| PQueueEntry { priority: 0, index: i as u32, }).collect(),
            lentm: HashMap::new(),
            lentq: BinaryHeap::new(),
        }
    }

    pub fn top(&self) -> Option<u32> {
        self.queue.peek().map(|e| e.index)
    }

    pub fn lend(&mut self, trigger_at: Duration) -> Option<u32> {
        if let Some(entry) = self.queue.pop() {
            let index = entry.index;
            self.lentm.insert(index, entry);
            self.lentq.push(LentEntry { trigger_at: trigger_at, index: index, });
            Some(index)
        } else {
            None
        }
    }

    pub fn next_timeout(&mut self) -> Option<Duration> {
        while let Some(&LentEntry { trigger_at: trigger, index: i, .. }) = self.lentq.peek() {
            if self.lentm.contains_key(&i) {
                return Some(trigger)
            }
            self.lentq.pop();
        }
        None
    }
    
    pub fn repay_timed_out(&mut self) {
        if let Some(LentEntry { index: qi, .. }) = self.lentq.pop() {
            if let Some(entry) = self.lentm.remove(&qi) {
                self.queue.push(entry)
            }
        }
    }
}
