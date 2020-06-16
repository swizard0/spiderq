#[derive(Debug)]
enum Op {
    Incr,
    Decr,
    Set(usize),
    Recount,
    Stop
}

#[derive(Clone)]
pub enum SledTree {
    Db(sled::Db),
    Tree(sled::Tree)
}

pub struct System {
    tree: sled::Tree,
    update_tx: std::sync::mpsc::Sender<Op>,
    thread_handle: Option<std::thread::JoinHandle<()>>
}

impl System {
    pub fn new(tree: sled::Tree, db: SledTree, recount: bool) -> Self
    {
        match tree.get("count") {
            Ok(Some(value)) => {
                match bincode::deserialize::<usize>(&value) {
                    Ok(count) => count,
                    Err(_) => 0
                }
            }
            Ok(None) | Err(_) => {
                let v = bincode::serialize::<usize>(&0).unwrap();
                tree.insert("count", v);

                0
            }
        };

        let (send, recv) = std::sync::mpsc::channel();
        let tree_copy = tree.clone();
        let db_copy = db.clone();

        let thread_handle = std::thread::spawn(move || {
            let mut count: i128 = 0;
            let mut ops_count = 0;

            for m in recv {
                ops_count += 1;

                match m {
                    Op::Incr => {
                        count += 1;
                    }
                    Op::Decr => {
                        count -= 1;
                    }
                    Op::Recount => {
                        count = match db_copy {
                            SledTree::Db(ref db) => db.len(),
                            SledTree::Tree(ref db) => db.len()
                        } as i128;
                    }
                    Op::Set(new_count) => {
                        count = new_count as i128;
                    }
                    Op::Stop => {
                        break;
                    }
                }

                if ops_count >= 5 {
                    tree_copy.fetch_and_update("count", |v| {
                        match v {
                            Some(v) => {
                                let new_count = match bincode::deserialize::<usize>(v) {
                                    Ok(existing_count) => match m {
                                        Op::Set(..) | Op::Recount => count as usize,
                                        _ => (existing_count as isize + count as isize) as usize
                                    },
                                    Err(_) => count as usize
                                };

                                let v = bincode::serialize::<usize>(&new_count).unwrap();
                                Some(v)
                            }
                            None => {
                                let v = bincode::serialize::<usize>(&(count as usize)).unwrap();
                                Some(v)
                            }
                        }
                    });
                    count = 0;
                    ops_count = 0;
                }
            }
        });

        if recount {
            let new_count = match db {
                SledTree::Db(ref db) => db.len(),
                SledTree::Tree(ref db) => db.len()
            };
            send.send(Op::Set(new_count));
        }

        Self {
            tree,
            update_tx: send,
            thread_handle: Some(thread_handle)
        }
    }

    pub fn incr(&self) {
        self.update_tx.send(Op::Incr);
    }

    pub fn decr(&self) {
        self.update_tx.send(Op::Decr);
    }

    pub fn count(&self) -> usize {
        match self.tree.get("count") {
            Ok(Some(value)) => {
                let count = match bincode::deserialize::<usize>(&value) {
                    Ok(count) => count,
                    Err(_) => 0
                };

                count
            }
            Ok(None) | Err(_) => {
                0
            }
        }
    }

    pub fn stop(&mut self) {
        self.update_tx.send(Op::Stop);
        self.thread_handle.take().map(|h| h.join().unwrap());
    }
}

impl Drop for System {
    fn drop(&mut self) {
        self.stop();
    }
}
