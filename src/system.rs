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
    update_tx: std::sync::mpsc::Sender<Op>
}

impl System {
    pub fn new(tree: sled::Tree, db: SledTree, recount: bool) -> Self
    {
        let count = match tree.get("count") {
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

        std::thread::spawn(move || {
            let mut count: usize = 0;
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
                        };
                    }
                    Op::Set(new_count) => {
                        count = new_count;
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
                                        Op::Set(..) | Op::Recount => count,
                                        _ => existing_count + count
                                    },
                                    Err(_) => count
                                };

                                let v = bincode::serialize::<usize>(&new_count).unwrap();
                                Some(v)
                            }
                            None => {
                                let v = bincode::serialize::<usize>(&count).unwrap();
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
            update_tx: send
        }
    }

    pub fn incr(&self) {
        self.update_tx.send(Op::Incr);
    }

    pub fn decr(&self) {
        self.update_tx.send(Op::Decr);
    }

    pub fn count(&self) -> usize {
        // match self.tree.get("count") {
        //     Ok(Some(value)) => {
        //         let count = match bincode::deserialize::<usize>(&value) {
        //             Ok(count) => count,
        //             Err(_) => 0
        //         };

        //         count
        //     }
        //     Ok(None) | Err(_) => {
        //         0
        //     }
        // }
        0
    }
}

impl Drop for System {
    fn drop(&mut self) {
        self.update_tx.send(Op::Stop).unwrap();
    }
}
