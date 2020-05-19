use quickcheck::{Gen, Arbitrary};

use spiderq_proto::{GlobalReq, GlobalRep, Key, Value};

#[derive(Debug)]
struct AddMode(spiderq_proto::AddMode);

impl Into<spiderq_proto::AddMode> for &AddMode {
    fn into(self) -> spiderq_proto::AddMode {
        use spiderq_proto::AddMode;

        match self.0 {
            AddMode::Head => AddMode::Head,
            AddMode::Tail => AddMode::Tail
        }
    }
}

impl Clone for AddMode {
    fn clone(&self) -> Self {
        use spiderq_proto::AddMode;

        match self.0 {
            AddMode::Head => Self(AddMode::Head),
            AddMode::Tail => Self(AddMode::Tail)
        }
    }
}

impl Arbitrary for AddMode {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        use spiderq_proto::AddMode;

        let d = u32::arbitrary(g) % 2;

        match d {
            0 => Self(AddMode::Head),
            1 => Self(AddMode::Tail),
            _ => unreachable!()
        }
    }
}

#[derive(Debug)]
struct LendMode(spiderq_proto::LendMode);

impl Clone for LendMode {
    fn clone(&self) -> Self {
        use spiderq_proto::LendMode;

        match self.0 {
            LendMode::Block => Self(LendMode::Block),
            LendMode::Poll => Self(LendMode::Poll)
        }
    }
}

impl Arbitrary for LendMode {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        use spiderq_proto::LendMode;

        let d = u32::arbitrary(g) % 2;

        match d {
            // 0 => Self(LendMode::Block),
            0 | 1 => Self(LendMode::Poll),
            _ => unreachable!()
        }
    }
}

impl Into<spiderq_proto::LendMode> for &LendMode {
    fn into(self) -> spiderq_proto::LendMode {
        use spiderq_proto::LendMode;

        match self.0 {
            LendMode::Block => LendMode::Block,
            LendMode::Poll => LendMode::Poll
        }
    }
}

#[derive(Debug)]
struct RepayStatus(spiderq_proto::RepayStatus);

impl Clone for RepayStatus {
    fn clone(&self) -> Self {
        use spiderq_proto::RepayStatus as R;

        match self.0 {
            R::Penalty => Self(R::Penalty),
            R::Reward => Self(R::Reward),
            R::Front => Self(R::Front),
            R::Drop => Self(R::Drop),
        }
    }
}

impl Arbitrary for RepayStatus {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        use spiderq_proto::RepayStatus as R;

        let d = u32::arbitrary(g) % 4;

        match d {
            0 => Self(R::Penalty),
            1 => Self(R::Reward),
            2 => Self(R::Front),
            3 => Self(R::Drop),
            _ => unreachable!()
        }
    }
}

impl Into<spiderq_proto::RepayStatus> for &RepayStatus {
    fn into(self) -> spiderq_proto::RepayStatus {
        use spiderq_proto::RepayStatus as R;

        match self.0 {
            R::Penalty => R::Penalty,
            R::Reward => R::Reward,
            R::Front => R::Front,
            R::Drop => R::Drop,
        }
    }
}

#[derive(Clone, Debug)]
enum Op {
    Add {
        key: u32,
        value: u32,
        mode: AddMode
    },
    Lend {
        timeout: u64,
        mode: LendMode
    },
    Repay {
        status: RepayStatus
    },
    Heartbeat {
        timeout: u64,
    },
    Remove,
    Timeout {
        wait: u64
    },
    End
}

impl Arbitrary for Op {
    fn arbitrary<G: Gen>(g: &mut G) -> Self {
        let d = u8::arbitrary(g) % 7;

        match d {
            0 => Op::gen_add(g),
            1 => Op::gen_lend(g),
            2 => Op::gen_repay(g),
            3 => Op::gen_heartbeat(g),
            4 => Op::Remove,
            5 => Op::gen_timeout(g),
            6 => Op::End,
            _ => unreachable!()
        }
    }
}

impl Op {
    fn gen_lend<G: Gen>(g: &mut G) -> Self {
        Op::Lend {
            timeout: u64::arbitrary(g),
            mode: LendMode::arbitrary(g)
        }
    }

    fn gen_timeout<G: Gen>(g: &mut G) -> Self {
        Op::Timeout {
            wait: u64::arbitrary(g)
        }
    }

    fn gen_add<G: Gen>(g: &mut G) -> Self {
        Op::Add {
            key: u32::arbitrary(g),
            value: u32::arbitrary(g),
            mode: AddMode::arbitrary(g)
        }
    }

    fn gen_heartbeat<G: Gen>(g: &mut G) -> Self {
        Op::Heartbeat {
            timeout: u64::arbitrary(g)
        }
    }

    fn gen_repay<G: Gen>(g: &mut G) -> Self {
        Op::Repay {
            status: RepayStatus::arbitrary(g)
        }
    }
}

struct LendResult {
    lend_key: u64,
    key: Key,
    value: Value
}

struct SimState {
    lend_results: Vec<LendResult>,
}

#[derive(Debug)]
struct Step {
    duration: u128,
    len: usize,
    last_lent_key: Option<u32>,
    reps: Vec<GlobalRep>
}

fn send_and_recv(sock: &zmq::Socket, packet: GlobalReq) -> GlobalRep {
    let required = packet.encode_len();
    let mut msg = zmq::Message::with_capacity(required).unwrap();
    packet.encode(&mut msg);
    sock.send_msg(msg, 0).unwrap();

    let reply_msg = sock.recv_msg(0).unwrap();
    let (rep, _) = GlobalRep::decode(&reply_msg).unwrap();

    rep
}

fn run_sim_step(sock: &zmq::Socket, ops: &Vec<Op>) -> Step {
    let mut sim_state = SimState {
        lend_results: Vec::new(),
    };
    let start = std::time::Instant::now();
    let mut last_lent_key = None;
    let mut reps = Vec::new();

    for op in ops.iter() {
        match op {
            Op::Add { key, value, mode } => {
                let req = GlobalReq::Add {
                    key: key.to_be_bytes().to_vec().into(),
                    value: value.to_be_bytes().to_vec().into(),
                    mode: mode.into()
                };

                let rep = send_and_recv(sock, req);

                match rep {
                    GlobalRep::Added | GlobalRep::Kept => {
                        reps.push(rep);
                    },
                    r => panic!("wrong response for add: {:?}", r)
                }
            },
            Op::Lend { timeout, mode } => {
                let req = GlobalReq::Lend {
                    timeout: *timeout,
                    mode: mode.into()
                };

                use std::convert::TryInto;

                let rep = send_and_recv(sock, req);
                match rep {
                    GlobalRep::Lent { lend_key, key, value } => {
                        let key_slice: &[u8] = &key;
                        last_lent_key = Some(u32::from_be_bytes(key_slice.try_into().unwrap()));

                        reps.push(GlobalRep::Lent {
                            lend_key,
                            key: key.clone(),
                            value: value.clone()
                        });

                        sim_state.lend_results.push(LendResult {
                            lend_key, key, value
                        });

                    },
                    GlobalRep::QueueEmpty => {
                        reps.push(rep);
                    },
                    r => panic!("wrong response for lend: {:?}", r)
                };
            }
            Op::Repay { status } => {
                if sim_state.lend_results.is_empty() {
                    continue;
                }

                let LendResult { lend_key, key, value } = sim_state.lend_results.pop().unwrap();

                let req = GlobalReq::Repay {
                    lend_key, key, value,
                    status: status.into()
                };

                let rep = send_and_recv(sock, req);

                match rep {
                    GlobalRep::Repaid | GlobalRep::NotFound => {
                        reps.push(rep);
                    },
                    r => panic!("wrong response for repay: {:?}", r)
                }
            }
            Op::Heartbeat { timeout } => {
                if sim_state.lend_results.is_empty() {
                    continue;
                }

                let LendResult { lend_key, key, .. } = sim_state.lend_results.pop().unwrap();

                let req = GlobalReq::Heartbeat {
                    lend_key, key,
                    timeout: *timeout
                };

                let rep = send_and_recv(sock, req);

                match rep {
                    GlobalRep::Heartbeaten | GlobalRep::Skipped => {
                        reps.push(rep);
                    },
                    r => panic!("wrong response for heartbeat: {:?}", r)
                }
            }
            Op::Remove => {
                if sim_state.lend_results.is_empty() {
                    continue;
                }

                let LendResult { key, .. } = sim_state.lend_results.pop().unwrap();
                let req = GlobalReq::Remove(key);

                let rep = send_and_recv(sock, req);

                match rep {
                    GlobalRep::Removed | GlobalRep::NotRemoved => {
                        reps.push(rep);
                    },
                    r => panic!("wrong response for remove: {:?}", r)
                }
            }
            Op::Timeout { wait } => {
                std::thread::sleep(std::time::Duration::from_millis(*wait));
            }
            Op::End => {}
        }
    }

    let end = std::time::Instant::now();
    let duration = (end - start).as_millis();

    let len = match send_and_recv(sock, GlobalReq::Count) {
        GlobalRep::Counted(len) => len,
        r => panic!("wrong response for len: {:?}", r)
    };

    Step {
        duration,
        last_lent_key,
        len,
        reps
    }
}

fn main() {
    let args = std::env::args();
    let mut opts = getopts::Options::new();

    opts.reqopt("n", "number", "count of script to generate and run", "10");

    let matches = opts.parse(args).unwrap();

    let times_to_run: usize = matches.opt_get("number").unwrap().unwrap();
    let mut scripts = Vec::with_capacity(times_to_run);

    println!("generating {} scripts to run...", times_to_run);

    let mut g = quickcheck::StdThreadGen::new(50);

    for _ in 0..times_to_run {
        let mut script = Vec::new();

        loop {
            let next_op = Op::arbitrary(&mut g);
            script.push(next_op.clone());

            if script.len() > 30 {
                break;
            }

            if let Op::End = next_op {
                break;
            }
        }

        // println!("{:?}", script);
        scripts.push(script);
    }

    // scripts.push(vec![
    //     Op::Lend { timeout: 20, mode: LendMode(spiderq_proto::LendMode::Block) },
    //     Op::gen_add(&mut g)
    // ]);

    use std::sync::mpsc;

    let mut receivers = Vec::new();

    assert!(matches.free.len() >= 1);

    let addrs: Vec<String> = matches.free.iter().skip(1).cloned().collect();

    for addr in addrs {
        println!("{}", addr);

        let (tx, rx) = mpsc::channel();
        receivers.push(rx);

        let scripts = scripts.clone();

        std::thread::spawn(move || {
            let zmq_ctx = zmq::Context::new();
            let sock = zmq_ctx.socket(zmq::REQ).unwrap();
            sock.connect(&addr).unwrap();

            for (i, s) in scripts.iter().enumerate() {
                let step = run_sim_step(&sock, s);
                tx.send(step).unwrap();

                if i % 100 == 0 {
                    println!("{} - {}/{}", addr, i, scripts.len());
                }
            }

            println!("{} done", addr);
        });
    }

    let results: Vec<Vec<Step>> = receivers.iter().map(|r| r.iter().collect()).collect();

    for i in 0..times_to_run {
        let step_to_compare = &results[0][i];

        // let lengths: Vec<usize> = results.iter().map(|r| r[i].len).collect();

        // if !lengths.iter().all(|len| *len == step_to_compare.len) {
        //     println!(
        //         "step #{}: lengths is not equal\nlengths:\n{:?}\nscript:\n{:?}",
        //         i, lengths, scripts[i]
        //     );
        // }

        let keys: Vec<Option<u32>> = results.iter().map(|r| r[i].last_lent_key).collect();

        if !keys.iter().all(|key| *key == step_to_compare.last_lent_key) {
            println!(
                "step #{}: keys is not equal\nkeys:\n{:?}\n",
                i, keys
            );

            for (k, s) in scripts.iter().take(i + 1).enumerate() {
                println!("{:?}", s);

                for (j, r) in results.iter().enumerate() {
                    println!("#{} reply", j);
                    println!("{:?}", r[k]);
                    println!("====");
                }
                println!("----");
            }

            break;
        }
    }

    let durations: Vec<u128> =
        results
            .iter()
            .map(|r|
                r
                    .iter()
                    .fold(0, |acc, s| acc + s.duration)
                )
            .collect();

    for (i, d) in durations.iter().enumerate() {
        println!("#{} - {} ms total", i, d);
    }
}
