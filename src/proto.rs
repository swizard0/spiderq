use std::sync::Arc;
use std::mem::size_of;
use std::slice::bytes;
use time::SteadyTime;
use byteorder::{ByteOrder, BigEndian};

pub type Key = Arc<Vec<u8>>;
pub type Value = Arc<Vec<u8>>;

#[derive(Debug, PartialEq)]
pub enum RepayStatus {
    Penalty,
    Reward,
    Front,
    Drop,
}

#[derive(Debug, PartialEq)]
pub enum GlobalReq {
    Count,
    Add(Key, Value),
    Lend { timeout: u64 },
    Repay(Key, Value, RepayStatus),
    Stats,
    Terminate,
}

#[derive(Debug, PartialEq)]
pub enum LocalReq {
    NextTrigger,
    Enqueue(Key),
    LendUntil(u64, SteadyTime),
    LoadLent(Key),
    RepayUpdate(Key, Value),
    RepayQueue(Key, RepayStatus),
    Stop,
}

#[derive(Debug, PartialEq)]
pub enum GlobalRep {
    Counted(usize),
    Added,
    Kept,
    Lent(Key, Value),
    Repaid,
    StatsGot { count: usize, add: usize, lend: usize, repay: usize, stats: usize, },
    Error(ProtoError),
}

#[derive(Debug, PartialEq)]
pub enum LocalRep {
    TriggerGot(SteadyTime),
    Added(Key),
    Kept,
    Lent(Key),
    EmptyQueueHit { timeout: u64, },
    Stopped,
}

#[derive(Debug, PartialEq)]
pub enum ProtoError {
    NotEnoughDataForGlobalReqTag { required: usize, given: usize, },
    InvalidGlobalReqTag(u8),
    NotEnoughDataForGlobalReqAddKeyLen { required: usize, given: usize, },
    NotEnoughDataForGlobalReqAddKey { required: usize, given: usize, },
    NotEnoughDataForGlobalReqAddValueLen { required: usize, given: usize, },
    NotEnoughDataForGlobalReqAddValue { required: usize, given: usize, },
    NotEnoughDataForGlobalReqLendTimeout { required: usize, given: usize, },
    NotEnoughDataForGlobalReqRepayKeyLen { required: usize, given: usize, },
    NotEnoughDataForGlobalReqRepayKey { required: usize, given: usize, },
    NotEnoughDataForGlobalReqRepayValueLen { required: usize, given: usize, },
    NotEnoughDataForGlobalReqRepayValue { required: usize, given: usize, },
    NotEnoughDataForGlobalReqRepayRepayStatus { required: usize, given: usize, },
    InvalidGlobalReqRepayRepayStatusTag(u8),
    NotEnoughDataForGlobalRepTag { required: usize, given: usize, },
    InvalidGlobalRepTag(u8),
    NotEnoughDataForGlobalRepCountCount { required: usize, given: usize, },
    NotEnoughDataForGlobalRepLentKeyLen { required: usize, given: usize, },
    NotEnoughDataForGlobalRepLentKey { required: usize, given: usize, },
    NotEnoughDataForGlobalRepLentValueLen { required: usize, given: usize, },
    NotEnoughDataForGlobalRepLentValue { required: usize, given: usize, },
    NotEnoughDataForGlobalRepStatsCount { required: usize, given: usize, },
    NotEnoughDataForGlobalRepStatsAdd { required: usize, given: usize, },
    NotEnoughDataForGlobalRepStatsLend { required: usize, given: usize, },
    NotEnoughDataForGlobalRepStatsRepay { required: usize, given: usize, },
    NotEnoughDataForGlobalRepStatsStats { required: usize, given: usize, },
    NotEnoughDataForProtoErrorTag { required: usize, given: usize, },
    InvalidProtoErrorTag(u8),
    NotEnoughDataForProtoErrorRequired { required: usize, given: usize, },
    NotEnoughDataForProtoErrorGiven { required: usize, given: usize, },
    NotEnoughDataForProtoErrorInvalidTag { required: usize, given: usize, },
    DbQueueOutOfSync(Key),
    NotEnoughDataForProtoErrorDbQueueOutOfSyncKeyLen { required: usize, given: usize, },
    NotEnoughDataForProtoErrorDbQueueOutOfSyncKey { required: usize, given: usize, },
}

macro_rules! try_get {
    ($data:ident, $ty:ty, $reader:ident, $err:ident) => 
        (if $data.len() < size_of::<$ty>() {
            return Err(ProtoError::$err { required: size_of::<$ty>(), given: $data.len(), })
        } else {
            (BigEndian::$reader($data), &$data[size_of::<$ty>() ..])
        })
}

macro_rules! put_adv {
    ($area:expr, $ty:ty, $writer:ident, $value:expr) => ({
        let area = $area;
        BigEndian::$writer(area, $value);
        &mut area[size_of::<$ty>() ..]
    })
}

trait U8Support {
    fn read_u8(buf: &[u8]) -> u8;
    fn write_u8(buf: &mut [u8], n: u8);
}

impl U8Support for BigEndian {
    fn read_u8(buf: &[u8]) -> u8 { 
        buf[0] 
    }

    fn write_u8(buf: &mut [u8], n: u8) {
        buf[0] = n;
    }
}

macro_rules! try_get_vec {
    ($buf:expr, $err_len:ident, $err_val:ident) => ({
        let buf = $buf;
        let (len, buf) = try_get!(buf, u32, read_u32, $err_len);
        let len = len as usize;
        if buf.len() < len {
            return Err(ProtoError::$err_val { required: len, given: buf.len(), })
        } else {
            (Arc::new(buf[0 .. len].to_owned()), &buf[len ..])
        }
    })
}

macro_rules! put_vec_adv {
    ($area:expr, $vec:ident) => ({
        let src = &*$vec;
        let dst = $area;
        let src_len_value = src.len() as u32;
        let area = put_adv!(dst, u32, write_u32, src_len_value);
        bytes::copy_memory(src, area);
        &mut area[src.len() ..]
    })
}

impl GlobalReq {
    pub fn decode<'a>(data: &'a [u8]) -> Result<(GlobalReq, &'a [u8]), ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForGlobalReqTag) {
            (1, buf) => 
                Ok((GlobalReq::Count, buf)),
            (2, buf) => {
                let (key, buf) = try_get_vec!(buf, NotEnoughDataForGlobalReqAddKeyLen, NotEnoughDataForGlobalReqAddKey);
                let (value, buf) = try_get_vec!(buf, NotEnoughDataForGlobalReqAddValueLen, NotEnoughDataForGlobalReqAddValue);
                Ok((GlobalReq::Add(key, value), buf))
            },
            (3, buf) => { 
                let (timeout, buf) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalReqLendTimeout);
                Ok((GlobalReq::Lend { timeout: timeout, }, buf))
            },
            (4, buf) => { 
                let (key, buf) = try_get_vec!(buf, NotEnoughDataForGlobalReqRepayKeyLen, NotEnoughDataForGlobalReqRepayKey);
                let (value, buf) = try_get_vec!(buf, NotEnoughDataForGlobalReqRepayValueLen, NotEnoughDataForGlobalReqRepayValue);
                let (status, buf) = match try_get!(buf, u8, read_u8, NotEnoughDataForGlobalReqRepayRepayStatus) {
                    (1, buf) => (RepayStatus::Penalty, buf),
                    (2, buf) => (RepayStatus::Reward, buf),
                    (3, buf) => (RepayStatus::Front, buf),
                    (4, buf) => (RepayStatus::Drop, buf),
                    (status_tag, _) => return Err(ProtoError::InvalidGlobalReqRepayRepayStatusTag(status_tag)),
                };
                Ok((GlobalReq::Repay(key, value, status), buf))
            },
            (5, buf) => 
                Ok((GlobalReq::Stats, buf)),
            (6, buf) => 
                Ok((GlobalReq::Terminate, buf)),
            (tag, _) => 
                return Err(ProtoError::InvalidGlobalReqTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &GlobalReq::Count | &GlobalReq::Stats | &GlobalReq::Terminate => 0,
            &GlobalReq::Add(ref key, ref value) => size_of::<u32>() * 2 + key.len() + value.len(),
            &GlobalReq::Lend { .. } => size_of::<u64>(),
            &GlobalReq::Repay(ref key, ref value, _) => size_of::<u32>() * 2 + key.len() + value.len() + size_of::<u8>(),
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &GlobalReq::Count =>
                put_adv!(area, u8, write_u8, 1),
            &GlobalReq::Add(ref key, ref value) => {
                let area = put_adv!(area, u8, write_u8, 2);
                let area = put_vec_adv!(area, key);
                let area = put_vec_adv!(area, value);
                area
            },
            &GlobalReq::Lend { timeout: t } => {
                let area = put_adv!(area, u8, write_u8, 3);
                put_adv!(area, u64, write_u64, t)
            },
            &GlobalReq::Repay(ref key, ref value, ref status) => {
                let area = put_adv!(area, u8, write_u8, 4);
                let area = put_vec_adv!(area, key);
                let area = put_vec_adv!(area, value);
                put_adv!(area, u8, write_u8, match status {
                    &RepayStatus::Penalty => 1,
                    &RepayStatus::Reward => 2,
                    &RepayStatus::Front => 3,
                    &RepayStatus::Drop => 4,
                })
            },
            &GlobalReq::Stats =>
                put_adv!(area, u8, write_u8, 5),
            &GlobalReq::Terminate =>
                put_adv!(area, u8, write_u8, 6),
        }
    }
}

impl GlobalRep {
    pub fn decode<'a>(data: &'a [u8]) -> Result<(GlobalRep, &'a [u8]), ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForGlobalRepTag) {
            (1, buf) => {
                let (count, buf) = try_get!(buf, u32, read_u32, NotEnoughDataForGlobalRepCountCount);
                Ok((GlobalRep::Counted(count as usize), buf))
            },
            (2, buf) =>
                Ok((GlobalRep::Added, buf)),
            (3, buf) =>
                Ok((GlobalRep::Kept, buf)),
            (4, buf) => {
                let (key, buf) = try_get_vec!(buf, NotEnoughDataForGlobalRepLentKeyLen, NotEnoughDataForGlobalRepLentKey);
                let (value, buf) = try_get_vec!(buf, NotEnoughDataForGlobalRepLentValueLen, NotEnoughDataForGlobalRepLentValue);
                Ok((GlobalRep::Lent(key, value), buf))
            },
            (5, buf) => 
                Ok((GlobalRep::Repaid, buf)),
            (6, buf) => {
                let (stats_count, buf) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalRepStatsCount);
                let (stats_add, buf) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalRepStatsAdd);
                let (stats_lend, buf) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalRepStatsLend);
                let (stats_repay, buf) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalRepStatsRepay);
                let (stats_stats, buf) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalRepStatsStats);
                Ok((GlobalRep::StatsGot { count: stats_count as usize,
                                          add: stats_add as usize,
                                          lend: stats_lend as usize,
                                          repay: stats_repay as usize,
                                          stats: stats_stats as usize, }, buf))
            },
            (7, buf) => {
                let (err, buf) = try!(ProtoError::decode(buf));
                Ok((GlobalRep::Error(err), buf))
            },
            (tag, _) => 
                return Err(ProtoError::InvalidGlobalRepTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &GlobalRep::Counted(..) => size_of::<u32>(),
            &GlobalRep::Added | &GlobalRep::Kept | &GlobalRep::Repaid => 0,
            &GlobalRep::Lent(ref key, ref value) => size_of::<u32>() * 2 + key.len() + value.len(),
            &GlobalRep::StatsGot { .. } => size_of::<u64>() * 5,
            &GlobalRep::Error(ref err) => err.encode_len(),
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &GlobalRep::Counted(count) => {
                let area = put_adv!(area, u8, write_u8, 1);
                put_adv!(area, u32, write_u32, count as u32)
            },
            &GlobalRep::Added =>
                put_adv!(area, u8, write_u8, 2),
            &GlobalRep::Kept =>
                put_adv!(area, u8, write_u8, 3),
            &GlobalRep::Lent(ref key, ref value) => {
                let area = put_adv!(area, u8, write_u8, 4);
                let area = put_vec_adv!(area, key);
                let area = put_vec_adv!(area, value);
                area
            },
            &GlobalRep::Repaid =>
                put_adv!(area, u8, write_u8, 5),
            &GlobalRep::StatsGot { count: stats_count, add: stats_add, lend: stats_lend, repay: stats_repay, stats: stats_stats, } => {
                let area = put_adv!(area, u8, write_u8, 6);
                let area = put_adv!(area, u64, write_u64, stats_count as u64);
                let area = put_adv!(area, u64, write_u64, stats_add as u64);
                let area = put_adv!(area, u64, write_u64, stats_lend as u64);
                let area = put_adv!(area, u64, write_u64, stats_repay as u64);
                let area = put_adv!(area, u64, write_u64, stats_stats as u64);
                area
            },
            &GlobalRep::Error(ref err) => {
                let area = put_adv!(area, u8, write_u8, 7);
                err.encode(area)
            },
        }
    }
}

macro_rules! decode_not_enough {
    ($buf:ident, $pe_type:ident) => ({
        let (required, given_buf) = try_get!($buf, u32, read_u32, NotEnoughDataForProtoErrorRequired);
        let (given, rest) = try_get!(given_buf, u32, read_u32, NotEnoughDataForProtoErrorGiven);
        Ok((ProtoError::$pe_type { required: required as usize, given: given as usize, }, rest))
    })
}

macro_rules! encode_not_enough {
    ($area:ident, $tag:expr, $required:expr, $given: expr) => ({
        let area = put_adv!($area, u8, write_u8, $tag);
        let area = put_adv!(area, u32, write_u32, $required as u32);
        put_adv!(area, u32, write_u32, $given as u32)
    })
}

macro_rules! decode_tag {
    ($buf:ident, $pe_type:ident) => ({
        let (tag, rest) = try_get!($buf, u8, read_u8, NotEnoughDataForProtoErrorInvalidTag);
        Ok((ProtoError::$pe_type(tag), rest))
    })
}

macro_rules! encode_tag {
    ($area:ident, $tag:expr, $invalid_tag:expr) => ({
        let area = put_adv!($area, u8, write_u8, $tag);
        put_adv!(area, u8, write_u8, $invalid_tag)
    })
}

impl ProtoError {
    pub fn decode<'a>(data: &'a [u8]) -> Result<(ProtoError, &'a [u8]), ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForProtoErrorTag) {
            (1, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqTag),
            (2, buf) => decode_tag!(buf, InvalidGlobalReqTag),
            (3, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqAddKeyLen),
            (4, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqAddKey),
            (5, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqAddValueLen),
            (6, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqAddValue),
            (7, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqLendTimeout),
            (8, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqRepayKeyLen),
            (9, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqRepayKey),
            (10, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqRepayValueLen),
            (11, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqRepayValue),
            (12, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqRepayRepayStatus),
            (13, buf) => decode_tag!(buf, InvalidGlobalReqRepayRepayStatusTag),
            (14, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepTag),
            (15, buf) => decode_tag!(buf, InvalidGlobalRepTag),
            (16, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepCountCount),
            (17, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepLentKeyLen),
            (18, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepLentKey),
            (19, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepLentValueLen),
            (20, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepLentValue),
            (21, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepStatsCount),
            (22, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepStatsAdd),
            (23, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepStatsLend),
            (24, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepStatsRepay),
            (25, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepStatsStats),
            (26, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorTag),
            (27, buf) => decode_tag!(buf, InvalidProtoErrorTag),
            (28, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorRequired),
            (29, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorGiven),
            (30, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorInvalidTag),
            (31, buf) => {
                let (key, buf) =
                    try_get_vec!(buf, NotEnoughDataForProtoErrorDbQueueOutOfSyncKeyLen, NotEnoughDataForProtoErrorDbQueueOutOfSyncKey);
                Ok((ProtoError::DbQueueOutOfSync(key), buf))
            },
            (32, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorDbQueueOutOfSyncKeyLen),
            (33, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorDbQueueOutOfSyncKey),
            (tag, _) => return Err(ProtoError::InvalidProtoErrorTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &ProtoError::NotEnoughDataForGlobalReqTag { .. } |
            &ProtoError::NotEnoughDataForGlobalReqAddKeyLen { .. } |
            &ProtoError::NotEnoughDataForGlobalReqAddKey { .. } |
            &ProtoError::NotEnoughDataForGlobalReqAddValueLen { .. } |
            &ProtoError::NotEnoughDataForGlobalReqAddValue { .. } |
            &ProtoError::NotEnoughDataForGlobalReqLendTimeout { .. } |
            &ProtoError::NotEnoughDataForGlobalReqRepayKeyLen { .. } |
            &ProtoError::NotEnoughDataForGlobalReqRepayKey { .. } |
            &ProtoError::NotEnoughDataForGlobalReqRepayValueLen { .. } |
            &ProtoError::NotEnoughDataForGlobalReqRepayValue { .. } |
            &ProtoError::NotEnoughDataForGlobalReqRepayRepayStatus { .. } |
            &ProtoError::NotEnoughDataForGlobalRepTag { .. } |
            &ProtoError::NotEnoughDataForGlobalRepCountCount { .. } |
            &ProtoError::NotEnoughDataForGlobalRepLentKeyLen { .. } |
            &ProtoError::NotEnoughDataForGlobalRepLentKey { .. } |
            &ProtoError::NotEnoughDataForGlobalRepLentValueLen { .. } |
            &ProtoError::NotEnoughDataForGlobalRepLentValue { .. } |
            &ProtoError::NotEnoughDataForGlobalRepStatsCount { .. } |
            &ProtoError::NotEnoughDataForGlobalRepStatsAdd { .. } |
            &ProtoError::NotEnoughDataForGlobalRepStatsLend { .. } |
            &ProtoError::NotEnoughDataForGlobalRepStatsRepay { .. } |
            &ProtoError::NotEnoughDataForGlobalRepStatsStats { .. } |
            &ProtoError::NotEnoughDataForProtoErrorTag { .. } |
            &ProtoError::NotEnoughDataForProtoErrorRequired { .. } |
            &ProtoError::NotEnoughDataForProtoErrorGiven { .. } |
            &ProtoError::NotEnoughDataForProtoErrorInvalidTag { .. } |
            &ProtoError::NotEnoughDataForProtoErrorDbQueueOutOfSyncKeyLen { .. } |
            &ProtoError::NotEnoughDataForProtoErrorDbQueueOutOfSyncKey { .. } =>
                size_of::<u32>() + size_of::<u32>(),
            &ProtoError::InvalidGlobalRepTag(..) |
            &ProtoError::InvalidGlobalReqTag(..) |
            &ProtoError::InvalidGlobalReqRepayRepayStatusTag(..) |
            &ProtoError::InvalidProtoErrorTag(..) =>
                size_of::<u8>(),
            &ProtoError::DbQueueOutOfSync(ref key) => size_of::<u32>() + key.len(),

        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &ProtoError::NotEnoughDataForGlobalReqTag { required: r, given: g, } => encode_not_enough!(area, 1, r, g),
            &ProtoError::InvalidGlobalReqTag(tag) => encode_tag!(area, 2, tag),
            &ProtoError::NotEnoughDataForGlobalReqAddKeyLen { required: r, given: g, } => encode_not_enough!(area, 3, r, g),
            &ProtoError::NotEnoughDataForGlobalReqAddKey { required: r, given: g, } => encode_not_enough!(area, 4, r, g),
            &ProtoError::NotEnoughDataForGlobalReqAddValueLen { required: r, given: g, } => encode_not_enough!(area, 5, r, g),
            &ProtoError::NotEnoughDataForGlobalReqAddValue { required: r, given: g, } => encode_not_enough!(area, 6, r, g),
            &ProtoError::NotEnoughDataForGlobalReqLendTimeout { required: r, given: g, } => encode_not_enough!(area, 7, r, g),
            &ProtoError::NotEnoughDataForGlobalReqRepayKeyLen { required: r, given: g, } => encode_not_enough!(area, 8, r, g),
            &ProtoError::NotEnoughDataForGlobalReqRepayKey { required: r, given: g, } => encode_not_enough!(area, 9, r, g),
            &ProtoError::NotEnoughDataForGlobalReqRepayValueLen { required: r, given: g, } => encode_not_enough!(area, 10, r, g),
            &ProtoError::NotEnoughDataForGlobalReqRepayValue { required: r, given: g, } => encode_not_enough!(area, 11, r, g),
            &ProtoError::NotEnoughDataForGlobalReqRepayRepayStatus { required: r, given: g, } => encode_not_enough!(area, 12, r, g),
            &ProtoError::InvalidGlobalReqRepayRepayStatusTag(tag) => encode_tag!(area, 13, tag),
            &ProtoError::NotEnoughDataForGlobalRepTag { required: r, given: g, } => encode_not_enough!(area, 14, r, g),
            &ProtoError::InvalidGlobalRepTag(tag) => encode_tag!(area, 15, tag),
            &ProtoError::NotEnoughDataForGlobalRepCountCount { required: r, given: g, } => encode_not_enough!(area, 16, r, g),
            &ProtoError::NotEnoughDataForGlobalRepLentKeyLen { required: r, given: g, } => encode_not_enough!(area, 17, r, g),
            &ProtoError::NotEnoughDataForGlobalRepLentKey { required: r, given: g, } => encode_not_enough!(area, 18, r, g),
            &ProtoError::NotEnoughDataForGlobalRepLentValueLen { required: r, given: g, } => encode_not_enough!(area, 19, r, g),
            &ProtoError::NotEnoughDataForGlobalRepLentValue { required: r, given: g, } => encode_not_enough!(area, 20, r, g),
            &ProtoError::NotEnoughDataForGlobalRepStatsCount { required: r, given: g, } => encode_not_enough!(area, 21, r, g),
            &ProtoError::NotEnoughDataForGlobalRepStatsAdd { required: r, given: g, } => encode_not_enough!(area, 22, r, g),
            &ProtoError::NotEnoughDataForGlobalRepStatsLend { required: r, given: g, } => encode_not_enough!(area, 23, r, g),
            &ProtoError::NotEnoughDataForGlobalRepStatsRepay { required: r, given: g, } => encode_not_enough!(area, 24, r, g),
            &ProtoError::NotEnoughDataForGlobalRepStatsStats { required: r, given: g, } => encode_not_enough!(area, 25, r, g),
            &ProtoError::NotEnoughDataForProtoErrorTag { required: r, given: g, } => encode_not_enough!(area, 26, r, g),
            &ProtoError::InvalidProtoErrorTag(tag) => encode_tag!(area, 27, tag),
            &ProtoError::NotEnoughDataForProtoErrorRequired { required: r, given: g, } => encode_not_enough!(area, 28, r, g),
            &ProtoError::NotEnoughDataForProtoErrorGiven { required: r, given: g, } => encode_not_enough!(area, 29, r, g),
            &ProtoError::NotEnoughDataForProtoErrorInvalidTag { required: r, given: g, } => encode_not_enough!(area, 30, r, g),
            &ProtoError::DbQueueOutOfSync(ref key) => {
                let area = put_adv!(area, u8, write_u8, 31);
                let area = put_vec_adv!(area, key);
                area
            },
            &ProtoError::NotEnoughDataForProtoErrorDbQueueOutOfSyncKeyLen { required: r, given: g, } => encode_not_enough!(area, 32, r, g),
            &ProtoError::NotEnoughDataForProtoErrorDbQueueOutOfSyncKey { required: r, given: g, } => encode_not_enough!(area, 33, r, g),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use super::{Key, Value, RepayStatus, GlobalReq, GlobalRep, ProtoError};

    macro_rules! defassert_encode_decode {
        ($name:ident, $ty:ty, $class:ident) => (fn $name(r: $ty) {
            let bytes_required = r.encode_len();
            let mut area: Vec<_> = (0 .. bytes_required).map(|_| 0).collect();
            assert!(r.encode(&mut area).len() == 0);
            let (assert_r, rest) = $class::decode(&area).unwrap();
            assert_eq!(rest.len(), 0);
            assert_eq!(r, assert_r);
        })
    }

    defassert_encode_decode!(assert_encode_decode_req, GlobalReq, GlobalReq);
    defassert_encode_decode!(assert_encode_decode_rep, GlobalRep, GlobalRep);

    fn dummy_key_value() -> (Key, Value) {
        (Arc::new("some key".as_bytes().to_owned()),
         Arc::new("some value".as_bytes().to_owned()))
    }

    #[test]
    fn globalreq_count() {
        assert_encode_decode_req(GlobalReq::Count);
    }

    #[test]
    fn globalreq_add() {
        let (key, value) = dummy_key_value();
        assert_encode_decode_req(GlobalReq::Add(key, value));
    }

    #[test]
    fn globalreq_lend() {
        assert_encode_decode_req(GlobalReq::Lend { timeout: 177, });
    }

    #[test]
    fn globalreq_repay_penalty() {
        let (key, value) = dummy_key_value();
        assert_encode_decode_req(GlobalReq::Repay(key, value, RepayStatus::Penalty));
    }

    #[test]
    fn globalreq_repay_reward() {
        let (key, value) = dummy_key_value();
        assert_encode_decode_req(GlobalReq::Repay(key, value, RepayStatus::Reward));
    }

    #[test]
    fn globalreq_repay_front() {
        let (key, value) = dummy_key_value();
        assert_encode_decode_req(GlobalReq::Repay(key, value, RepayStatus::Front));
    }

    #[test]
    fn globalreq_repay_drop() {
        let (key, value) = dummy_key_value();
        assert_encode_decode_req(GlobalReq::Repay(key, value, RepayStatus::Drop));
    }

    #[test]
    fn globalreq_stats() {
        assert_encode_decode_req(GlobalReq::Stats);
    }

    #[test]
    fn globalreq_terminate() {
        assert_encode_decode_req(GlobalReq::Terminate);
    }

    #[test]
    fn globalrep_counted() {
        assert_encode_decode_rep(GlobalRep::Counted(97));
    }

    #[test]
    fn globalrep_added() {
        assert_encode_decode_rep(GlobalRep::Added);
    }

    #[test]
    fn globalrep_kept() {
        assert_encode_decode_rep(GlobalRep::Kept);
    }

    #[test]
    fn globalrep_lent() {
        let (key, value) = dummy_key_value();
        assert_encode_decode_rep(GlobalRep::Lent(key, value));
    }

    #[test]
    fn globalrep_repaid() {
        assert_encode_decode_rep(GlobalRep::Repaid);
    }

    #[test]
    fn globalrep_stats() {
        assert_encode_decode_rep(GlobalRep::StatsGot { count: 177, add: 277, lend: 377, repay: 477, stats: 577, });
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqtag() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqTag { required: 177, given: 277, }));
    }

    #[test]
    fn globalrep_error_invalidglobalreqtag() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::InvalidGlobalReqTag(177)));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqaddkeylen() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqAddKeyLen { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqaddkey() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqAddKey { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqaddvaluelen() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqAddValueLen { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqaddvalue() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqAddValue { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqlendtimeout() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqLendTimeout { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqrepaykeylen() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqRepayKeyLen { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqrepaykey() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqRepayKey { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqrepayvaluelen() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqRepayValueLen { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqrepayvalue() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqRepayValue { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreqrepayrepaystatus() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalReqRepayRepayStatus { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_invalidglobalreqrepayrepaystatustag() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::InvalidGlobalReqRepayRepayStatusTag(177)));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreptag() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepTag { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_invalidglobalreptag() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::InvalidGlobalRepTag(177)));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalrepcountcount() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepCountCount { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreplentkeylen() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepLentKeyLen { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreplentkey() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepLentKey { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreplentvaluelen() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepLentValueLen { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalreplentvalue() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepLentValue { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalrepstatscount() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepStatsCount { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalrepstatsadd() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepStatsAdd { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalrepstatslend() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepStatsLend { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalrepstatsrepay() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepStatsRepay { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforglobalrepstatsstats() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForGlobalRepStatsStats { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforprotoerrortag() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForProtoErrorTag { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_invalidprotoerrortag() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::InvalidProtoErrorTag(177)));
    }

    #[test]
    fn globalrep_error_notenoughdataforprotoerrorrequired() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForProtoErrorRequired { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforprotoerrorgiven() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForProtoErrorGiven { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforprotoerrorinvalidtag() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForProtoErrorInvalidTag { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforprotoerrordbqueueoutofsynckey() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForProtoErrorDbQueueOutOfSyncKey { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_notenoughdataforprotoerrordbqueueoutofsynckeylen() {
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::NotEnoughDataForProtoErrorDbQueueOutOfSyncKeyLen { required: 177, given: 177, }));
    }

    #[test]
    fn globalrep_error_dbqueueoutofsync () {
        let (key, _) = dummy_key_value();
        assert_encode_decode_rep(GlobalRep::Error(ProtoError::DbQueueOutOfSync(key)));
    }
}
