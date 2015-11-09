use std::mem::size_of;
use std::slice::bytes;
use byteorder::{ByteOrder, BigEndian};

#[derive(Debug, PartialEq)]
pub enum RepayStatus {
    Penalty,
    Reward,
    Front,
}

#[derive(Debug, PartialEq)]
pub enum GlobalReq<'a> {
    Count,
    Add(Option<&'a [u8]>),
    Lend { timeout: u64 },
    Repay(u32, RepayStatus),
    Stats,
}

#[derive(Debug, PartialEq)]
pub enum LocalReq {
    Load(u32),
    AddEnqueue(u32),
    Stop,
}

#[derive(Debug, PartialEq)]
pub enum Req<'a> {
    Global(GlobalReq<'a>),
    Local(LocalReq),
}

#[derive(Debug, PartialEq)]
pub enum GlobalRep<'a> {
    Counted(usize),
    Added(u32),
    Lent(u32, Option<&'a [u8]>),
    Repaid,
    StatsGot { count: usize, add: usize, lend: usize, repay: usize, stats: usize, },
}

#[derive(Debug, PartialEq)]
pub enum LocalRep<'a> {
    Added(u32),
    Lent(u32),
    Stopped,
    Panicked(&'a str),
}

#[derive(Debug, PartialEq)]
pub enum Rep<'a> {
    GlobalOk(GlobalRep<'a>),
    GlobalErr(ProtoError),
    Local(LocalRep<'a>),
}

#[derive(Debug, PartialEq)]
pub enum ProtoError {
    NotEnoughDataForReqTag { required: usize, given: usize, },
    InvalidReqTag(u8),
    NotEnoughDataForGlobalReqTag { required: usize, given: usize, },
    InvalidGlobalReqTag(u8),
    NotEnoughDataForGlobalReqLendTimeout { required: usize, given: usize, },
    NotEnoughDataForGlobalReqRepayId { required: usize, given: usize, },
    NotEnoughDataForGlobalReqRepayStatus { required: usize, given: usize, },
    InvalidGlobalReqRepayStatusTag(u8),
    NotEnoughDataForLocalReqTag { required: usize, given: usize, },
    InvalidLocalReqTag(u8),
    NotEnoughDataForLocalReqLoadId { required: usize, given: usize, },
    NotEnoughDataForLocalReqAddEnqueueId { required: usize, given: usize, },
    NotEnoughDataForRepTag { required: usize, given: usize, },
    InvalidRepTag(u8),
    NotEnoughDataForGlobalRepTag { required: usize, given: usize, },
    InvalidGlobalRepTag(u8),
    NotEnoughDataForGlobalRepCountCount { required: usize, given: usize, },
    NotEnoughDataForGlobalRepAddedId { required: usize, given: usize, },
    NotEnoughDataForGlobalRepLendId { required: usize, given: usize, },
    NotEnoughDataForProtoErrorTag { required: usize, given: usize, },
    InvalidProtoErrorTag(u8),
    NotEnoughDataForProtoErrorRequired { required: usize, given: usize, },
    NotEnoughDataForProtoErrorGiven { required: usize, given: usize, },
    NotEnoughDataForProtoErrorInvalidTag { required: usize, given: usize, },
    NotEnoughDataForLocalRepTag { required: usize, given: usize, },
    InvalidLocalRepTag(u8),
    NotEnoughDataForLocalRepLendId { required: usize, given: usize, },
    NotEnoughDataForLocalRepAddId { required: usize, given: usize, },
    UnexpectedWorkerDbRequest,
    UnexpectedWorkerPqRequest,
    UnexpectedMasterRequest,
    NotEnoughDataForGlobalRepStatsCount { required: usize, given: usize, },
    NotEnoughDataForGlobalRepStatsAdd { required: usize, given: usize, },
    NotEnoughDataForGlobalRepStatsLend { required: usize, given: usize, },
    NotEnoughDataForGlobalRepStatsRepay { required: usize, given: usize, },
    NotEnoughDataForGlobalRepStatsStats { required: usize, given: usize, },
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

impl<'a> Req<'a> {
    pub fn decode(data: &'a [u8]) -> Result<Req<'a>, ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForReqTag) {
            (1, packet) => Ok(Req::Global(try!(GlobalReq::decode(packet)))),
            (2, packet) => Ok(Req::Local(try!(LocalReq::decode(packet)))),
            (tag, _) => return Err(ProtoError::InvalidReqTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &Req::Global(ref req) => req.encode_len(),
            &Req::Local(ref req) => req.encode_len(),
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &Req::Global(ref req) => req.encode(put_adv!(area, u8, write_u8, 1)),
            &Req::Local(ref req) => req.encode(put_adv!(area, u8, write_u8, 2)),
        }
    }
}

impl<'a> GlobalReq<'a> {
    pub fn decode(data: &'a [u8]) -> Result<GlobalReq<'a>, ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForGlobalReqTag) {
            (1, _) => 
                Ok(GlobalReq::Count),
            (2, data_to_add) if data_to_add.len() == 0 => 
                Ok(GlobalReq::Add(None)),
            (2, data_to_add) => 
                Ok(GlobalReq::Add(Some(data_to_add))),
            (3, timeout_buf) => { 
                let (timeout, _) = try_get!(timeout_buf, u64, read_u64, NotEnoughDataForGlobalReqLendTimeout);
                Ok(GlobalReq::Lend { timeout: timeout, })
            },
            (4, buf) => { 
                let (id, status_buf) = try_get!(buf, u32, read_u32, NotEnoughDataForGlobalReqRepayId);
                let status = match try_get!(status_buf, u8, read_u8, NotEnoughDataForGlobalReqRepayStatus) {
                    (1, _) => RepayStatus::Penalty,
                    (2, _) => RepayStatus::Reward,
                    (3, _) => RepayStatus::Front,
                    (status_tag, _) => return Err(ProtoError::InvalidGlobalReqRepayStatusTag(status_tag)),
                };
                Ok(GlobalReq::Repay(id, status))
            },
            (5, _) => 
                Ok(GlobalReq::Stats),
            (tag, _) => 
                return Err(ProtoError::InvalidGlobalReqTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &GlobalReq::Count | &GlobalReq::Stats => 0,
            &GlobalReq::Add(None) => 0,
            &GlobalReq::Add(Some(data)) => data.len(),
            &GlobalReq::Lend { .. } => size_of::<u64>(),
            &GlobalReq::Repay(..) => size_of::<u32>() + size_of::<u8>(),
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &GlobalReq::Count =>
                put_adv!(area, u8, write_u8, 1),
            &GlobalReq::Add(None) =>
                put_adv!(area, u8, write_u8, 2),
            &GlobalReq::Add(Some(data)) => {
                let area = put_adv!(area, u8, write_u8, 2);
                bytes::copy_memory(data, area);
                &mut area[data.len() ..]
            },
            &GlobalReq::Lend { timeout: t } => {
                let area = put_adv!(area, u8, write_u8, 3);
                put_adv!(area, u64, write_u64, t)
            },
            &GlobalReq::Repay(id, ref status) => {
                let area = put_adv!(area, u8, write_u8, 4);
                let area = put_adv!(area, u32, write_u32, id);
                put_adv!(area, u8, write_u8, match status {
                    &RepayStatus::Penalty => 1,
                    &RepayStatus::Reward => 2,
                    &RepayStatus::Front => 3,
                })
            },
            &GlobalReq::Stats =>
                put_adv!(area, u8, write_u8, 5),
        }
    }
}

impl LocalReq {
    pub fn decode(data: &[u8]) -> Result<LocalReq, ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForLocalReqTag) {
            (1, id_buf) => {
                let (id, _) = try_get!(id_buf, u32, read_u32, NotEnoughDataForLocalReqLoadId);
                Ok(LocalReq::Load(id))
            },
            (2, id_buf) => {
                let (id, _) = try_get!(id_buf, u32, read_u32, NotEnoughDataForLocalReqAddEnqueueId);
                Ok(LocalReq::AddEnqueue(id))
            },
            (3, _) => 
                Ok(LocalReq::Stop),
            (tag, _) => 
                return Err(ProtoError::InvalidLocalReqTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &LocalReq::Load(..) | &LocalReq::AddEnqueue(..) => size_of::<u32>(),
            &LocalReq::Stop => 0,
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &LocalReq::Load(id) => {
                let area = put_adv!(area, u8, write_u8, 1);
                put_adv!(area, u32, write_u32, id)
            },
            &LocalReq::AddEnqueue(id) => {
                let area = put_adv!(area, u8, write_u8, 2);
                put_adv!(area, u32, write_u32, id)
            },
            &LocalReq::Stop =>
                put_adv!(area, u8, write_u8, 3),
        }
    }
}

impl<'a> Rep<'a> {
    pub fn decode(data: &'a [u8]) -> Result<Rep<'a>, ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForRepTag) {
            (1, packet) => Ok(Rep::GlobalOk(try!(GlobalRep::decode(packet)))),
            (2, packet) => Ok(Rep::GlobalErr(try!(ProtoError::decode(packet)))),
            (3, packet) => Ok(Rep::Local(try!(LocalRep::decode(packet)))),
            (tag, _) => return Err(ProtoError::InvalidRepTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &Rep::GlobalOk(ref rep) => rep.encode_len(),
            &Rep::GlobalErr(ref err) => err.encode_len(),
            &Rep::Local(ref rep) => rep.encode_len(),
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &Rep::GlobalOk(ref rep) => rep.encode(put_adv!(area, u8, write_u8, 1)),
            &Rep::GlobalErr(ref err) => err.encode(put_adv!(area, u8, write_u8, 2)),
            &Rep::Local(ref rep) => rep.encode(put_adv!(area, u8, write_u8, 3)),
        }
    }
}

impl<'a> GlobalRep<'a> {
    pub fn decode(data: &'a [u8]) -> Result<GlobalRep<'a>, ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForGlobalRepTag) {
            (1, count_buf) => {
                let (count, _) = try_get!(count_buf, u32, read_u32, NotEnoughDataForGlobalRepCountCount);
                Ok(GlobalRep::Counted(count as usize))
            },
            (2, id_buf) => {
                let (id, _) = try_get!(id_buf, u32, read_u32, NotEnoughDataForGlobalRepAddedId);
                Ok(GlobalRep::Added(id))
            },
            (3, rest_buf) => {
                let (id, lent_data) = try_get!(rest_buf, u32, read_u32, NotEnoughDataForGlobalRepLendId);
                Ok(GlobalRep::Lent(id, if lent_data.len() == 0 { None } else { Some(lent_data) }))
            },
            (4, _) => 
                Ok(GlobalRep::Repaid),
            (5, buf) => {
                let (stats_count, buf) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalRepStatsCount);
                let (stats_add, buf) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalRepStatsAdd);
                let (stats_lend, buf) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalRepStatsLend);
                let (stats_repay, buf) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalRepStatsRepay);
                let (stats_stats, _) = try_get!(buf, u64, read_u64, NotEnoughDataForGlobalRepStatsStats);
                Ok(GlobalRep::StatsGot { count: stats_count as usize,
                                         add: stats_add as usize,
                                         lend: stats_lend as usize,
                                         repay: stats_repay as usize,
                                         stats: stats_stats as usize, })
            },
            (tag, _) => 
                return Err(ProtoError::InvalidGlobalRepTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &GlobalRep::Counted(..) => size_of::<u32>(),
            &GlobalRep::Added(..) => size_of::<u32>(),
            &GlobalRep::Lent(_, maybe_data) => size_of::<u32>() + maybe_data.map(|data| data.len()).unwrap_or(0),
            &GlobalRep::Repaid => 0,
            &GlobalRep::StatsGot { .. } => size_of::<u64>() * 5,
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &GlobalRep::Counted(count) => {
                let area = put_adv!(area, u8, write_u8, 1);
                put_adv!(area, u32, write_u32, count as u32)
            },
            &GlobalRep::Added(id) => {
                let area = put_adv!(area, u8, write_u8, 2);
                put_adv!(area, u32, write_u32, id)
            },
            &GlobalRep::Lent(id, maybe_data) => {
                let area = put_adv!(area, u8, write_u8, 3);
                let area = put_adv!(area, u32, write_u32, id);
                if let Some(data) = maybe_data {
                    bytes::copy_memory(data, area);
                    &mut area[data.len() ..]
                } else {
                    area
                }
            },
            &GlobalRep::Repaid =>
                put_adv!(area, u8, write_u8, 4),
            &GlobalRep::StatsGot { count: stats_count, add: stats_add, lend: stats_lend, repay: stats_repay, stats: stats_stats, } => {
                let area = put_adv!(area, u8, write_u8, 5);
                let area = put_adv!(area, u64, write_u64, stats_count as u64);
                let area = put_adv!(area, u64, write_u64, stats_add as u64);
                let area = put_adv!(area, u64, write_u64, stats_lend as u64);
                let area = put_adv!(area, u64, write_u64, stats_repay as u64);
                let area = put_adv!(area, u64, write_u64, stats_stats as u64);
                area
            },
        }
    }
}

macro_rules! decode_not_enough {
    ($buf:ident, $pe_type:ident) => ({
        let (required, given_buf) = try_get!($buf, u32, read_u32, NotEnoughDataForProtoErrorRequired);
        let (given, _) = try_get!(given_buf, u32, read_u32, NotEnoughDataForProtoErrorGiven);
        Ok(ProtoError::$pe_type { required: required as usize, given: given as usize, })
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
        let (tag, _) = try_get!($buf, u8, read_u8, NotEnoughDataForProtoErrorInvalidTag);
        Ok(ProtoError::$pe_type(tag))
    })
}

macro_rules! encode_tag {
    ($area:ident, $tag:expr, $invalid_tag:expr) => ({
        let area = put_adv!($area, u8, write_u8, $tag);
        put_adv!(area, u8, write_u8, $invalid_tag)
    })
}

impl ProtoError {
    pub fn decode(data: &[u8]) -> Result<ProtoError, ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForProtoErrorTag) {
            (1, buf) => decode_not_enough!(buf, NotEnoughDataForReqTag),
            (2, buf) => decode_tag!(buf, InvalidReqTag),
            (3, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqTag),
            (4, buf) => decode_tag!(buf, InvalidGlobalReqTag),
            (5, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqLendTimeout),
            (6, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqRepayId),
            (7, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalReqRepayStatus),
            (8, buf) => decode_tag!(buf, InvalidGlobalReqRepayStatusTag),
            (9, buf) => decode_not_enough!(buf, NotEnoughDataForLocalReqTag),
            (10, buf) => decode_tag!(buf, InvalidLocalReqTag),
            (11, buf) => decode_not_enough!(buf, NotEnoughDataForLocalReqLoadId),
            (12, buf) => decode_not_enough!(buf, NotEnoughDataForLocalReqAddEnqueueId),
            (13, buf) => decode_not_enough!(buf, NotEnoughDataForRepTag),
            (14, buf) => decode_tag!(buf, InvalidRepTag),
            (15, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepTag),
            (16, buf) => decode_tag!(buf, InvalidGlobalRepTag),
            (17, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepCountCount),
            (18, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepAddedId),
            (19, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepLendId),
            (20, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorTag),
            (21, buf) => decode_tag!(buf, InvalidProtoErrorTag),
            (22, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorRequired),
            (23, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorGiven),
            (24, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorInvalidTag),
            (25, buf) => decode_not_enough!(buf, NotEnoughDataForLocalRepTag),
            (26, buf) => decode_tag!(buf, InvalidLocalRepTag),
            (27, buf) => decode_not_enough!(buf, NotEnoughDataForLocalRepLendId),
            (28, buf) => decode_not_enough!(buf, NotEnoughDataForLocalRepAddId),
            (29, _) => Ok(ProtoError::UnexpectedWorkerDbRequest),
            (30, _) => Ok(ProtoError::UnexpectedWorkerPqRequest),
            (31, _) => Ok(ProtoError::UnexpectedMasterRequest),
            (32, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepStatsCount),
            (33, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepStatsAdd),
            (34, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepStatsLend),
            (35, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepStatsRepay),
            (36, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepStatsStats),
            (tag, _) => return Err(ProtoError::InvalidProtoErrorTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &ProtoError::NotEnoughDataForReqTag { .. } |
            &ProtoError::NotEnoughDataForGlobalReqTag { .. } |
            &ProtoError::NotEnoughDataForGlobalReqLendTimeout { .. } |
            &ProtoError::NotEnoughDataForGlobalReqRepayId { .. } |
            &ProtoError::NotEnoughDataForGlobalReqRepayStatus { .. } |
            &ProtoError::NotEnoughDataForLocalReqTag { .. } |
            &ProtoError::NotEnoughDataForLocalReqLoadId { .. } |
            &ProtoError::NotEnoughDataForLocalReqAddEnqueueId { .. } |
            &ProtoError::NotEnoughDataForRepTag { .. } |
            &ProtoError::NotEnoughDataForGlobalRepTag { .. } |
            &ProtoError::NotEnoughDataForGlobalRepCountCount { .. } |
            &ProtoError::NotEnoughDataForGlobalRepAddedId { .. } |
            &ProtoError::NotEnoughDataForGlobalRepLendId { .. } |
            &ProtoError::NotEnoughDataForProtoErrorTag { .. } |
            &ProtoError::NotEnoughDataForProtoErrorRequired { .. } |
            &ProtoError::NotEnoughDataForProtoErrorGiven { .. } |
            &ProtoError::NotEnoughDataForProtoErrorInvalidTag { .. } |
            &ProtoError::NotEnoughDataForLocalRepTag { .. } |
            &ProtoError::NotEnoughDataForLocalRepLendId { .. } |
            &ProtoError::NotEnoughDataForLocalRepAddId { .. } |
            &ProtoError::NotEnoughDataForGlobalRepStatsCount { .. } |
            &ProtoError::NotEnoughDataForGlobalRepStatsAdd { .. } |
            &ProtoError::NotEnoughDataForGlobalRepStatsLend { .. } |
            &ProtoError::NotEnoughDataForGlobalRepStatsRepay { .. } |
            &ProtoError::NotEnoughDataForGlobalRepStatsStats { .. } =>
                size_of::<u32>() + size_of::<u32>(),
            &ProtoError::InvalidReqTag(..) |
            &ProtoError::InvalidGlobalReqTag(..) |
            &ProtoError::InvalidGlobalReqRepayStatusTag(..) |
            &ProtoError::InvalidLocalReqTag(..) |
            &ProtoError::InvalidRepTag(..) |
            &ProtoError::InvalidGlobalRepTag(..) |
            &ProtoError::InvalidProtoErrorTag(..) |
            &ProtoError::InvalidLocalRepTag(..) =>
                size_of::<u8>(),
            &ProtoError::UnexpectedWorkerDbRequest |
            &ProtoError::UnexpectedWorkerPqRequest |
            &ProtoError::UnexpectedMasterRequest =>
                0,
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &ProtoError::NotEnoughDataForReqTag { required: r, given: g, } => encode_not_enough!(area, 1, r, g),
            &ProtoError::InvalidReqTag(tag) => encode_tag!(area, 2, tag),
            &ProtoError::NotEnoughDataForGlobalReqTag { required: r, given: g, } => encode_not_enough!(area, 3, r, g),
            &ProtoError::InvalidGlobalReqTag(tag) => encode_tag!(area, 4, tag),
            &ProtoError::NotEnoughDataForGlobalReqLendTimeout { required: r, given: g, } => encode_not_enough!(area, 5, r, g),
            &ProtoError::NotEnoughDataForGlobalReqRepayId { required: r, given: g, } => encode_not_enough!(area, 6, r, g),
            &ProtoError::NotEnoughDataForGlobalReqRepayStatus { required: r, given: g, } => encode_not_enough!(area, 7, r, g),
            &ProtoError::InvalidGlobalReqRepayStatusTag(tag) => encode_tag!(area, 8, tag),
            &ProtoError::NotEnoughDataForLocalReqTag { required: r, given: g, } => encode_not_enough!(area, 9, r, g),
            &ProtoError::InvalidLocalReqTag(tag) => encode_tag!(area, 10, tag),
            &ProtoError::NotEnoughDataForLocalReqLoadId { required: r, given: g, } => encode_not_enough!(area, 11, r, g),
            &ProtoError::NotEnoughDataForLocalReqAddEnqueueId { required: r, given: g, } => encode_not_enough!(area, 12, r, g),
            &ProtoError::NotEnoughDataForRepTag { required: r, given: g, } => encode_not_enough!(area, 13, r, g),
            &ProtoError::InvalidRepTag(tag) => encode_tag!(area, 14, tag),
            &ProtoError::NotEnoughDataForGlobalRepTag { required: r, given: g, } => encode_not_enough!(area, 15, r, g),
            &ProtoError::InvalidGlobalRepTag(tag) => encode_tag!(area, 16, tag),
            &ProtoError::NotEnoughDataForGlobalRepCountCount { required: r, given: g, } => encode_not_enough!(area, 17, r, g),
            &ProtoError::NotEnoughDataForGlobalRepAddedId { required: r, given: g, } => encode_not_enough!(area, 18, r, g),
            &ProtoError::NotEnoughDataForGlobalRepLendId { required: r, given: g, } => encode_not_enough!(area, 19, r, g),
            &ProtoError::NotEnoughDataForProtoErrorTag { required: r, given: g, } => encode_not_enough!(area, 20, r, g),
            &ProtoError::InvalidProtoErrorTag(tag) => encode_tag!(area, 21, tag),
            &ProtoError::NotEnoughDataForProtoErrorRequired { required: r, given: g, } => encode_not_enough!(area, 22, r, g),
            &ProtoError::NotEnoughDataForProtoErrorGiven { required: r, given: g, } => encode_not_enough!(area, 23, r, g),
            &ProtoError::NotEnoughDataForProtoErrorInvalidTag { required: r, given: g, } => encode_not_enough!(area, 24, r, g),
            &ProtoError::NotEnoughDataForLocalRepTag { required: r, given: g, } => encode_not_enough!(area, 25, r, g),
            &ProtoError::InvalidLocalRepTag(tag) => encode_tag!(area, 26, tag),
            &ProtoError::NotEnoughDataForLocalRepLendId { required: r, given: g, } => encode_not_enough!(area, 27, r, g),
            &ProtoError::NotEnoughDataForLocalRepAddId { required: r, given: g, } => encode_not_enough!(area, 28, r, g),
            &ProtoError::UnexpectedWorkerDbRequest => put_adv!(area, u8, write_u8, 29),
            &ProtoError::UnexpectedWorkerPqRequest => put_adv!(area, u8, write_u8, 30),
            &ProtoError::UnexpectedMasterRequest => put_adv!(area, u8, write_u8, 31),
            &ProtoError::NotEnoughDataForGlobalRepStatsCount { required: r, given: g, } => encode_not_enough!(area, 32, r, g),
            &ProtoError::NotEnoughDataForGlobalRepStatsAdd { required: r, given: g, } => encode_not_enough!(area, 33, r, g),
            &ProtoError::NotEnoughDataForGlobalRepStatsLend { required: r, given: g, } => encode_not_enough!(area, 34, r, g),
            &ProtoError::NotEnoughDataForGlobalRepStatsRepay { required: r, given: g, } => encode_not_enough!(area, 35, r, g),
            &ProtoError::NotEnoughDataForGlobalRepStatsStats { required: r, given: g, } => encode_not_enough!(area, 36, r, g),
        }
    }
}

impl<'a> LocalRep<'a> {
    pub fn decode(data: &'a [u8]) -> Result<LocalRep<'a>, ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForLocalRepTag) {
            (1, id_buf) => {
                let (id, _) = try_get!(id_buf, u32, read_u32, NotEnoughDataForLocalRepLendId);
                Ok(LocalRep::Lent(id))
            },
            (2, id_buf) => {
                let (id, _) = try_get!(id_buf, u32, read_u32, NotEnoughDataForLocalRepAddId);
                Ok(LocalRep::Added(id))
            },
            (3, _) => 
                Ok(LocalRep::Stopped),
            (4, msg_buf) =>
                Ok(LocalRep::Panicked(unsafe { ::std::str::from_utf8_unchecked(msg_buf) })),
            (tag, _) => 
                return Err(ProtoError::InvalidLocalRepTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &LocalRep::Lent(..) | &LocalRep::Added(..) => size_of::<u32>(),
            &LocalRep::Stopped => 0,
            &LocalRep::Panicked(ref msg) => msg.as_bytes().len(),
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &LocalRep::Lent(id) => {
                let area = put_adv!(area, u8, write_u8, 1);
                put_adv!(area, u32, write_u32, id)
            },
            &LocalRep::Added(id) => {
                let area = put_adv!(area, u8, write_u8, 2);
                put_adv!(area, u32, write_u32, id)
            },
            &LocalRep::Stopped =>
                put_adv!(area, u8, write_u8, 3),
            &LocalRep::Panicked(ref msg) => {
                let area = put_adv!(area, u8, write_u8, 4);
                let msg_bytes = msg.as_bytes();
                bytes::copy_memory(msg_bytes, area);
                &mut area[msg_bytes.len() ..]
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::{RepayStatus, Req, GlobalReq, LocalReq, Rep, GlobalRep, LocalRep, ProtoError};

    macro_rules! defassert_encode_decode {
        ($name:ident, $ty:ty, $class:ident) => (fn $name(r: $ty) {
            let bytes_required = r.encode_len();
            let mut area: Vec<_> = (0 .. bytes_required).map(|_| 0).collect();
            assert!(r.encode(&mut area).len() == 0);
            let assert_r = $class::decode(&area).unwrap();
            assert_eq!(r, assert_r);
        })
    }

    defassert_encode_decode!(assert_encode_decode_req, Req, Req);
    defassert_encode_decode!(assert_encode_decode_rep, Rep, Rep);

    #[test]
    fn req_global_globalreq_count() {
        assert_encode_decode_req(Req::Global(GlobalReq::Count));
    }

    #[test]
    fn req_global_globalreq_add_none() {
        assert_encode_decode_req(Req::Global(GlobalReq::Add(None)));
    }

    #[test]
    fn req_global_globalreq_add_some() {
        let some_data = "hello world".as_bytes();
        assert_encode_decode_req(Req::Global(GlobalReq::Add(Some(some_data))));
    }

    #[test]
    fn req_global_globalreq_lend() {
        assert_encode_decode_req(Req::Global(GlobalReq::Lend { timeout: 177, }));
    }

    #[test]
    fn req_global_globalreq_repay_penalty() {
        assert_encode_decode_req(Req::Global(GlobalReq::Repay(17, RepayStatus::Penalty)));
    }

    #[test]
    fn req_global_globalreq_repay_reward() {
        assert_encode_decode_req(Req::Global(GlobalReq::Repay(18, RepayStatus::Reward)));
    }

    #[test]
    fn req_global_globalreq_repay_front() {
        assert_encode_decode_req(Req::Global(GlobalReq::Repay(19, RepayStatus::Front)));
    }

    #[test]
    fn req_global_globalreq_stats() {
        assert_encode_decode_req(Req::Global(GlobalReq::Stats));
    }

    #[test]
    fn req_local_localreq_load() {
        assert_encode_decode_req(Req::Local(LocalReq::Load(217)));
    }

    #[test]
    fn req_local_localreq_addenqueue() {
        assert_encode_decode_req(Req::Local(LocalReq::AddEnqueue(597)));
    }

    #[test]
    fn req_local_localreq_stop() {
        assert_encode_decode_req(Req::Local(LocalReq::Stop));
    }

    #[test]
    fn rep_globalok_globalrep_count() {
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::Counted(97)));
    }

    #[test]
    fn rep_globalok_globalrep_added() {
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::Added(167)));
    }

    #[test]
    fn rep_globalok_globalrep_lend_none() {
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::Lent(317, None)));
    }

    #[test]
    fn rep_globalok_globalrep_lend_some() {
        let some_data = "hello world".as_bytes();
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::Lent(316, Some(some_data))));
    }

    #[test]
    fn rep_globalok_globalrep_repaid() {
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::Repaid));
    }

    #[test]
    fn rep_globalok_globalrep_stats() {
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::StatsGot { count: 177, add: 277, lend: 377, repay: 477, stats: 577, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforreqtag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForReqTag { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_invalidreqtag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidReqTag(157)));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalreqtag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalReqTag { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_invalidglobalreqtag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidGlobalReqTag(157)));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalreqlendtimeout() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalReqLendTimeout { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalreqrepayid() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalReqRepayId { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalreqrepaystatus() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalReqRepayStatus { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_invalidglobalreqrepaystatustag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidGlobalReqRepayStatusTag(157)));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforlocalreqtag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForLocalReqTag { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_invalidlocalreqtag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidLocalReqTag(157)));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforlocalreqloadid() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForLocalReqLoadId { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforlocalreqaddenqueueid() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForLocalReqAddEnqueueId { required: 137, given: 467, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforreptag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForRepTag { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_invalidreptag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidRepTag(157)));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalreptag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepTag { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_invalidglobalreptag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidGlobalRepTag(157)));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalrepcountcount() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepCountCount { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalrepaddedid() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepAddedId { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalreplendid() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepLendId { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforprotoerrortag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForProtoErrorTag { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_invalidprotoerrortag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidProtoErrorTag(157)));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforprotoerrorrequired() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForProtoErrorRequired { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforprotoerrorgiven() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForProtoErrorGiven { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforprotoerrorinvalidtag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForProtoErrorInvalidTag { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforlocalreptag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForLocalRepTag { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_invalidlocalreptag() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidLocalRepTag(157)));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforlocalreplendid() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForLocalRepLendId { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforlocalrepaddid() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForLocalRepAddId { required: 337, given: 27, }));
    }

    #[test]
    fn rep_globalerr_protoerror_unexpectedworkerdbrequest() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::UnexpectedWorkerDbRequest));
    }

    #[test]
    fn rep_globalerr_protoerror_unexpectedworkerpqrequest() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::UnexpectedWorkerPqRequest));
    }

    #[test]
    fn rep_globalerr_protoerror_unexpectedmasterrequest() {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::UnexpectedMasterRequest));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalrepstatscount () {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepStatsCount { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalrepstatsadd () {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepStatsAdd { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalrepstatslend () {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepStatsLend { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalrepstatsrepay () {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepStatsRepay { required: 177, given: 167, }));
    }

    #[test]
    fn rep_globalerr_protoerror_notenoughdataforglobalrepstatsstats () {
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepStatsStats { required: 177, given: 167, }));
    }

    #[test]
    fn rep_local_localrep_lend() {
        assert_encode_decode_rep(Rep::Local(LocalRep::Lent(147)));
    }

    #[test]
    fn rep_local_localrep_add() {
        assert_encode_decode_rep(Rep::Local(LocalRep::Added(87)));
    }

    #[test]
    fn rep_local_localrep_stopack() {
        assert_encode_decode_rep(Rep::Local(LocalRep::Stopped));
    }

    #[test]
    fn rep_local_localrep_panic() {
        assert_encode_decode_rep(Rep::Local(LocalRep::Panicked("some panic message")));
    }
}
