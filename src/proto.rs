use std::mem::size_of;
use byteorder::{ByteOrder, BigEndian};
use super::pq::RepayStatus;

#[derive(Debug)]
pub enum GlobalReq<'a> {
    Count,
    Add(Option<&'a [u8]>),
    Lend { timeout: u64 },
    Repay(u32, RepayStatus),
}

#[derive(Debug)]
pub enum LocalReq {
    Load(u32),
}

#[derive(Debug)]
pub enum Req<'a> {
    Global(GlobalReq<'a>),
    Local(LocalReq),
}

#[derive(Debug)]
pub enum GlobalRep<'a> {
    Count(usize),
    Added(u32),
    Lend(u32, Option<&'a [u8]>),
    Repaid,
}

#[derive(Debug)]
pub enum LocalRep {
    Lend(u32),
}

#[derive(Debug)]
pub enum Rep<'a> {
    GlobalOk(GlobalRep<'a>),
    GlobalErr(ProtoError),
    Local(LocalRep),
}

#[derive(Debug)]
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
}

macro_rules! try_get {
    ($data:ident, $ty:ty, $reader:ident, $err:ident) => 
        (if $data.len() < size_of::<$ty>() {
            return Err(ProtoError::$err { required: size_of::<$ty>(), given: $data.len(), })
        } else {
            (BigEndian::$reader($data), &$data[size_of::<$ty>() ..])
        })
}

trait U8Support {
    fn read_u8(buf: &[u8]) -> u8;
}

impl U8Support for BigEndian {
    fn read_u8(buf: &[u8]) -> u8 { 
        buf[0] 
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
}

impl<'a> GlobalReq<'a> {
    pub fn decode(data: &'a [u8]) -> Result<GlobalReq<'a>, ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForGlobalReqTag) {
            (1, _) => Ok(GlobalReq::Count),
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
                    (3, _) => RepayStatus::Requeue,
                    (status_tag, _) => return Err(ProtoError::InvalidGlobalReqRepayStatusTag(status_tag)),
                };
                Ok(GlobalReq::Repay(id, status))
            },
            (tag, _) => return Err(ProtoError::InvalidGlobalReqTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &GlobalReq::Count => 0,
            &GlobalReq::Add(None) => 0,
            &GlobalReq::Add(Some(data)) => data.len(),
            &GlobalReq::Lend { .. } => size_of::<u64>(),
            &GlobalReq::Repay(..) => size_of::<u32>() + size_of::<u8>(),
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
            (tag, _) => return Err(ProtoError::InvalidLocalReqTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &LocalReq::Load(..) => size_of::<u32>(),
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
}

impl<'a> GlobalRep<'a> {
    pub fn decode(data: &'a [u8]) -> Result<GlobalRep<'a>, ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForGlobalRepTag) {
            (1, count_buf) => {
                let (count, _) = try_get!(count_buf, u32, read_u32, NotEnoughDataForGlobalRepCountCount);
                Ok(GlobalRep::Count(count as usize))
            },
            (2, id_buf) => {
                let (id, _) = try_get!(id_buf, u32, read_u32, NotEnoughDataForGlobalRepAddedId);
                Ok(GlobalRep::Added(id))
            },
            (3, rest_buf) => {
                let (id, lent_data) = try_get!(rest_buf, u32, read_u32, NotEnoughDataForGlobalRepLendId);
                Ok(GlobalRep::Lend(id, if lent_data.len() == 0 { None } else { Some(lent_data) }))
            },
            (4, _) => Ok(GlobalRep::Repaid),
            (tag, _) => return Err(ProtoError::InvalidGlobalRepTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &GlobalRep::Count(..) => size_of::<u32>(),
            &GlobalRep::Added(..) => size_of::<u32>(),
            &GlobalRep::Lend(_, None) => size_of::<u32>(),
            &GlobalRep::Lend(_, Some(data)) => size_of::<u32>() + data.len(),
            &GlobalRep::Repaid => 0,
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

macro_rules! decode_tag {
    ($buf:ident, $pe_type:ident) => ({
        let (tag, _) = try_get!($buf, u8, read_u8, NotEnoughDataForProtoErrorInvalidTag);
        Ok(ProtoError::$pe_type(tag))
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
            (12, buf) => decode_not_enough!(buf, NotEnoughDataForRepTag),
            (13, buf) => decode_tag!(buf, InvalidRepTag),
            (14, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepTag),
            (15, buf) => decode_tag!(buf, InvalidGlobalRepTag),
            (16, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepCountCount),
            (17, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepAddedId),
            (18, buf) => decode_not_enough!(buf, NotEnoughDataForGlobalRepLendId),
            (19, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorTag),
            (20, buf) => decode_tag!(buf, InvalidProtoErrorTag),
            (21, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorRequired),
            (22, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorGiven),
            (23, buf) => decode_not_enough!(buf, NotEnoughDataForProtoErrorInvalidTag),
            (24, buf) => decode_not_enough!(buf, NotEnoughDataForLocalRepTag),
            (25, buf) => decode_tag!(buf, InvalidLocalRepTag),
            (26, buf) => decode_not_enough!(buf, NotEnoughDataForLocalRepLendId),
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
            &ProtoError::NotEnoughDataForLocalRepLendId { .. } =>
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
        }
    }
}

impl LocalRep {
    pub fn decode(data: &[u8]) -> Result<LocalRep, ProtoError> {
        match try_get!(data, u8, read_u8, NotEnoughDataForLocalRepTag) {
            (1, id_buf) => {
                let (id, _) = try_get!(id_buf, u32, read_u32, NotEnoughDataForLocalRepLendId);
                Ok(LocalRep::Lend(id))
            },
            (tag, _) => return Err(ProtoError::InvalidLocalRepTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &LocalRep::Lend(..) => size_of::<u32>(),
        }
    }
}
