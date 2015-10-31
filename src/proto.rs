use std::mem::size_of;
use std::slice::bytes;
use byteorder::{ByteOrder, BigEndian};

#[derive(Debug, PartialEq)]
pub enum RepayStatus {
    Penalty,
    Reward,
    Requeue,
}

#[derive(Debug, PartialEq)]
pub enum GlobalReq<'a> {
    Count,
    Add(Option<&'a [u8]>),
    Lend { timeout: u64 },
    Repay(u32, RepayStatus),
}

#[derive(Debug, PartialEq)]
pub enum LocalReq {
    Load(u32),
    Stop,
}

#[derive(Debug, PartialEq)]
pub enum Req<'a> {
    Global(GlobalReq<'a>),
    Local(LocalReq),
}

#[derive(Debug, PartialEq)]
pub enum GlobalRep<'a> {
    Count(usize),
    Added(u32),
    Lend(u32, Option<&'a [u8]>),
    Repaid,
}

#[derive(Debug, PartialEq)]
pub enum LocalRep {
    Lend(u32),
    StopAck,
}

#[derive(Debug, PartialEq)]
pub enum Rep<'a> {
    GlobalOk(GlobalRep<'a>),
    GlobalErr(ProtoError),
    Local(LocalRep),
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
    UnexpectedWorkerDbRequest,
    UnexpectedWorkerPqRequest,
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
            (tag, _) => 
                return Err(ProtoError::InvalidGlobalReqTag(tag)),
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
                    &RepayStatus::Requeue => 3,
                })
            },
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
            (2, _) => 
                Ok(LocalReq::Stop),
            (tag, _) => 
                return Err(ProtoError::InvalidLocalReqTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &LocalReq::Load(..) => size_of::<u32>(),
            &LocalReq::Stop => 0,
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &LocalReq::Load(id) => {
                let area = put_adv!(area, u8, write_u8, 1);
                put_adv!(area, u32, write_u32, id)
            },
            &LocalReq::Stop =>
                put_adv!(area, u8, write_u8, 2),
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
            &GlobalRep::Lend(_, maybe_data) => size_of::<u32>() + maybe_data.map(|data| data.len()).unwrap_or(0),
            &GlobalRep::Repaid => 0,
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &GlobalRep::Count(count) => {
                let area = put_adv!(area, u8, write_u8, 1);
                put_adv!(area, u32, write_u32, count as u32)
            },
            &GlobalRep::Added(id) => {
                let area = put_adv!(area, u8, write_u8, 2);
                put_adv!(area, u32, write_u32, id)
            },
            &GlobalRep::Lend(id, maybe_data) => {
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
            (27, _) => Ok(ProtoError::UnexpectedWorkerDbRequest),
            (28, _) => Ok(ProtoError::UnexpectedWorkerPqRequest),
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
            &ProtoError::UnexpectedWorkerDbRequest |
            &ProtoError::UnexpectedWorkerPqRequest =>
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
            &ProtoError::NotEnoughDataForRepTag { required: r, given: g, } => encode_not_enough!(area, 12, r, g),
            &ProtoError::InvalidRepTag(tag) => encode_tag!(area, 13, tag),
            &ProtoError::NotEnoughDataForGlobalRepTag { required: r, given: g, } => encode_not_enough!(area, 14, r, g),
            &ProtoError::InvalidGlobalRepTag(tag) => encode_tag!(area, 15, tag),
            &ProtoError::NotEnoughDataForGlobalRepCountCount { required: r, given: g, } => encode_not_enough!(area, 16, r, g),
            &ProtoError::NotEnoughDataForGlobalRepAddedId { required: r, given: g, } => encode_not_enough!(area, 17, r, g),
            &ProtoError::NotEnoughDataForGlobalRepLendId { required: r, given: g, } => encode_not_enough!(area, 18, r, g),
            &ProtoError::NotEnoughDataForProtoErrorTag { required: r, given: g, } => encode_not_enough!(area, 19, r, g),
            &ProtoError::InvalidProtoErrorTag(tag) => encode_tag!(area, 20, tag),
            &ProtoError::NotEnoughDataForProtoErrorRequired { required: r, given: g, } => encode_not_enough!(area, 21, r, g),
            &ProtoError::NotEnoughDataForProtoErrorGiven { required: r, given: g, } => encode_not_enough!(area, 22, r, g),
            &ProtoError::NotEnoughDataForProtoErrorInvalidTag { required: r, given: g, } => encode_not_enough!(area, 23, r, g),
            &ProtoError::NotEnoughDataForLocalRepTag { required: r, given: g, } => encode_not_enough!(area, 24, r, g),
            &ProtoError::InvalidLocalRepTag(tag) => encode_tag!(area, 25, tag),
            &ProtoError::NotEnoughDataForLocalRepLendId { required: r, given: g, } => encode_not_enough!(area, 26, r, g),
            &ProtoError::UnexpectedWorkerDbRequest => put_adv!(area, u8, write_u8, 27),
            &ProtoError::UnexpectedWorkerPqRequest => put_adv!(area, u8, write_u8, 28),
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
            (2, _) => 
                Ok(LocalRep::StopAck),
            (tag, _) => 
                return Err(ProtoError::InvalidLocalRepTag(tag)),
        }
    }

    pub fn encode_len(&self) -> usize {
        size_of::<u8>() + match self {
            &LocalRep::Lend(..) => size_of::<u32>(),
            &LocalRep::StopAck => 0,
        }
    }

    pub fn encode<'b>(&self, area: &'b mut [u8]) -> &'b mut [u8] {
        match self {
            &LocalRep::Lend(id) => {
                let area = put_adv!(area, u8, write_u8, 1);
                put_adv!(area, u32, write_u32, id)
            },
            &LocalRep::StopAck =>
                put_adv!(area, u8, write_u8, 2),
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
    fn encode_decode() {
        let some_data = "hello world".as_bytes();
        assert_encode_decode_req(Req::Global(GlobalReq::Count));
        assert_encode_decode_req(Req::Global(GlobalReq::Add(None)));
        assert_encode_decode_req(Req::Global(GlobalReq::Add(Some(some_data))));
        assert_encode_decode_req(Req::Global(GlobalReq::Lend { timeout: 177, }));
        assert_encode_decode_req(Req::Global(GlobalReq::Repay(17, RepayStatus::Penalty)));
        assert_encode_decode_req(Req::Global(GlobalReq::Repay(18, RepayStatus::Reward)));
        assert_encode_decode_req(Req::Global(GlobalReq::Repay(19, RepayStatus::Requeue)));
        assert_encode_decode_req(Req::Local(LocalReq::Load(217)));
        assert_encode_decode_req(Req::Local(LocalReq::Stop));
        
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::Count(97)));
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::Added(167)));
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::Lend(317, None)));
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::Lend(316, Some(some_data))));
        assert_encode_decode_rep(Rep::GlobalOk(GlobalRep::Repaid));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForReqTag { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidReqTag(157)));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalReqTag { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidGlobalReqTag(157)));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalReqLendTimeout { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalReqRepayId { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalReqRepayStatus { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidGlobalReqRepayStatusTag(157)));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForLocalReqTag { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidLocalReqTag(157)));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForLocalReqLoadId { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForRepTag { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidRepTag(157)));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepTag { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidGlobalRepTag(157)));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepCountCount { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepAddedId { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForGlobalRepLendId { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForProtoErrorTag { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidProtoErrorTag(157)));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForProtoErrorRequired { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForProtoErrorGiven { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForProtoErrorInvalidTag { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForLocalRepTag { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::InvalidLocalRepTag(157)));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::NotEnoughDataForLocalRepLendId { required: 177, given: 167, }));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::UnexpectedWorkerDbRequest));
        assert_encode_decode_rep(Rep::GlobalErr(ProtoError::UnexpectedWorkerPqRequest));
        assert_encode_decode_rep(Rep::Local(LocalRep::Lend(147)));
        assert_encode_decode_rep(Rep::Local(LocalRep::StopAck));
    }
}