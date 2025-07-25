use io::{Read, ProtoRead, Write, ProtoWrite, Error as IoError};

#[derive(Fail, Debug)]
pub enum Error<T> {
    #[fail(display = "unknown packet {:#02x}", _0)]
    UnknownPacket(u8),
    #[fail(display = "{}", _0)]
    Io(#[cause] IoError<T>)
}

impl<T> From<IoError<T>> for Error<T> {
    fn from(value: IoError<T>) -> Error<T> {
        Error::Io(value)
    }
}

const MAX_PACKET: usize = 1024;
// maximum size of arbitrary payloads
// used by satellite -> master CoaXPress communication and errors
pub const CXP_PAYLOAD_MAX_SIZE: usize = /*max size*/
    MAX_PACKET - /*packet ID*/1 - /*length*/2 - /*CRC*/4 - /*padding to keep CXP register access 4 bytes align*/1;
// used by satellite -> master CoaXPress roi viewer pixel data transfer
pub const CXP_PAYLOAD_MAX_SIZE_U64: usize = CXP_PAYLOAD_MAX_SIZE / 8;
// used by satellite -> master analyzer, subkernel exceptions
pub const SAT_PAYLOAD_MAX_SIZE: usize  = /*max size*/MAX_PACKET - /*CRC*/4 - /*packet ID*/1 - /*last*/1 - /*length*/2;
// used by DDMA, subkernel program data (need to provide extra ID and destination)
pub const MASTER_PAYLOAD_MAX_SIZE: usize = SAT_PAYLOAD_MAX_SIZE - /*source*/1 - /*destination*/1 - /*ID*/4;

#[derive(PartialEq, Clone, Copy, Debug)]
#[repr(u8)]
pub enum PayloadStatus {
    Middle = 0,
    First = 1,
    Last = 2,
    FirstAndLast = 3,
}

impl From<u8> for PayloadStatus {
    fn from(value: u8) -> PayloadStatus {
        match value {
            0 => PayloadStatus::Middle,
            1 => PayloadStatus::First,
            2 => PayloadStatus::Last,
            3 => PayloadStatus::FirstAndLast,
            _ => unreachable!(),
        }
    }
}

impl PayloadStatus {
    pub fn is_first(self) -> bool {
        self == PayloadStatus::First || self == PayloadStatus::FirstAndLast
    }

    pub fn is_last(self) -> bool {
        self == PayloadStatus::Last || self == PayloadStatus::FirstAndLast
    }

    pub fn from_status(first: bool, last: bool) -> PayloadStatus {
        match (first, last) {
            (true, true) => PayloadStatus::FirstAndLast,
            (true, false) => PayloadStatus::First,
            (false, true) => PayloadStatus::Last,
            (false, false) => PayloadStatus::Middle
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum Packet {
    EchoRequest,
    EchoReply,
    ResetRequest,
    ResetAck,
    TSCAck,

    DestinationStatusRequest { destination: u8 },
    DestinationDownReply,
    DestinationOkReply,
    DestinationSequenceErrorReply { channel: u16 },
    DestinationCollisionReply { channel: u16 },
    DestinationBusyReply { channel: u16 },

    RoutingSetPath { destination: u8, hops: [u8; 32] },
    RoutingSetRank { rank: u8 },
    RoutingAck,

    MonitorRequest { destination: u8, channel: u16, probe: u8 },
    MonitorReply { value: u64 },
    InjectionRequest { destination: u8, channel: u16, overrd: u8, value: u8 },
    InjectionStatusRequest { destination: u8, channel: u16, overrd: u8 },
    InjectionStatusReply { value: u8 },

    I2cStartRequest { destination: u8, busno: u8 },
    I2cRestartRequest { destination: u8, busno: u8 },
    I2cStopRequest { destination: u8, busno: u8 },
    I2cWriteRequest { destination: u8, busno: u8, data: u8 },
    I2cWriteReply { succeeded: bool, ack: bool },
    I2cReadRequest { destination: u8, busno: u8, ack: bool },
    I2cReadReply { succeeded: bool, data: u8 },
    I2cBasicReply { succeeded: bool },
    I2cSwitchSelectRequest { destination: u8, busno: u8, address: u8, mask: u8 },

    SpiSetConfigRequest { destination: u8, busno: u8, flags: u8, length: u8, div: u8, cs: u8 },
    SpiWriteRequest { destination: u8, busno: u8, data: u32 },
    SpiReadRequest { destination: u8, busno: u8 },
    SpiReadReply { succeeded: bool, data: u32 },
    SpiBasicReply { succeeded: bool },

    AnalyzerHeaderRequest { destination: u8 },
    AnalyzerHeader { sent_bytes: u32, total_byte_count: u64, overflow_occurred: bool },
    AnalyzerDataRequest { destination: u8 },
    AnalyzerData { last: bool, length: u16, data: [u8; SAT_PAYLOAD_MAX_SIZE]},

    DmaAddTraceRequest { 
        source: u8, destination: u8, 
        id: u32, status: PayloadStatus, 
        length: u16, trace: [u8; MASTER_PAYLOAD_MAX_SIZE] 
    },
    DmaAddTraceReply { source: u8, destination: u8, id: u32, succeeded: bool },
    DmaRemoveTraceRequest { source: u8, destination: u8, id: u32 },
    DmaRemoveTraceReply { destination: u8, succeeded: bool },
    DmaPlaybackRequest { source: u8, destination: u8, id: u32, timestamp: u64 },
    DmaPlaybackReply { destination: u8, succeeded: bool },
    DmaPlaybackStatus { source: u8, destination: u8, id: u32, error: u8, channel: u32, timestamp: u64 },

    SubkernelAddDataRequest { destination: u8, id: u32, status: PayloadStatus, length: u16, data: [u8; MASTER_PAYLOAD_MAX_SIZE] },
    SubkernelAddDataReply { succeeded: bool },
    SubkernelLoadRunRequest { source: u8, destination: u8, id: u32, run: bool, timestamp: u64 },
    SubkernelLoadRunReply { destination: u8, succeeded: bool },
    SubkernelFinished { destination: u8, id: u32, with_exception: bool, exception_src: u8 },
    SubkernelExceptionRequest { source: u8, destination: u8 },
    SubkernelException { destination: u8, last: bool, length: u16, data: [u8; MASTER_PAYLOAD_MAX_SIZE] },
    SubkernelMessage { source: u8, destination: u8, id: u32, status: PayloadStatus, length: u16, data: [u8; MASTER_PAYLOAD_MAX_SIZE] },
    SubkernelMessageAck { destination: u8 },

    CoreMgmtGetLogRequest { destination: u8, clear: bool },
    CoreMgmtClearLogRequest { destination: u8 },
    CoreMgmtSetLogLevelRequest { destination: u8, log_level: u8 },
    CoreMgmtSetUartLogLevelRequest { destination: u8, log_level: u8 },
    CoreMgmtConfigReadRequest { destination: u8, length: u16, key: [u8; MASTER_PAYLOAD_MAX_SIZE] },
    CoreMgmtConfigReadContinue { destination: u8 },
    CoreMgmtConfigWriteRequest { destination: u8, last: bool, length: u16, data: [u8; MASTER_PAYLOAD_MAX_SIZE] },
    CoreMgmtConfigRemoveRequest { destination: u8, length: u16, key: [u8; MASTER_PAYLOAD_MAX_SIZE] },
    CoreMgmtConfigEraseRequest { destination: u8 },
    CoreMgmtRebootRequest { destination: u8 },
    CoreMgmtAllocatorDebugRequest { destination: u8 },
    CoreMgmtFlashRequest { destination: u8, payload_length: u32 },
    CoreMgmtFlashAddDataRequest { destination: u8, last: bool, length: u16, data: [u8; MASTER_PAYLOAD_MAX_SIZE] },
    CoreMgmtDropLinkAck { destination: u8 },
    CoreMgmtDropLink,
    CoreMgmtGetLogReply { last: bool, length: u16, data: [u8; SAT_PAYLOAD_MAX_SIZE] },
    CoreMgmtConfigReadReply { last: bool, length: u16, value: [u8; SAT_PAYLOAD_MAX_SIZE] },
    CoreMgmtReply { succeeded: bool },
    CXPError { length: u16, message: [u8; CXP_PAYLOAD_MAX_SIZE] },
    CXPWaitReply,
    CXPReadRequest { destination: u8, address: u32, length: u16 },
    CXPReadReply { length: u16, data: [u8; CXP_PAYLOAD_MAX_SIZE] },
    CXPWrite32Request { destination: u8, address: u32, value: u32 },
    CXPWrite32Reply,
    CXPROIViewerSetupRequest { destination: u8, x0: u16, y0: u16, x1: u16, y1: u16 },
    CXPROIViewerSetupReply,
    CXPROIViewerDataRequest { destination: u8 },
    CXPROIViewerPixelDataReply { length: u16, data: [u64; CXP_PAYLOAD_MAX_SIZE_U64] },
    CXPROIViewerFrameDataReply { width: u16, height: u16, pixel_code: u16 },
}

impl Packet {
    pub fn read_from<R>(reader: &mut R) -> Result<Self, Error<R::ReadError>>
        where R: Read + ?Sized
    {
        Ok(match reader.read_u8()? {
            0x00 => Packet::EchoRequest,
            0x01 => Packet::EchoReply,
            0x02 => Packet::ResetRequest,
            0x03 => Packet::ResetAck,
            0x04 => Packet::TSCAck,

            0x20 => Packet::DestinationStatusRequest {
                destination: reader.read_u8()?
            },
            0x21 => Packet::DestinationDownReply,
            0x22 => Packet::DestinationOkReply,
            0x23 => Packet::DestinationSequenceErrorReply {
                channel: reader.read_u16()?
            },
            0x24 => Packet::DestinationCollisionReply {
                channel: reader.read_u16()?
            },
            0x25 => Packet::DestinationBusyReply {
                channel: reader.read_u16()?
            },

            0x30 => {
                let destination = reader.read_u8()?;
                let mut hops = [0; 32];
                reader.read_exact(&mut hops)?;
                Packet::RoutingSetPath {
                    destination: destination,
                    hops: hops
                }
            },
            0x31 => Packet::RoutingSetRank {
                rank: reader.read_u8()?
            },
            0x32 => Packet::RoutingAck,

            0x40 => Packet::MonitorRequest {
                destination: reader.read_u8()?,
                channel: reader.read_u16()?,
                probe: reader.read_u8()?
            },
            0x41 => Packet::MonitorReply {
                value: reader.read_u64()?
            },
            0x50 => Packet::InjectionRequest {
                destination: reader.read_u8()?,
                channel: reader.read_u16()?,
                overrd: reader.read_u8()?,
                value: reader.read_u8()?
            },
            0x51 => Packet::InjectionStatusRequest {
                destination: reader.read_u8()?,
                channel: reader.read_u16()?,
                overrd: reader.read_u8()?
            },
            0x52 => Packet::InjectionStatusReply {
                value: reader.read_u8()?
            },

            0x80 => Packet::I2cStartRequest {
                destination: reader.read_u8()?,
                busno: reader.read_u8()?
            },
            0x81 => Packet::I2cRestartRequest {
                destination: reader.read_u8()?,
                busno: reader.read_u8()?
            },
            0x82 => Packet::I2cStopRequest {
                destination: reader.read_u8()?,
                busno: reader.read_u8()?
            },
            0x83 => Packet::I2cWriteRequest {
                destination: reader.read_u8()?,
                busno: reader.read_u8()?,
                data: reader.read_u8()?
            },
            0x84 => Packet::I2cWriteReply {
                succeeded: reader.read_bool()?,
                ack: reader.read_bool()?
            },
            0x85 => Packet::I2cReadRequest {
                destination: reader.read_u8()?,
                busno: reader.read_u8()?,
                ack: reader.read_bool()?
            },
            0x86 => Packet::I2cReadReply {
                succeeded: reader.read_bool()?,
                data: reader.read_u8()?
            },
            0x87 => Packet::I2cBasicReply {
                succeeded: reader.read_bool()?
            },
            0x88 => Packet::I2cSwitchSelectRequest {
                destination: reader.read_u8()?,
                busno: reader.read_u8()?,
                address: reader.read_u8()?,
                mask: reader.read_u8()?,
            },

            0x90 => Packet::SpiSetConfigRequest {
                destination: reader.read_u8()?,
                busno: reader.read_u8()?,
                flags: reader.read_u8()?,
                length: reader.read_u8()?,
                div: reader.read_u8()?,
                cs: reader.read_u8()?
            },
            /* 0x91: was Packet::SpiSetXferRequest */
            0x92 => Packet::SpiWriteRequest {
                destination: reader.read_u8()?,
                busno: reader.read_u8()?,
                data: reader.read_u32()?
            },
            0x93 => Packet::SpiReadRequest {
                destination: reader.read_u8()?,
                busno: reader.read_u8()?
            },
            0x94 => Packet::SpiReadReply {
                succeeded: reader.read_bool()?,
                data: reader.read_u32()?
            },
            0x95 => Packet::SpiBasicReply {
                succeeded: reader.read_bool()?
            },

            0xa0 => Packet::AnalyzerHeaderRequest {
                destination: reader.read_u8()?
            },
            0xa1 => Packet::AnalyzerHeader {
                sent_bytes: reader.read_u32()?, 
                total_byte_count: reader.read_u64()?, 
                overflow_occurred: reader.read_bool()?,
            },
            0xa2 => Packet::AnalyzerDataRequest {
                destination: reader.read_u8()?
            },
            0xa3 => {
                let last = reader.read_bool()?;
                let length = reader.read_u16()?;
                let mut data: [u8; SAT_PAYLOAD_MAX_SIZE] = [0; SAT_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut data[0..length as usize])?;
                Packet::AnalyzerData {
                    last: last,
                    length: length,
                    data: data
                }
            },

            0xb0 => {
                let source = reader.read_u8()?;
                let destination = reader.read_u8()?;
                let id = reader.read_u32()?;
                let status = reader.read_u8()?;
                let length = reader.read_u16()?;
                let mut trace: [u8; MASTER_PAYLOAD_MAX_SIZE] = [0; MASTER_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut trace[0..length as usize])?;
                Packet::DmaAddTraceRequest {
                    source: source,
                    destination: destination,
                    id: id,
                    status: PayloadStatus::from(status),
                    length: length as u16,
                    trace: trace,
                }
            },
            0xb1 => Packet::DmaAddTraceReply {
                source: reader.read_u8()?,
                destination: reader.read_u8()?,
                id: reader.read_u32()?,
                succeeded: reader.read_bool()?
            },
            0xb2 => Packet::DmaRemoveTraceRequest {
                source: reader.read_u8()?,
                destination: reader.read_u8()?,
                id: reader.read_u32()?
            },
            0xb3 => Packet::DmaRemoveTraceReply {
                destination: reader.read_u8()?,
                succeeded: reader.read_bool()?
            },
            0xb4 => Packet::DmaPlaybackRequest {
                source: reader.read_u8()?,
                destination: reader.read_u8()?,
                id: reader.read_u32()?,
                timestamp: reader.read_u64()?
            },
            0xb5 => Packet::DmaPlaybackReply {
                destination: reader.read_u8()?,
                succeeded: reader.read_bool()?
            },
            0xb6 => Packet::DmaPlaybackStatus {
                source: reader.read_u8()?,
                destination: reader.read_u8()?,
                id: reader.read_u32()?,
                error: reader.read_u8()?,
                channel: reader.read_u32()?,
                timestamp: reader.read_u64()?
            },

            0xc0 => { 
                let destination = reader.read_u8()?;
                let id = reader.read_u32()?;
                let status = reader.read_u8()?;
                let length = reader.read_u16()?;
                let mut data: [u8; MASTER_PAYLOAD_MAX_SIZE] = [0; MASTER_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut data[0..length as usize])?;
                Packet::SubkernelAddDataRequest {
                    destination: destination,
                    id: id,
                    status: PayloadStatus::from(status),
                    length: length as u16,
                    data: data,
                }
            },
            0xc1 => Packet::SubkernelAddDataReply {
                succeeded: reader.read_bool()?
            },
            0xc4 => Packet::SubkernelLoadRunRequest {
                source: reader.read_u8()?,
                destination: reader.read_u8()?,
                id: reader.read_u32()?,
                run: reader.read_bool()?,
                timestamp: reader.read_u64()?
            },
            0xc5 => Packet::SubkernelLoadRunReply {
                destination: reader.read_u8()?,
                succeeded: reader.read_bool()?
            },
            0xc8 => Packet::SubkernelFinished {
                destination: reader.read_u8()?,
                id: reader.read_u32()?,
                with_exception: reader.read_bool()?,
                exception_src: reader.read_u8()?
            },
            0xc9 => Packet::SubkernelExceptionRequest {
                source: reader.read_u8()?,
                destination: reader.read_u8()?
            },
            0xca => {
                let destination = reader.read_u8()?;
                let last = reader.read_bool()?;
                let length = reader.read_u16()?;
                let mut data: [u8; MASTER_PAYLOAD_MAX_SIZE] = [0; MASTER_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut data[0..length as usize])?;
                Packet::SubkernelException {
                    destination: destination,
                    last: last,
                    length: length,
                    data: data
                }
            },
            0xcb => {
                let source = reader.read_u8()?;
                let destination = reader.read_u8()?;
                let id = reader.read_u32()?;
                let status = reader.read_u8()?;
                let length = reader.read_u16()?;
                let mut data: [u8; MASTER_PAYLOAD_MAX_SIZE] = [0; MASTER_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut data[0..length as usize])?;
                Packet::SubkernelMessage {
                    source: source,
                    destination: destination,
                    id: id,
                    status: PayloadStatus::from(status),
                    length: length as u16,
                    data: data,
                }
            },
            0xcc => Packet::SubkernelMessageAck {
                destination: reader.read_u8()?
            },

            0xd0 => Packet::CoreMgmtGetLogRequest {
                destination: reader.read_u8()?,
                clear: reader.read_bool()?,
            },
            0xd1 => Packet::CoreMgmtClearLogRequest {
                destination: reader.read_u8()?,
            },
            0xd2 => Packet::CoreMgmtSetLogLevelRequest {
                destination: reader.read_u8()?,
                log_level: reader.read_u8()?,
            },
            0xd3 => Packet::CoreMgmtSetUartLogLevelRequest {
                destination: reader.read_u8()?,
                log_level: reader.read_u8()?,
            },
            0xd4 => {
                let destination = reader.read_u8()?;
                let length = reader.read_u16()?;
                let mut key: [u8; MASTER_PAYLOAD_MAX_SIZE] = [0; MASTER_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut key[0..length as usize])?;
                Packet::CoreMgmtConfigReadRequest {
                    destination: destination,
                    length: length,
                    key: key,
                }
            },
            0xd5 => Packet::CoreMgmtConfigReadContinue {
                destination: reader.read_u8()?,
            },
            0xd6 => {
                let destination = reader.read_u8()?;
                let last = reader.read_bool()?;
                let length = reader.read_u16()?;
                let mut data: [u8; MASTER_PAYLOAD_MAX_SIZE] = [0; MASTER_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut data[0..length as usize])?;
                Packet::CoreMgmtConfigWriteRequest {
                    destination: destination,
                    last: last,
                    length: length,
                    data: data,
                }
            },
            0xd7 => {
                let destination = reader.read_u8()?;
                let length = reader.read_u16()?;
                let mut key: [u8; MASTER_PAYLOAD_MAX_SIZE] = [0; MASTER_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut key[0..length as usize])?;
                Packet::CoreMgmtConfigRemoveRequest {
                    destination: destination,
                    length: length,
                    key: key,
                }
            },
            0xd8 => Packet::CoreMgmtConfigEraseRequest {
                destination: reader.read_u8()?,
            },
            0xd9 => Packet::CoreMgmtRebootRequest {
                destination: reader.read_u8()?,
            },
            0xda => Packet::CoreMgmtAllocatorDebugRequest {
                destination: reader.read_u8()?,
            },
            0xdb => Packet::CoreMgmtFlashRequest {
                destination: reader.read_u8()?,
                payload_length: reader.read_u32()?,
            },
            0xdc => {
                let destination = reader.read_u8()?;
                let last = reader.read_bool()?;
                let length = reader.read_u16()?;
                let mut data: [u8; MASTER_PAYLOAD_MAX_SIZE] = [0; MASTER_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut data[0..length as usize])?;
                Packet::CoreMgmtFlashAddDataRequest {
                    destination: destination,
                    last: last,
                    length: length,
                    data: data,
                }
            },
            0xdd => Packet::CoreMgmtDropLinkAck {
                destination: reader.read_u8()?,
            },
            0xde => Packet::CoreMgmtDropLink,
            0xdf => {
                let last = reader.read_bool()?;
                let length = reader.read_u16()?;
                let mut data: [u8; SAT_PAYLOAD_MAX_SIZE] = [0; SAT_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut data[0..length as usize])?;
                Packet::CoreMgmtGetLogReply {
                    last: last,
                    length: length,
                    data: data,
                }
            },
            0xe0 => {
                let last = reader.read_bool()?;
                let length = reader.read_u16()?;
                let mut value: [u8; SAT_PAYLOAD_MAX_SIZE] = [0; SAT_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut value[0..length as usize])?;
                Packet::CoreMgmtConfigReadReply {
                    last: last,
                    length: length,
                    value: value,
                }
            },
            0xe1 => Packet::CoreMgmtReply {
                succeeded: reader.read_bool()?,
            },
            0xe2 => {
                let length = reader.read_u16()?;
                let mut message: [u8; CXP_PAYLOAD_MAX_SIZE] = [0; CXP_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut message[0..length as usize])?;
                Packet::CXPError { length, message }
            }
            0xe3 => Self::CXPWaitReply,
            0xe4 => Packet::CXPReadRequest {
                destination: reader.read_u8()?,
                address: reader.read_u32()?,
                length: reader.read_u16()?,
            },
            0xe5 => {
                let length = reader.read_u16()?;
                let mut data: [u8; CXP_PAYLOAD_MAX_SIZE] = [0; CXP_PAYLOAD_MAX_SIZE];
                reader.read_exact(&mut data[0..length as usize])?;
                Packet::CXPReadReply { length, data }
            }
            0xe6 => Packet::CXPWrite32Request {
                destination: reader.read_u8()?,
                address: reader.read_u32()?,
                value: reader.read_u32()?,
            },
            0xe7 => Packet::CXPWrite32Reply,
            0xe8 => Packet::CXPROIViewerSetupRequest {
                destination: reader.read_u8()?,
                x0: reader.read_u16()?,
                y0: reader.read_u16()?,
                x1: reader.read_u16()?,
                y1: reader.read_u16()?,
            },
            0xe9 => Packet::CXPROIViewerSetupReply,
            0xea => Packet::CXPROIViewerDataRequest {
                destination: reader.read_u8()?,
            },
            0xeb => {
                let length = reader.read_u16()?;
                let mut data: [u64; CXP_PAYLOAD_MAX_SIZE / 8] = [0; CXP_PAYLOAD_MAX_SIZE / 8];
                for i in 0..length as usize {
                    data[i] = reader.read_u64()?;
                }
                Packet::CXPROIViewerPixelDataReply { length, data }
            }
            0xec => Packet::CXPROIViewerFrameDataReply {
                width: reader.read_u16()?,
                height: reader.read_u16()?,
                pixel_code: reader.read_u16()?,
            },

            ty => return Err(Error::UnknownPacket(ty))
        })
    }

    pub fn write_to<W>(&self, writer: &mut W) -> Result<(), IoError<W::WriteError>>
        where W: Write + ?Sized
    {
        match *self {
            Packet::EchoRequest =>
                writer.write_u8(0x00)?,
            Packet::EchoReply =>
                writer.write_u8(0x01)?,
            Packet::ResetRequest =>
                writer.write_u8(0x02)?,
            Packet::ResetAck =>
                writer.write_u8(0x03)?,
            Packet::TSCAck =>
                writer.write_u8(0x04)?,

            Packet::DestinationStatusRequest { destination } => {
                writer.write_u8(0x20)?;
                writer.write_u8(destination)?;
            },
            Packet::DestinationDownReply =>
                writer.write_u8(0x21)?,
            Packet::DestinationOkReply =>
                writer.write_u8(0x22)?,
            Packet::DestinationSequenceErrorReply { channel } => {
                writer.write_u8(0x23)?;
                writer.write_u16(channel)?;
            },
            Packet::DestinationCollisionReply { channel } => {
                writer.write_u8(0x24)?;
                writer.write_u16(channel)?;
            },
            Packet::DestinationBusyReply { channel } => {
                writer.write_u8(0x25)?;
                writer.write_u16(channel)?;
            },

            Packet::RoutingSetPath { destination, hops } => {
                writer.write_u8(0x30)?;
                writer.write_u8(destination)?;
                writer.write_all(&hops)?;
            },
            Packet::RoutingSetRank { rank } => {
                writer.write_u8(0x31)?;
                writer.write_u8(rank)?;
            },
            Packet::RoutingAck =>
                writer.write_u8(0x32)?,

            Packet::MonitorRequest { destination, channel, probe } => {
                writer.write_u8(0x40)?;
                writer.write_u8(destination)?;
                writer.write_u16(channel)?;
                writer.write_u8(probe)?;
            },
            Packet::MonitorReply { value } => {
                writer.write_u8(0x41)?;
                writer.write_u64(value)?;
            },
            Packet::InjectionRequest { destination, channel, overrd, value } => {
                writer.write_u8(0x50)?;
                writer.write_u8(destination)?;
                writer.write_u16(channel)?;
                writer.write_u8(overrd)?;
                writer.write_u8(value)?;
            },
            Packet::InjectionStatusRequest { destination, channel, overrd } => {
                writer.write_u8(0x51)?;
                writer.write_u8(destination)?;
                writer.write_u16(channel)?;
                writer.write_u8(overrd)?;
            },
            Packet::InjectionStatusReply { value } => {
                writer.write_u8(0x52)?;
                writer.write_u8(value)?;
            },

            Packet::I2cStartRequest { destination, busno } => {
                writer.write_u8(0x80)?;
                writer.write_u8(destination)?;
                writer.write_u8(busno)?;
            },
            Packet::I2cRestartRequest { destination, busno } => {
                writer.write_u8(0x81)?;
                writer.write_u8(destination)?;
                writer.write_u8(busno)?;
            },
            Packet::I2cStopRequest { destination, busno } => {
                writer.write_u8(0x82)?;
                writer.write_u8(destination)?;
                writer.write_u8(busno)?;
            },
            Packet::I2cWriteRequest { destination, busno, data } => {
                writer.write_u8(0x83)?;
                writer.write_u8(destination)?;
                writer.write_u8(busno)?;
                writer.write_u8(data)?;
            },
            Packet::I2cWriteReply { succeeded, ack } => {
                writer.write_u8(0x84)?;
                writer.write_bool(succeeded)?;
                writer.write_bool(ack)?;
            },
            Packet::I2cReadRequest { destination, busno, ack } => {
                writer.write_u8(0x85)?;
                writer.write_u8(destination)?;
                writer.write_u8(busno)?;
                writer.write_bool(ack)?;
            },
            Packet::I2cReadReply { succeeded, data } => {
                writer.write_u8(0x86)?;
                writer.write_bool(succeeded)?;
                writer.write_u8(data)?;
            },
            Packet::I2cBasicReply { succeeded } => {
                writer.write_u8(0x87)?;
                writer.write_bool(succeeded)?;
            },
            Packet::I2cSwitchSelectRequest { destination, busno, address, mask } => {
                writer.write_u8(0x88)?;
                writer.write_u8(destination)?;
                writer.write_u8(busno)?;
                writer.write_u8(address)?;
                writer.write_u8(mask)?;
            },

            Packet::SpiSetConfigRequest { destination, busno, flags, length, div, cs } => {
                writer.write_u8(0x90)?;
                writer.write_u8(destination)?;
                writer.write_u8(busno)?;
                writer.write_u8(flags)?;
                writer.write_u8(length)?;
                writer.write_u8(div)?;
                writer.write_u8(cs)?;
            },
            Packet::SpiWriteRequest { destination, busno, data } => {
                writer.write_u8(0x92)?;
                writer.write_u8(destination)?;
                writer.write_u8(busno)?;
                writer.write_u32(data)?;
            },
            Packet::SpiReadRequest { destination, busno } => {
                writer.write_u8(0x93)?;
                writer.write_u8(destination)?;
                writer.write_u8(busno)?;
            },
            Packet::SpiReadReply { succeeded, data } => {
                writer.write_u8(0x94)?;
                writer.write_bool(succeeded)?;
                writer.write_u32(data)?;
            },
            Packet::SpiBasicReply { succeeded } => {
                writer.write_u8(0x95)?;
                writer.write_bool(succeeded)?;
            },

            Packet::AnalyzerHeaderRequest { destination } => {
                writer.write_u8(0xa0)?;
                writer.write_u8(destination)?;
            },
            Packet::AnalyzerHeader { sent_bytes, total_byte_count, overflow_occurred } => { 
                writer.write_u8(0xa1)?;
                writer.write_u32(sent_bytes)?;
                writer.write_u64(total_byte_count)?;
                writer.write_bool(overflow_occurred)?;
            },
            Packet::AnalyzerDataRequest { destination } => {
                writer.write_u8(0xa2)?;
                writer.write_u8(destination)?;
            },
            Packet::AnalyzerData { last, length, data } => {
                writer.write_u8(0xa3)?;
                writer.write_bool(last)?;
                writer.write_u16(length)?;
                writer.write_all(&data[0..length as usize])?;
            },

            Packet::DmaAddTraceRequest { source, destination, id, status, trace, length } => {
                writer.write_u8(0xb0)?;
                writer.write_u8(source)?;
                writer.write_u8(destination)?;
                writer.write_u32(id)?;
                writer.write_u8(status as u8)?;
                // trace may be broken down to fit within drtio aux memory limit
                // will be reconstructed by satellite
                writer.write_u16(length)?;
                writer.write_all(&trace[0..length as usize])?;
            },
            Packet::DmaAddTraceReply { source, destination, id, succeeded } => {
                writer.write_u8(0xb1)?;
                writer.write_u8(source)?;
                writer.write_u8(destination)?;
                writer.write_u32(id)?;
                writer.write_bool(succeeded)?;
            },
            Packet::DmaRemoveTraceRequest { source, destination, id } => {
                writer.write_u8(0xb2)?;
                writer.write_u8(source)?;
                writer.write_u8(destination)?;
                writer.write_u32(id)?;
            },
            Packet::DmaRemoveTraceReply { destination, succeeded } => {
                writer.write_u8(0xb3)?;
                writer.write_u8(destination)?;
                writer.write_bool(succeeded)?;
            },
            Packet::DmaPlaybackRequest { source, destination, id, timestamp } => {
                writer.write_u8(0xb4)?;
                writer.write_u8(source)?;
                writer.write_u8(destination)?;
                writer.write_u32(id)?;
                writer.write_u64(timestamp)?;
            },
            Packet::DmaPlaybackReply { destination, succeeded } => {
                writer.write_u8(0xb5)?;
                writer.write_u8(destination)?;
                writer.write_bool(succeeded)?;
            },
            Packet::DmaPlaybackStatus { source, destination, id, error, channel, timestamp } => {
                writer.write_u8(0xb6)?;
                writer.write_u8(source)?;
                writer.write_u8(destination)?;
                writer.write_u32(id)?;
                writer.write_u8(error)?;
                writer.write_u32(channel)?;
                writer.write_u64(timestamp)?;
            },

            Packet::SubkernelAddDataRequest { destination, id, status, data, length } => {
                writer.write_u8(0xc0)?;
                writer.write_u8(destination)?;
                writer.write_u32(id)?;
                writer.write_u8(status as u8)?;
                writer.write_u16(length)?;
                writer.write_all(&data[0..length as usize])?;
            },
            Packet::SubkernelAddDataReply { succeeded } => {
                writer.write_u8(0xc1)?;
                writer.write_bool(succeeded)?;
            },
            Packet::SubkernelLoadRunRequest { source, destination, id, run, timestamp } => {
                writer.write_u8(0xc4)?;
                writer.write_u8(source)?;
                writer.write_u8(destination)?;
                writer.write_u32(id)?;
                writer.write_bool(run)?;
                writer.write_u64(timestamp)?;
            },
            Packet::SubkernelLoadRunReply { destination, succeeded } => {
                writer.write_u8(0xc5)?;
                writer.write_u8(destination)?;
                writer.write_bool(succeeded)?;
            },
            Packet::SubkernelFinished { destination, id, with_exception, exception_src } => {
                writer.write_u8(0xc8)?;
                writer.write_u8(destination)?;
                writer.write_u32(id)?;
                writer.write_bool(with_exception)?;
                writer.write_u8(exception_src)?;
            },
            Packet::SubkernelExceptionRequest { source, destination } => {
                writer.write_u8(0xc9)?;
                writer.write_u8(source)?;
                writer.write_u8(destination)?;
            },
            Packet::SubkernelException { destination, last, length, data } => {
                writer.write_u8(0xca)?;
                writer.write_u8(destination)?;
                writer.write_bool(last)?;
                writer.write_u16(length)?;
                writer.write_all(&data[0..length as usize])?;
            },
            Packet::SubkernelMessage { source, destination, id, status, data, length } => {
                writer.write_u8(0xcb)?;
                writer.write_u8(source)?;
                writer.write_u8(destination)?;
                writer.write_u32(id)?;
                writer.write_u8(status as u8)?;
                writer.write_u16(length)?;
                writer.write_all(&data[0..length as usize])?;
            },
            Packet::SubkernelMessageAck { destination } => {
                writer.write_u8(0xcc)?;
                writer.write_u8(destination)?;
            },

            Packet::CoreMgmtGetLogRequest { destination, clear } => {
                writer.write_u8(0xd0)?;
                writer.write_u8(destination)?;
                writer.write_bool(clear)?;
            },
            Packet::CoreMgmtClearLogRequest { destination } => {
                writer.write_u8(0xd1)?;
                writer.write_u8(destination)?;
            },
            Packet::CoreMgmtSetLogLevelRequest { destination, log_level } => {
                writer.write_u8(0xd2)?;
                writer.write_u8(destination)?;
                writer.write_u8(log_level)?;
            },
            Packet::CoreMgmtSetUartLogLevelRequest { destination, log_level } => {
                writer.write_u8(0xd3)?;
                writer.write_u8(destination)?;
                writer.write_u8(log_level)?;
            },
            Packet::CoreMgmtConfigReadRequest {
                destination,
                length,
                key,
            } => {
                writer.write_u8(0xd4)?;
                writer.write_u8(destination)?;
                writer.write_u16(length)?;
                writer.write_all(&key[0..length as usize])?;
            },
            Packet::CoreMgmtConfigReadContinue { destination } => {
                writer.write_u8(0xd5)?;
                writer.write_u8(destination)?;
            },
            Packet::CoreMgmtConfigWriteRequest {
                destination,
                last,
                length,
                data,
            } => {
                writer.write_u8(0xd6)?;
                writer.write_u8(destination)?;
                writer.write_bool(last)?;
                writer.write_u16(length)?;
                writer.write_all(&data[0..length as usize])?;
            },
            Packet::CoreMgmtConfigRemoveRequest {
                destination,
                length,
                key,
            } => {
                writer.write_u8(0xd7)?;
                writer.write_u8(destination)?;
                writer.write_u16(length)?;
                writer.write_all(&key[0..length as usize])?;
            },
            Packet::CoreMgmtConfigEraseRequest { destination } => {
                writer.write_u8(0xd8)?;
                writer.write_u8(destination)?;
            },
            Packet::CoreMgmtRebootRequest { destination } => {
                writer.write_u8(0xd9)?;
                writer.write_u8(destination)?;
            },
            Packet::CoreMgmtAllocatorDebugRequest { destination } => {
                writer.write_u8(0xda)?;
                writer.write_u8(destination)?;
            },
            Packet::CoreMgmtFlashRequest { destination, payload_length } => {
                writer.write_u8(0xdb)?;
                writer.write_u8(destination)?;
                writer.write_u32(payload_length)?;
            },
            Packet::CoreMgmtFlashAddDataRequest { destination, last, length, data } => {
                writer.write_u8(0xdc)?;
                writer.write_u8(destination)?;
                writer.write_bool(last)?;
                writer.write_u16(length)?;
                writer.write_all(&data[..length as usize])?;
            },
            Packet::CoreMgmtDropLinkAck { destination } => {
                writer.write_u8(0xdd)?;
                writer.write_u8(destination)?;
            },
            Packet::CoreMgmtDropLink => 
                writer.write_u8(0xde)?,
            Packet::CoreMgmtGetLogReply { last, length, data } => {
                writer.write_u8(0xdf)?;
                writer.write_bool(last)?;
                writer.write_u16(length)?;
                writer.write_all(&data[0..length as usize])?;
            },
            Packet::CoreMgmtConfigReadReply { last, length, value } => {
                writer.write_u8(0xe0)?;
                writer.write_bool(last)?;
                writer.write_u16(length)?;
                writer.write_all(&value[0..length as usize])?;
            },
            Packet::CoreMgmtReply { succeeded } => {
                writer.write_u8(0xe1)?;
                writer.write_bool(succeeded)?;
            },
            Packet::CXPError { length, message } => {
                writer.write_u8(0xe2)?;
                writer.write_u16(length)?;
                writer.write_all(&message[0..length as usize])?;
            }
            Packet::CXPWaitReply => {
                writer.write_u8(0xe3)?;
            }
            Packet::CXPReadRequest {
                destination,
                address,
                length,
            } => {
                writer.write_u8(0xe4)?;
                writer.write_u8(destination)?;
                writer.write_u32(address)?;
                writer.write_u16(length)?;
            }
            Packet::CXPReadReply { length, data } => {
                writer.write_u8(0xe5)?;
                writer.write_u16(length)?;
                writer.write_all(&data[0..length as usize])?;
            }
            Packet::CXPWrite32Request {
                destination,
                address,
                value,
            } => {
                writer.write_u8(0xe6)?;
                writer.write_u8(destination)?;
                writer.write_u32(address)?;
                writer.write_u32(value)?;
            }
            Packet::CXPWrite32Reply => {
                writer.write_u8(0xe7)?;
            }
            Packet::CXPROIViewerSetupRequest {
                destination,
                x0,
                y0,
                x1,
                y1,
            } => {
                writer.write_u8(0xe8)?;
                writer.write_u8(destination)?;
                writer.write_u16(x0)?;
                writer.write_u16(y0)?;
                writer.write_u16(x1)?;
                writer.write_u16(y1)?;
            }
            Packet::CXPROIViewerSetupReply => {
                writer.write_u8(0xe9)?;
            }
            Packet::CXPROIViewerDataRequest { destination } => {
                writer.write_u8(0xea)?;
                writer.write_u8(destination)?;
            }
            Packet::CXPROIViewerPixelDataReply { length, data } => {
                writer.write_u8(0xeb)?;
                writer.write_u16(length)?;
                for i in 0..length as usize {
                    writer.write_u64(data[i])?;
                }
            }
            Packet::CXPROIViewerFrameDataReply {
                width,
                height,
                pixel_code,
            } => {
                writer.write_u8(0xec)?;
                writer.write_u16(width)?;
                writer.write_u16(height)?;
                writer.write_u16(pixel_code)?;
            }
        }
        Ok(())
    }

    pub fn routable_destination(&self) -> Option<u8> {
        // only for packets that could be re-routed, not only forwarded
        match self {
            Packet::DmaAddTraceRequest        { destination, .. } => Some(*destination),
            Packet::DmaAddTraceReply          { destination, .. } => Some(*destination),
            Packet::DmaRemoveTraceRequest     { destination, .. } => Some(*destination),
            Packet::DmaRemoveTraceReply       { destination, .. } => Some(*destination),
            Packet::DmaPlaybackRequest        { destination, .. } => Some(*destination),
            Packet::DmaPlaybackReply          { destination, .. } => Some(*destination),
            Packet::SubkernelLoadRunRequest   { destination, .. } => Some(*destination),
            Packet::SubkernelLoadRunReply     { destination, .. } => Some(*destination),
            Packet::SubkernelMessage          { destination, .. } => Some(*destination),
            Packet::SubkernelMessageAck       { destination, .. } => Some(*destination),
            Packet::SubkernelExceptionRequest { destination, .. } => Some(*destination),
            Packet::SubkernelException        { destination, .. } => Some(*destination),
            Packet::DmaPlaybackStatus         { destination, .. } => Some(*destination),
            Packet::SubkernelFinished         { destination, .. } => Some(*destination),
            _ => None
        }
    }

    pub fn expects_response(&self) -> bool {
        // returns true if the routable packet should elicit a response
        // e.g. reply, ACK packets end a conversation,
        // and firmware should not wait for response
        match self {
            Packet::DmaAddTraceReply { .. } | Packet::DmaRemoveTraceReply { .. } |
                Packet::DmaPlaybackReply { .. } | Packet::SubkernelLoadRunReply { .. } |
                Packet::SubkernelMessageAck { .. } | Packet::DmaPlaybackStatus { .. } |
                Packet::SubkernelFinished { .. } | Packet::CoreMgmtDropLinkAck { .. } |
                Packet::InjectionRequest { .. } => false,
            _ => true
        }
    }
}
