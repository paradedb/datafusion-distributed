//! Multi-channel Arrow IPC frame codec.
//!
//! Every wire message carries a fixed 16-byte [`MultiChannelFrameHeader`] prefix
//! `(magic, flags, stage_id, partition)` so a single underlying byte queue can
//! interleave frames for many logical `(stage_id, partition)` channels.
//! Receivers demultiplex by header before touching Arrow IPC.
//!
//! Wire format:
//!
//! ```text
//! [ magic | flags | stage_id | partition ] [ Arrow IPC stream bytes ]
//! |---------- 16 bytes --------|           |---- variable ----|
//! ```
//!
//! Transport-agnostic: the codec knows nothing about gRPC, `shm_mq`, or
//! `std::sync::mpsc`. Embedders pick a transport and use these primitives to
//! frame each payload. [`crate::FlightWorkerTransport`] doesn't use it (gRPC has
//! its own framing); embedders that fold multiple logical channels onto one
//! byte stream do.

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::DataFusionError;

/// Magic bytes "MPPF" (Multi-channel Plan Framing) on every wire message. Lets
/// receivers reject misrouted or corrupt frames before they hit Arrow IPC.
const FRAME_MAGIC: u32 = 0x4D505046;

/// Wire-format size of [`MultiChannelFrameHeader`] in bytes. Asserted at compile
/// time below.
pub const MULTI_CHANNEL_FRAME_HEADER_SIZE: usize = 16;

/// Kind of payload following [`MultiChannelFrameHeader`].
///
/// `Batch` is the common case: the header is followed by an Arrow IPC stream
/// containing one `RecordBatch`. `Eof` carries no payload. It tells the receiver
/// the named `(stage_id, partition)` channel is done, even if the underlying
/// byte queue still carries frames for other channels.
///
/// Crate-private: external callers don't introspect the discriminant.
/// `encode_*_frame_into` takes a `MultiChannelFrameHeader` directly and
/// `decode_frame` returns the typed payload via `Option<RecordBatch>`. Promote
/// to `pub` if a future caller needs to branch on kind without going through
/// `decode_frame`.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameKind {
    Batch = 0,
    Eof = 1,
}

/// 16-byte prefix on every transport frame.
///
/// Fixed layout `[magic, flags, stage_id, partition]` (4×u32). Senders prepend
/// it before the Arrow IPC stream bytes; receivers parse it to decide which
/// channel buffer the payload belongs to.
///
/// `flags` carries the [`FrameKind`] in its low byte (mask `0x0000_00FF`). The
/// upper 24 bits are reserved-must-be-zero and get validated at parse time, so
/// a future use can repurpose them without a wire-format break.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultiChannelFrameHeader {
    pub magic: u32,
    pub flags: u32,
    pub stage_id: u32,
    pub partition: u32,
}

/// Bit mask in [`MultiChannelFrameHeader::flags`] for the [`FrameKind`] discriminant.
const FRAME_KIND_MASK: u32 = 0x0000_00FF;

const _: () = {
    // Wire-layer slot-size math downstream depends on this being exact (e.g. embedder
    // shared-memory mesh sizing). Catch a field reorder at build time before it can
    // silently shift slot offsets.
    assert!(std::mem::size_of::<MultiChannelFrameHeader>() == MULTI_CHANNEL_FRAME_HEADER_SIZE);
};

impl MultiChannelFrameHeader {
    /// Build a `Batch` header for the given `(stage_id, partition)`.
    pub fn batch(stage_id: u32, partition: u32) -> Self {
        Self {
            magic: FRAME_MAGIC,
            flags: FrameKind::Batch as u32,
            stage_id,
            partition,
        }
    }

    /// Build an `Eof` header for the given `(stage_id, partition)`. Carries no payload.
    /// Receivers route it to the channel buffer's source-done counter. Emit after a
    /// producer fragment's per-partition stream exhausts (or errors).
    pub fn eof(stage_id: u32, partition: u32) -> Self {
        Self {
            magic: FRAME_MAGIC,
            flags: FrameKind::Eof as u32,
            stage_id,
            partition,
        }
    }

    /// Read the kind out of `flags`. Errors if the kind byte is unknown or any
    /// reserved upper bit is set. Catches wire-format drift early.
    pub(crate) fn kind(&self) -> Result<FrameKind, DataFusionError> {
        let reserved = self.flags & !FRAME_KIND_MASK;
        if reserved != 0 {
            return Err(DataFusionError::Internal(format!(
                "multi_channel_frame: reserved frame flag bits set ({reserved:#x})"
            )));
        }
        match self.flags & FRAME_KIND_MASK {
            0 => Ok(FrameKind::Batch),
            1 => Ok(FrameKind::Eof),
            other => Err(DataFusionError::Internal(format!(
                "multi_channel_frame: unknown frame kind {other:#x}"
            ))),
        }
    }

    /// Serialize into the first [`MULTI_CHANNEL_FRAME_HEADER_SIZE`] bytes of `out`.
    /// `out.len()` must be `>= MULTI_CHANNEL_FRAME_HEADER_SIZE`.
    fn write_to(&self, out: &mut [u8]) {
        debug_assert!(out.len() >= MULTI_CHANNEL_FRAME_HEADER_SIZE);
        out[0..4].copy_from_slice(&self.magic.to_le_bytes());
        out[4..8].copy_from_slice(&self.flags.to_le_bytes());
        out[8..12].copy_from_slice(&self.stage_id.to_le_bytes());
        out[12..16].copy_from_slice(&self.partition.to_le_bytes());
    }

    /// Parse from the first [`MULTI_CHANNEL_FRAME_HEADER_SIZE`] bytes of `bytes`. Returns
    /// `Err` if the slice is too short or the magic doesn't match.
    fn parse(bytes: &[u8]) -> Result<Self, DataFusionError> {
        if bytes.len() < MULTI_CHANNEL_FRAME_HEADER_SIZE {
            // No encoder here emits sub-header output, so a short frame means the byte
            // queue stitched together payloads from different senders. Hex-dump the bytes
            // so the source is identifiable from logs without a debugger.
            let hex = bytes
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<_>>()
                .join(" ");
            return Err(DataFusionError::Internal(format!(
                "multi_channel_frame: frame too short for header ({} < {}); bytes = [{hex}]",
                bytes.len(),
                MULTI_CHANNEL_FRAME_HEADER_SIZE
            )));
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != FRAME_MAGIC {
            return Err(DataFusionError::Internal(format!(
                "multi_channel_frame: bad frame magic {magic:#x} (expected {FRAME_MAGIC:#x})"
            )));
        }
        Ok(Self {
            magic,
            flags: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            stage_id: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            partition: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
        })
    }
}

/// Serialize `batch` into `buf` with a 16-byte [`MultiChannelFrameHeader`] prefix
/// addressing it to `(header.stage_id, header.partition)`. Wire format:
///
/// ```text
/// [ magic | flags | stage_id | partition ] [ Arrow IPC stream bytes ]
/// |---------- 16 bytes --------|           |---- variable ----|
/// ```
///
/// Hold `buf` alive across many encodes to amortize the peak-sized allocation.
pub fn encode_frame_into(
    header: MultiChannelFrameHeader,
    batch: &RecordBatch,
    buf: &mut Vec<u8>,
) -> Result<(), DataFusionError> {
    buf.clear();
    buf.resize(MULTI_CHANNEL_FRAME_HEADER_SIZE, 0);
    header.write_to(&mut buf[..MULTI_CHANNEL_FRAME_HEADER_SIZE]);
    let mut writer = StreamWriter::try_new(&mut *buf, batch.schema_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(())
}

/// Serialize a payload-less [`FrameKind::Eof`] frame for `(stage_id, partition)`
/// into `buf`. Receivers read it as a 16-byte message and route to the channel
/// buffer's source-done counter without touching Arrow IPC.
///
/// Emit when a producer fragment's per-partition stream exhausts: the receiver's
/// `(stage_id, partition)` channel transitions to `Eof` while the multiplexed
/// underlying queue stays attached for other channels.
pub fn encode_eof_frame_into(
    stage_id: u32,
    partition: u32,
    buf: &mut Vec<u8>,
) -> Result<(), DataFusionError> {
    buf.clear();
    buf.resize(MULTI_CHANNEL_FRAME_HEADER_SIZE, 0);
    MultiChannelFrameHeader::eof(stage_id, partition)
        .write_to(&mut buf[..MULTI_CHANNEL_FRAME_HEADER_SIZE]);
    Ok(())
}

/// Inverse of [`encode_frame_into`]. Parses the 16-byte header and, for `Batch`
/// frames, decodes the trailing Arrow IPC stream. `Eof` frames return
/// `(header, None)`. Branch on `header.kind()` for routing.
pub fn decode_frame(
    bytes: &[u8],
) -> Result<(MultiChannelFrameHeader, Option<RecordBatch>), DataFusionError> {
    let header = MultiChannelFrameHeader::parse(bytes)?;
    match header.kind()? {
        FrameKind::Eof => {
            if bytes.len() != MULTI_CHANNEL_FRAME_HEADER_SIZE {
                return Err(DataFusionError::Internal(format!(
                    "multi_channel_frame: Eof frame carries payload ({} > {})",
                    bytes.len(),
                    MULTI_CHANNEL_FRAME_HEADER_SIZE
                )));
            }
            Ok((header, None))
        }
        FrameKind::Batch => {
            let payload = &bytes[MULTI_CHANNEL_FRAME_HEADER_SIZE..];
            let mut reader = StreamReader::try_new(payload, None)?;
            let batch = reader.next().ok_or_else(|| {
                DataFusionError::Execution(
                    "multi_channel_frame: empty arrow-ipc stream in decode_frame".into(),
                )
            })??;
            Ok((header, Some(batch)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn sample_batch(rows: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Int32Array::from_iter_values(0..rows);
        let names = StringArray::from_iter_values((0..rows).map(|i| format!("n{i}")));
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    #[test]
    fn frame_round_trips_a_batch_with_header() {
        let orig = sample_batch(64);
        let header = MultiChannelFrameHeader::batch(7, 3);
        let mut buf = Vec::with_capacity(1024);
        encode_frame_into(header, &orig, &mut buf).expect("encode_frame");

        let (parsed, batch_opt) = decode_frame(&buf).expect("decode_frame");
        assert_eq!(parsed, header);
        assert_eq!(parsed.kind().unwrap(), FrameKind::Batch);
        let decoded = batch_opt.expect("Batch frame must carry a payload");
        assert_eq!(decoded.num_rows(), 64);
        assert_eq!(decoded.schema(), orig.schema());
        assert_eq!(decoded.num_columns(), orig.num_columns());
        for col in 0..orig.num_columns() {
            assert_eq!(orig.column(col).as_ref(), decoded.column(col).as_ref());
        }
    }

    #[test]
    fn frame_round_trips_eof() {
        let mut buf = Vec::new();
        encode_eof_frame_into(2, 5, &mut buf).expect("encode_eof");
        assert_eq!(buf.len(), MULTI_CHANNEL_FRAME_HEADER_SIZE);

        let (header, batch_opt) = decode_frame(&buf).expect("decode_frame");
        assert_eq!(header, MultiChannelFrameHeader::eof(2, 5));
        assert_eq!(header.kind().unwrap(), FrameKind::Eof);
        assert!(batch_opt.is_none());
    }

    #[test]
    fn frame_rejects_short_message() {
        let too_short = vec![0u8; MULTI_CHANNEL_FRAME_HEADER_SIZE - 1];
        let err = decode_frame(&too_short).expect_err("short frame must fail");
        assert!(format!("{err}").contains("too short"));
    }

    #[test]
    fn frame_rejects_bad_magic() {
        // Explicit non-zero, non-magic prefix. Don't rely on `0u32 != FRAME_MAGIC` by chance.
        let mut bad = vec![0u8; MULTI_CHANNEL_FRAME_HEADER_SIZE];
        bad[0..4].copy_from_slice(&0xCAFEBABE_u32.to_le_bytes());
        let err = decode_frame(&bad).expect_err("bad magic must fail");
        assert!(format!("{err}").contains("bad frame magic"));
        bad[0..4].copy_from_slice(&0xDEADBEEF_u32.to_le_bytes());
        let err = decode_frame(&bad).expect_err("bad magic must fail");
        assert!(format!("{err}").contains("bad frame magic"));
    }

    #[test]
    fn frame_rejects_unknown_kind() {
        let header = MultiChannelFrameHeader {
            magic: FRAME_MAGIC,
            flags: 0x42, // unknown kind byte, no reserved bits set
            stage_id: 0,
            partition: 0,
        };
        let mut buf = vec![0u8; MULTI_CHANNEL_FRAME_HEADER_SIZE];
        header.write_to(&mut buf);
        let err = decode_frame(&buf).expect_err("unknown kind must fail");
        assert!(format!("{err}").contains("unknown frame kind"));
    }

    #[test]
    fn frame_rejects_reserved_flag_bits() {
        // Any bit above the low byte of `flags` is reserved-must-be-zero. Setting one
        // should trip `kind()` before the kind byte is consulted.
        let header = MultiChannelFrameHeader {
            magic: FRAME_MAGIC,
            flags: 0x0000_0100, // bit 8 set, kind byte 0 (would be Batch)
            stage_id: 0,
            partition: 0,
        };
        let mut buf = vec![0u8; MULTI_CHANNEL_FRAME_HEADER_SIZE];
        header.write_to(&mut buf);
        let err = decode_frame(&buf).expect_err("reserved bit must fail");
        assert!(format!("{err}").contains("reserved frame flag bits"));
    }

    #[test]
    fn frame_eof_with_payload_is_rejected() {
        let mut buf = Vec::with_capacity(32);
        encode_eof_frame_into(0, 0, &mut buf).expect("encode_eof");
        buf.push(0xAB); // smuggle a payload byte after the Eof header
        let err = decode_frame(&buf).expect_err("Eof+payload must fail");
        assert!(format!("{err}").contains("Eof frame carries payload"));
    }

    #[test]
    fn codec_round_trips_many_batch_sizes() {
        let mut buf = Vec::with_capacity(1024);
        for rows in [0, 1, 7, 64, 1024] {
            let orig = sample_batch(rows);
            encode_frame_into(MultiChannelFrameHeader::batch(0, 0), &orig, &mut buf)
                .expect("encode");
            let (_header, decoded) = decode_frame(&buf).expect("decode");
            let decoded = decoded.expect("Batch frame must carry a payload");
            assert_eq!(orig.num_rows(), decoded.num_rows());
        }
    }
}
