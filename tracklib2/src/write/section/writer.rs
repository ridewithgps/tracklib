use crate::error::Result;
use crate::schema::*;
use crate::write::crcwriter::CrcWriter;
use crate::write::encoders::*;
use std::io::{self, Write};

#[derive(Debug)]
pub(crate) struct BufferImpl<E: Encoder> {
    buf: Vec<u8>,
    presence: Vec<bool>,
    encoder: E,
}

impl<E: Encoder> BufferImpl<E> {
    fn new(encoder: E) -> Self {
        Self {
            encoder,
            presence: vec![],
            buf: vec![],
        }
    }

    fn write_data<W: Write>(&self, out: &mut W) -> Result<()> {
        let mut crcwriter = CrcWriter::new32(out);
        io::copy(&mut io::Cursor::new(&self.buf), &mut crcwriter)?;
        crcwriter.append_crc()?;
        Ok(())
    }

    fn data_size(&self) -> usize {
        const CRC_BYTES: usize = 4;
        self.buf.len() + CRC_BYTES
    }
}

#[derive(Debug)]
pub(crate) enum Buffer {
    I64(BufferImpl<I64Encoder>),
    U64(BufferImpl<U64Encoder>),
    F64(BufferImpl<F64Encoder>),
    Bool(BufferImpl<BoolEncoder>),
    String(BufferImpl<StringEncoder>),
    BoolArray(BufferImpl<BoolArrayEncoder>),
    U64Array(BufferImpl<U64ArrayEncoder>),
    ByteArray(BufferImpl<ByteArrayEncoder>),
}

impl Buffer {
    pub(crate) fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::I64 => Buffer::I64(BufferImpl::new(I64Encoder::default())),
            DataType::U64 => Buffer::U64(BufferImpl::new(U64Encoder::default())),
            DataType::Bool => Buffer::Bool(BufferImpl::new(BoolEncoder::default())),
            DataType::String => Buffer::String(BufferImpl::new(StringEncoder::default())),
            DataType::F64 { scale } => Buffer::F64(BufferImpl::new(F64Encoder::new(*scale))),
            DataType::BoolArray => Buffer::BoolArray(BufferImpl::new(BoolArrayEncoder::default())),
            DataType::U64Array => Buffer::U64Array(BufferImpl::new(U64ArrayEncoder::default())),
            DataType::ByteArray => Buffer::ByteArray(BufferImpl::new(ByteArrayEncoder::default())),
        }
    }

    pub(crate) fn data_size(&self) -> usize {
        match self {
            Self::I64(buffer_impl) => buffer_impl.data_size(),
            Self::U64(buffer_impl) => buffer_impl.data_size(),
            Self::F64(buffer_impl) => buffer_impl.data_size(),
            Self::Bool(buffer_impl) => buffer_impl.data_size(),
            Self::String(buffer_impl) => buffer_impl.data_size(),
            Self::BoolArray(buffer_impl) => buffer_impl.data_size(),
            Self::U64Array(buffer_impl) => buffer_impl.data_size(),
            Self::ByteArray(buffer_impl) => buffer_impl.data_size(),
        }
    }

    pub(crate) fn is_present(&self, row: usize) -> Option<bool> {
        match self {
            Self::I64(buffer_impl) => buffer_impl.presence.get(row).copied(),
            Self::U64(buffer_impl) => buffer_impl.presence.get(row).copied(),
            Self::F64(buffer_impl) => buffer_impl.presence.get(row).copied(),
            Self::Bool(buffer_impl) => buffer_impl.presence.get(row).copied(),
            Self::String(buffer_impl) => buffer_impl.presence.get(row).copied(),
            Self::BoolArray(buffer_impl) => buffer_impl.presence.get(row).copied(),
            Self::U64Array(buffer_impl) => buffer_impl.presence.get(row).copied(),
            Self::ByteArray(buffer_impl) => buffer_impl.presence.get(row).copied(),
        }
    }

    pub(crate) fn write_data<W: Write>(&self, out: &mut W) -> Result<()> {
        match self {
            Self::I64(buffer_impl) => buffer_impl.write_data(out),
            Self::U64(buffer_impl) => buffer_impl.write_data(out),
            Self::F64(buffer_impl) => buffer_impl.write_data(out),
            Self::Bool(buffer_impl) => buffer_impl.write_data(out),
            Self::String(buffer_impl) => buffer_impl.write_data(out),
            Self::BoolArray(buffer_impl) => buffer_impl.write_data(out),
            Self::U64Array(buffer_impl) => buffer_impl.write_data(out),
            Self::ByteArray(buffer_impl) => buffer_impl.write_data(out),
        }
    }
}

pub struct RowBuilder<'a> {
    schema: &'a Schema,
    column_data: &'a mut Vec<Buffer>,
    field_index: usize,
}

impl<'a> RowBuilder<'a> {
    pub(crate) fn new(schema: &'a Schema, column_data: &'a mut Vec<Buffer>) -> Self {
        Self {
            schema,
            column_data,
            field_index: 0,
        }
    }

    /// mut borrow of self so only one column writer can be open at a time
    pub fn next_column_writer(&mut self) -> Option<ColumnWriter> {
        let field_index = self.field_index;
        self.field_index += 1;

        let maybe_field_def = self.schema.fields().get(field_index);
        let maybe_buffer = self.column_data.get_mut(field_index);

        match (maybe_field_def, maybe_buffer) {
            (Some(field_def), Some(Buffer::I64(ref mut buffer_impl))) => Some(
                ColumnWriter::I64ColumnWriter(ColumnWriterImpl::new(field_def, buffer_impl)),
            ),
            (Some(field_def), Some(Buffer::U64(ref mut buffer_impl))) => Some(
                ColumnWriter::U64ColumnWriter(ColumnWriterImpl::new(field_def, buffer_impl)),
            ),
            (Some(field_def), Some(Buffer::F64(ref mut buffer_impl))) => Some(
                ColumnWriter::F64ColumnWriter(ColumnWriterImpl::new(field_def, buffer_impl)),
            ),
            (Some(field_def), Some(Buffer::Bool(ref mut buffer_impl))) => Some(
                ColumnWriter::BoolColumnWriter(ColumnWriterImpl::new(field_def, buffer_impl)),
            ),
            (Some(field_def), Some(Buffer::String(ref mut buffer_impl))) => Some(
                ColumnWriter::StringColumnWriter(ColumnWriterImpl::new(field_def, buffer_impl)),
            ),
            (Some(field_def), Some(Buffer::BoolArray(ref mut buffer_impl))) => Some(
                ColumnWriter::BoolArrayColumnWriter(ColumnWriterImpl::new(field_def, buffer_impl)),
            ),
            (Some(field_def), Some(Buffer::U64Array(ref mut buffer_impl))) => Some(
                ColumnWriter::U64ArrayColumnWriter(ColumnWriterImpl::new(field_def, buffer_impl)),
            ),
            (Some(field_def), Some(Buffer::ByteArray(ref mut buffer_impl))) => Some(
                ColumnWriter::ByteArrayColumnWriter(ColumnWriterImpl::new(field_def, buffer_impl)),
            ),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum ColumnWriter<'a> {
    I64ColumnWriter(ColumnWriterImpl<'a, I64Encoder>),
    U64ColumnWriter(ColumnWriterImpl<'a, U64Encoder>),
    F64ColumnWriter(ColumnWriterImpl<'a, F64Encoder>),
    BoolColumnWriter(ColumnWriterImpl<'a, BoolEncoder>),
    StringColumnWriter(ColumnWriterImpl<'a, StringEncoder>),
    BoolArrayColumnWriter(ColumnWriterImpl<'a, BoolArrayEncoder>),
    U64ArrayColumnWriter(ColumnWriterImpl<'a, U64ArrayEncoder>),
    ByteArrayColumnWriter(ColumnWriterImpl<'a, ByteArrayEncoder>),
}

#[derive(Debug)]
// TODO: is this must_use correct?
#[must_use]
pub struct ColumnWriterImpl<'a, E: Encoder> {
    field_definition: &'a FieldDefinition,
    buf: &'a mut BufferImpl<E>,
}

impl<'a, E: Encoder> ColumnWriterImpl<'a, E> {
    fn new(field_definition: &'a FieldDefinition, buf: &'a mut BufferImpl<E>) -> Self {
        Self {
            field_definition,
            buf,
        }
    }

    pub fn field_definition(&self) -> &FieldDefinition {
        self.field_definition
    }

    /// Takes `self` so that only one value per column can be written
    pub fn write(self, value: Option<&E::T>) -> Result<()> {
        let BufferImpl {
            ref mut encoder,
            ref mut buf,
            ref mut presence,
            ..
        } = self.buf;
        encoder.encode(value, buf, presence)
    }
}
