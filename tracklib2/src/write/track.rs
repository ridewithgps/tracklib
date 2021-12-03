use super::encoders::{BoolEncoder, Encoder, I64Encoder, StringEncoder};
use super::types::FieldType;
use crate::error::Result;
use std::convert::TryFrom;
use std::io::{self, Write};

#[derive(Default, Debug)]
struct BufferImpl<E: Encoder> {
    buf: Vec<u8>,
    presence: Vec<bool>,
    encoder: E,
}

impl<E: Encoder> BufferImpl<E> {
    fn write_data<W: Write>(&self, out: &mut W) -> Result<usize> {
        let written = io::copy(&mut io::Cursor::new(&self.buf), out)?;
        // io::copy (annoyingly) returns u64 so coerce it to a usize to return
        Ok(usize::try_from(written).unwrap())
    }
}

#[derive(Debug)]
enum Buffer {
    I64(BufferImpl<I64Encoder>),
    Bool(BufferImpl<BoolEncoder>),
    String(BufferImpl<StringEncoder>),
}

impl Buffer {
    fn new(field_type: &FieldType) -> Self {
        match field_type {
            &FieldType::I64 => Buffer::I64(BufferImpl::default()),
            &FieldType::Bool => Buffer::Bool(BufferImpl::default()),
            &FieldType::String => Buffer::String(BufferImpl::default()),
        }
    }
}

#[derive(Clone, Debug)]
struct FieldDescription {
    name: String,
    fieldtype: FieldType,
}

struct TrackWriter {
    rows_written: usize,
    fields: Vec<FieldDescription>,
    column_data: Vec<Buffer>,
}

impl TrackWriter {
    // TODO: provide a size_hint param to size buffer Vecs (at least presence)
    fn new(mapping: Vec<(String, FieldType)>) -> Self {
        let mut fields = Vec::with_capacity(mapping.len());
        let mut column_data = Vec::with_capacity(mapping.len());

        for (name, fieldtype) in mapping {
            column_data.push(Buffer::new(&fieldtype));
            fields.push(FieldDescription { name, fieldtype });
        }

        Self {
            rows_written: 0,
            fields,
            column_data,
        }
    }

    fn fields(&self) -> &[FieldDescription] {
        &self.fields
    }

    fn field_description(&self, name: &str) -> Option<&FieldDescription> {
        self.fields
            .iter()
            .enumerate()
            .filter_map(|(i, field_desc)| {
                if field_desc.name == name {
                    Some(field_desc)
                } else {
                    None
                }
            })
            .next()
    }

    /// mut borrow of self so only one row can be open at a time
    fn open_row_builder(&mut self) -> RowBuilder {
        self.rows_written += 1; // TODO: bug when you open a row builder but never write data with it?
        RowBuilder::new(&self.fields, &mut self.column_data)
    }

    fn write_presence_column<W: Write>(&self, out: &mut W) -> Result<usize> {
        let mut bytes_written = 0;
        let bytes_required = (self.fields.len() + 7) / 8;

        for row_i in 0..self.rows_written {
            let mut entry: u64 = 0;
            for (field_i, buffer) in self.column_data.iter().enumerate() {
                let bit = match buffer {
                    Buffer::I64(buffer_impl) => {
                        if let Some(true) = buffer_impl.presence.get(row_i) {
                            1
                        } else {
                            0
                        }
                    }
                    Buffer::Bool(buffer_impl) => {
                        if let Some(true) = buffer_impl.presence.get(row_i) {
                            1
                        } else {
                            0
                        }
                    }
                    Buffer::String(buffer_impl) => {
                        if let Some(true) = buffer_impl.presence.get(row_i) {
                            1
                        } else {
                            0
                        }
                    }
                };
                entry |= bit << field_i;
            }

            out.write_all(&entry.to_le_bytes()[..bytes_required])?;
            bytes_written += bytes_required;
        }

        Ok(bytes_written)
    }
}

struct RowBuilder<'a> {
    fields: &'a [FieldDescription],
    column_data: &'a mut Vec<Buffer>,
    field_index: usize,
}

impl<'a> RowBuilder<'a> {
    fn new(fields: &'a [FieldDescription], column_data: &'a mut Vec<Buffer>) -> Self {
        Self {
            fields,
            column_data,
            field_index: 0,
        }
    }

    /// mut borrow of self so only one column writer can be open at a time
    fn next_column_writer(&mut self) -> Option<ColumnWriter> {
        let field_index = self.field_index;
        self.field_index += 1;

        let maybe_field_desc = self.fields.get(field_index);
        let maybe_buffer = self.column_data.get_mut(field_index);

        match (maybe_field_desc, maybe_buffer) {
            (Some(field_desc), Some(Buffer::I64(ref mut buffer_impl))) => Some(
                ColumnWriter::I64ColumnWriter(ColumnWriterImpl::new(&field_desc, buffer_impl)),
            ),
            (Some(field_desc), Some(Buffer::Bool(ref mut buffer_impl))) => Some(
                ColumnWriter::BoolColumnWriter(ColumnWriterImpl::new(&field_desc, buffer_impl)),
            ),
            (Some(field_desc), Some(Buffer::String(ref mut buffer_impl))) => Some(
                ColumnWriter::StringColumnWriter(ColumnWriterImpl::new(&field_desc, buffer_impl)),
            ),
            _ => None,
        }
    }
}

enum ColumnWriter<'a> {
    I64ColumnWriter(ColumnWriterImpl<'a, I64Encoder>),
    BoolColumnWriter(ColumnWriterImpl<'a, BoolEncoder>),
    StringColumnWriter(ColumnWriterImpl<'a, StringEncoder>),
}

// TODO: is this must_use correct?
#[must_use]
struct ColumnWriterImpl<'a, E: Encoder> {
    field_description: &'a FieldDescription,
    buf: &'a mut BufferImpl<E>,
}

impl<'a, E: Encoder> ColumnWriterImpl<'a, E> {
    fn new(field_description: &'a FieldDescription, buf: &'a mut BufferImpl<E>) -> Self {
        Self {
            field_description,
            buf,
        }
    }

    fn field_description(&self) -> &FieldDescription {
        &self.field_description
    }

    /// Takes `self` so that only one value per column can be written
    fn write(self, value: Option<&E::T>) -> Result<()> {
        let BufferImpl {
            ref mut encoder,
            ref mut buf,
            ref mut presence,
            ..
        } = self.buf;
        encoder.encode(value, buf, presence)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_presence_column() {
        let mut trackwriter = TrackWriter::new(vec![
            ("a".to_string(), FieldType::I64),
            ("b".to_string(), FieldType::Bool),
            ("c".to_string(), FieldType::String),
        ]);
        let mut buf = Vec::new();
        let bytes_written = trackwriter.write_presence_column(&mut buf);
        assert!(bytes_written.is_ok());
        assert_eq!(bytes_written.unwrap(), 0);
        assert_eq!(buf.len(), 0);

        let m_vals = vec![Some(&42), Some(&0), None, Some(&-20)];
        let k_vals = vec![Some(&true), None, Some(&false), Some(&false)];
        let j_vals = vec![
            None,
            Some("hi".to_string()),
            Some("tracklib".to_string()),
            Some("!".to_string()),
        ];

        for i in 0..4 {
            let mut rowbuilder = trackwriter.open_row_builder();
            while let Some(cw) = rowbuilder.next_column_writer() {
                match cw {
                    ColumnWriter::I64ColumnWriter(cwi) => {
                        assert!(cwi.write(m_vals[i]).is_ok());
                    }
                    ColumnWriter::BoolColumnWriter(cwi) => {
                        assert!(cwi.write(k_vals[i]).is_ok());
                    }
                    ColumnWriter::StringColumnWriter(cwi) => {
                        assert!(cwi.write(j_vals[i].as_ref()).is_ok());
                    }
                }
            }
        }

        let bytes_written = trackwriter.write_presence_column(&mut buf);
        assert!(bytes_written.is_ok());
        assert_eq!(bytes_written.unwrap(), 4);
        assert_eq!(buf.len(), 4);
        assert_eq!(buf[0], 0b00000011);
        assert_eq!(buf[1], 0b00000101);
        assert_eq!(buf[2], 0b00000110);
        assert_eq!(buf[3], 0b00000111);
    }
}
