use super::crcwriter::CrcWriter;
use super::encoders::{BoolEncoder, Encoder, I64Encoder, StringEncoder};
use crate::consts::{CRC16, CRC32};
use crate::error::Result;
use crate::types::{FieldDescription, FieldType};
use std::convert::TryFrom;
use std::io::{self, Write};

impl FieldType {
    fn type_tag(&self) -> u8 {
        match self {
            Self::I64 => 0x00,
            Self::String => 0x04,
            Self::Bool => 0x05,
        }
    }
}

#[derive(Default, Debug)]
struct BufferImpl<E: Encoder> {
    buf: Vec<u8>,
    presence: Vec<bool>,
    encoder: E,
}

impl<E: Encoder> BufferImpl<E> {
    fn write_data<W: Write>(&self, out: &mut W) -> Result<()> {
        let mut crcwriter = CrcWriter::new32(out);
        io::copy(&mut io::Cursor::new(&self.buf), &mut crcwriter)?;
        crcwriter.append_crc()?;
        Ok(())
    }

    fn data_size(&self) -> usize {
        self.buf.len()
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

    fn len(&self) -> usize {
        match self {
            Self::I64(buffer_impl) => buffer_impl.buf.len(),
            Self::Bool(buffer_impl) => buffer_impl.buf.len(),
            Self::String(buffer_impl) => buffer_impl.buf.len(),
        }
    }
}

pub enum SectionType {
    TrackPoints,
    CoursePoints,
}

impl SectionType {
    fn type_tag(&self) -> u8 {
        match self {
            Self::TrackPoints => 0x00,
            Self::CoursePoints => 0x01,
        }
    }
}

pub struct Section {
    section_type: SectionType,
    rows_written: usize,
    fields: Vec<FieldDescription>,
    column_data: Vec<Buffer>,
}

impl Section {
    // TODO: provide a size_hint param to size buffer Vecs (at least presence)
    pub fn new(section_type: SectionType, mapping: Vec<(String, FieldType)>) -> Self {
        let mut fields = Vec::with_capacity(mapping.len());
        let mut column_data = Vec::with_capacity(mapping.len());

        for (name, fieldtype) in mapping {
            column_data.push(Buffer::new(&fieldtype));
            fields.push(FieldDescription::new(name, fieldtype));
        }

        Self {
            section_type,
            rows_written: 0,
            fields,
            column_data,
        }
    }

    pub fn fields(&self) -> &[FieldDescription] {
        &self.fields
    }

    /// mut borrow of self so only one row can be open at a time
    pub fn open_row_builder(&mut self) -> RowBuilder {
        self.rows_written += 1; // TODO: bug when you open a row builder but never write data with it?
        RowBuilder::new(&self.fields, &mut self.column_data)
    }

    pub(crate) fn type_tag(&self) -> u8 {
        self.section_type.type_tag()
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows_written
    }

    pub(crate) fn data_size(&self) -> usize {
        let presence_bytes_required = (self.fields.len() + 7) / 8;
        let presence_bytes = presence_bytes_required * self.rows_written;
        let data_bytes: usize = self.column_data.iter().map(|buffer| buffer.len()).sum();
        data_bytes + presence_bytes
    }

    fn write_presence_column<W: Write>(&self, out: &mut W) -> Result<()> {
        let mut crcwriter = CrcWriter::new32(out);
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

            crcwriter.write_all(&entry.to_le_bytes()[..bytes_required])?;
        }
        crcwriter.append_crc()?;

        Ok(())
    }

    #[rustfmt::skip]
    pub(crate) fn write_types_table<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_all(&u8::try_from(self.fields.len())?.to_le_bytes())?;          // 1 byte  - number of entries

        for (i, field) in self.fields.iter().enumerate() {
            let data_column_size = self
                .column_data
                .get(i)
                .map(|buffer| match buffer {
                    Buffer::I64(buffer_impl) => buffer_impl.data_size(),
                    Buffer::Bool(buffer_impl) => buffer_impl.data_size(),
                    Buffer::String(buffer_impl) => buffer_impl.data_size(),
                })
                .unwrap_or(0);

            out.write_all(&field.fieldtype().type_tag().to_le_bytes())?;          // 1 byte  - field type tag
            out.write_all(&u8::try_from(field.name().len())?.to_le_bytes())?;     // 1 byte  - field name length
            out.write_all(field.name().as_bytes())?;                              // ? bytes - the name of this field
            leb128::write::unsigned(out, u64::try_from(data_column_size)?)?;      // ? bytes - leb128 column data size
        }

        Ok(())
    }

    #[rustfmt::skip]
    pub(crate) fn write<W: Write>(&self, out: &mut W) -> Result<()> {
        self.write_presence_column(out)?;                                         // ? bytes - presence column with crc
        for buffer in self.column_data.iter() {
            match buffer {
                Buffer::I64(buffer_impl) => buffer_impl.write_data(out)?,         // \
                Buffer::Bool(buffer_impl) => buffer_impl.write_data(out)?,        //  \
                Buffer::String(buffer_impl) => buffer_impl.write_data(out)?,      //   > ? bytes - data column with crc
            };
        }

        Ok(())
    }
}

pub struct RowBuilder<'a> {
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
    pub fn next_column_writer(&mut self) -> Option<ColumnWriter> {
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

#[derive(Debug)]
pub enum ColumnWriter<'a> {
    I64ColumnWriter(ColumnWriterImpl<'a, I64Encoder>),
    BoolColumnWriter(ColumnWriterImpl<'a, BoolEncoder>),
    StringColumnWriter(ColumnWriterImpl<'a, StringEncoder>),
}

#[derive(Debug)]
// TODO: is this must_use correct?
#[must_use]
pub struct ColumnWriterImpl<'a, E: Encoder> {
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

    pub fn field_description(&self) -> &FieldDescription {
        &self.field_description
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

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use std::collections::HashMap;

    #[test]
    fn test_write_presence_column() {
        let mut section = Section::new(
            SectionType::TrackPoints,
            vec![
                ("a".to_string(), FieldType::I64),
                ("b".to_string(), FieldType::Bool),
                ("c".to_string(), FieldType::String),
            ],
        );
        let mut buf = Vec::new();
        assert_matches!(section.write_presence_column(&mut buf), Ok(()) => {
            assert_eq!(buf,
                       &[0x00, // crc
                         0x00,
                         0x00,
                         0x00]);
        });

        let m_vals = vec![Some(&42), Some(&0), None, Some(&-20)];
        let k_vals = vec![Some(&true), None, Some(&false), Some(&false)];
        let j_vals = vec![
            None,
            Some("hi".to_string()),
            Some("tracklib".to_string()),
            Some("!".to_string()),
        ];

        for i in 0..4 {
            let mut rowbuilder = section.open_row_builder();
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

        let mut buf = Vec::new();
        assert_matches!(section.write_presence_column(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0b00000011,
                         0b00000101,
                         0b00000110,
                         0b00000111,
                         0xD2, // crc
                         0x61,
                         0xA7,
                         0xA5]);
        });
    }

    #[test]
    fn test_multibyte_presence_column() {
        let mut section = Section::new(
            SectionType::TrackPoints,
            vec![
                ("1".to_string(), FieldType::Bool),
                ("2".to_string(), FieldType::Bool),
                ("3".to_string(), FieldType::Bool),
                ("4".to_string(), FieldType::Bool),
                ("5".to_string(), FieldType::Bool),
                ("6".to_string(), FieldType::Bool),
                ("7".to_string(), FieldType::Bool),
                ("8".to_string(), FieldType::Bool),
                ("9".to_string(), FieldType::Bool),
                ("10".to_string(), FieldType::Bool),
                ("11".to_string(), FieldType::Bool),
                ("12".to_string(), FieldType::Bool),
                ("13".to_string(), FieldType::Bool),
                ("14".to_string(), FieldType::Bool),
                ("15".to_string(), FieldType::Bool),
                ("16".to_string(), FieldType::Bool),
                ("17".to_string(), FieldType::Bool),
                ("18".to_string(), FieldType::Bool),
                ("19".to_string(), FieldType::Bool),
                ("20".to_string(), FieldType::Bool),
            ],
        );

        for i in 0..2 {
            let mut rowbuilder = section.open_row_builder();
            while let Some(cw) = rowbuilder.next_column_writer() {
                match cw {
                    ColumnWriter::BoolColumnWriter(cwi) => assert!(cwi.write(Some(&true)).is_ok()),
                    _ => assert!(false, "unexpected column writer type here"),
                }
            }
        }

        let mut buf = Vec::new();
        assert_matches!(section.write_presence_column(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0b11111111, 0b11111111, 0b00001111,
                         0b11111111, 0b11111111, 0b00001111,
                         0x91, // crc
                         0xA0,
                         0x07,
                         0xE3]);
        });
    }

    #[test]
    fn test_types_table() {
        let mut section = Section::new(
            SectionType::TrackPoints,
            vec![
                ("m".to_string(), FieldType::I64),
                ("k".to_string(), FieldType::Bool),
                ("long name!".to_string(), FieldType::String),
                ("i".to_string(), FieldType::I64),
            ],
        );

        let mut rowbuilder = section.open_row_builder();
        while let Some(cw) = rowbuilder.next_column_writer() {
            match cw {
                ColumnWriter::I64ColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&500)).is_ok());
                }
                ColumnWriter::BoolColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&false)).is_ok());
                }
                ColumnWriter::StringColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&"Hello!".to_string())).is_ok());
                }
            }
        }

        let mut buf = Vec::new();
        assert_matches!(section.write_types_table(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x04, // entry count = 4
                         0x00, // first entry type: i64 = 0
                         0x01, // name len = 1
                         b'm', // name = "m"
                         0x02, // data size = 2
                         0x05, // second entry type: bool = 5
                         0x01, // name len = 1
                         b'k', // name = "k"
                         0x01, // data size = 1
                         0x04, // third entry type: string = 4
                         0x0A, // name len = 10
                         b'l', // name = "long name!"
                         b'o',
                         b'n',
                         b'g',
                         b' ',
                         b'n',
                         b'a',
                         b'm',
                         b'e',
                         b'!',
                         0x07, // data size = 7 ("Hello!" + leb128 length prefix)
                         0x00, // fourth entry type: i64 = 0
                         0x01, // name len = 1
                         b'i', // name = "i"
                         0x02]); // data size = 2
        });
    }

    #[test]
    fn test_writing_a_section() {
        enum V {
            I64(i64),
            Bool(bool),
            String(String),
        }

        let mut v = Vec::new();
        let mut h = HashMap::new();
        h.insert("a", V::I64(1));
        h.insert("b", V::Bool(false));
        h.insert("c", V::String("Ride".to_string()));
        v.push(h);
        let mut h = HashMap::new();
        h.insert("a", V::I64(2));
        h.insert("c", V::String("with".to_string()));
        v.push(h);
        let mut h = HashMap::new();
        h.insert("a", V::I64(4));
        h.insert("b", V::Bool(true));
        h.insert("c", V::String("GPS".to_string()));
        v.push(h);

        let mut section = Section::new(
            SectionType::TrackPoints,
            vec![
                ("a".to_string(), FieldType::I64),
                ("b".to_string(), FieldType::Bool),
                ("c".to_string(), FieldType::String),
            ],
        );

        let mapping = section.fields().to_vec();

        for entry in v {
            let mut rowbuilder = section.open_row_builder();

            for field_desc in mapping.iter() {
                assert_matches!(rowbuilder.next_column_writer(), Some(cw) => {
                    match cw {
                        ColumnWriter::I64ColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_desc.name())
                                    .map(|v| match v {
                                        V::I64(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::BoolColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_desc.name())
                                    .map(|v| match v {
                                        V::Bool(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::StringColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_desc.name())
                                    .map(|v| match v {
                                        V::String(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                    }
                });
            }
        }

        let mut buf = Vec::new();
        assert_matches!(section.write(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf, &[
                // Presence Column
                0b00000111,
                0b00000101,
                0b00000111,
                0x1A, // crc
                0x75,
                0xEA,
                0xC4,

                // Data Column 1 = I64
                0x01, // 1
                0x01, // 2
                0x02, // 4
                0xCA, // crc
                0xD4,
                0xD8,
                0x92,

                // Data Column 2 = Bool
                0x00, // false
                0x00, // missing
                0x01, // true
                0x48, // crc
                0x9F,
                0x5A,
                0x4C,

                // Data Column 3 = String
                0x04, // length 4
                b'R',
                b'i',
                b'd',
                b'e',
                0x04, // length 4
                b'w',
                b'i',
                b't',
                b'h',
                0x03, // length 3
                b'G',
                b'P',
                b'S',
                0xA3, // crc
                0x02,
                0xEC,
                0x48]);
        });
    }
}
