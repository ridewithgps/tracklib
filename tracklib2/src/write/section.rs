use super::crcwriter::CrcWriter;
use super::encoders::*;
use crate::error::Result;
use crate::schema::*;
use crate::types::SectionType;
use std::convert::TryFrom;
use std::io::{self, Write};

impl DataType {
    fn type_tag(&self) -> u8 {
        match self {
            Self::I64 => 0x00,
            Self::F64 => 0x01,
            Self::String => 0x04,
            Self::Bool => 0x05,
        }
    }
}

impl SectionType {
    fn type_tag(&self) -> u8 {
        match self {
            Self::TrackPoints => 0x00,
            Self::CoursePoints => 0x01,
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
        const CRC_BYTES: usize = 4;
        self.buf.len() + CRC_BYTES
    }
}

#[derive(Debug)]
enum Buffer {
    I64(BufferImpl<I64Encoder>),
    F64(BufferImpl<F64Encoder>),
    Bool(BufferImpl<BoolEncoder>),
    String(BufferImpl<StringEncoder>),
}

impl Buffer {
    fn new(data_type: &DataType) -> Self {
        match data_type {
            DataType::I64 => Buffer::I64(BufferImpl::default()),
            DataType::Bool => Buffer::Bool(BufferImpl::default()),
            DataType::String => Buffer::String(BufferImpl::default()),
            DataType::F64 => Buffer::F64(BufferImpl::default()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::I64(buffer_impl) => buffer_impl.buf.len(),
            Self::F64(buffer_impl) => buffer_impl.buf.len(),
            Self::Bool(buffer_impl) => buffer_impl.buf.len(),
            Self::String(buffer_impl) => buffer_impl.buf.len(),
        }
    }
}

pub struct Section {
    section_type: SectionType,
    rows_written: usize,
    schema: Schema,
    column_data: Vec<Buffer>,
}

impl Section {
    // TODO: provide a size_hint param to size buffer Vecs (at least presence)
    pub fn new(section_type: SectionType, schema: Schema) -> Self {
        let column_data = schema
            .fields()
            .iter()
            .map(|field_def| Buffer::new(field_def.data_type()))
            .collect();

        Self {
            section_type,
            rows_written: 0,
            schema,
            column_data,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    /// mut borrow of self so only one row can be open at a time
    pub fn open_row_builder(&mut self) -> RowBuilder {
        self.rows_written += 1; // TODO: bug when you open a row builder but never write data with it?
        RowBuilder::new(&self.schema, &mut self.column_data)
    }

    pub(crate) fn type_tag(&self) -> u8 {
        self.section_type.type_tag()
    }

    pub(crate) fn rows(&self) -> usize {
        self.rows_written
    }

    pub(crate) fn data_size(&self) -> usize {
        const CRC_BYTES: usize = 4;
        let presence_bytes_required = (self.schema.fields().len() + 7) / 8;
        let presence_bytes = (presence_bytes_required * self.rows_written) + CRC_BYTES;
        let data_bytes: usize = self
            .column_data
            .iter()
            .map(|buffer| buffer.len() + CRC_BYTES)
            .sum();
        data_bytes + presence_bytes
    }

    fn write_presence_column<W: Write>(&self, out: &mut W) -> Result<()> {
        let mut crcwriter = CrcWriter::new32(out);
        let bytes_required = (self.schema.fields().len() + 7) / 8;

        for row_i in 0..self.rows_written {
            let mut row = vec![0; bytes_required];
            let mut mask: u8 = 1;
            let mut bit_index = (self.schema.fields().len() + 7) & !7; // next multiple of 8
            for buffer in self.column_data.iter() {
                let is_present = match buffer {
                    Buffer::I64(buffer_impl) => buffer_impl.presence.get(row_i),
                    Buffer::F64(buffer_impl) => buffer_impl.presence.get(row_i),
                    Buffer::Bool(buffer_impl) => buffer_impl.presence.get(row_i),
                    Buffer::String(buffer_impl) => buffer_impl.presence.get(row_i),
                };

                if let Some(true) = is_present {
                    let byte_index = ((bit_index + 7) / 8) - 1;
                    row[byte_index] |= mask;
                }
                mask = mask.rotate_left(1);

                bit_index -= 1;
            }

            crcwriter.write_all(&row)?;
        }
        crcwriter.append_crc()?;

        Ok(())
    }

    #[rustfmt::skip]
    pub(crate) fn write_types_table<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_all(&u8::try_from(self.schema.fields().len())?.to_le_bytes())?; // 1 byte  - number of entries

        for (i, field_def) in self.schema.fields().iter().enumerate() {
            let data_column_size = self
                .column_data
                .get(i)
                .map(|buffer| match buffer {
                    Buffer::I64(buffer_impl) => buffer_impl.data_size(),
                    Buffer::F64(buffer_impl) => buffer_impl.data_size(),
                    Buffer::Bool(buffer_impl) => buffer_impl.data_size(),
                    Buffer::String(buffer_impl) => buffer_impl.data_size(),
                })
                .unwrap_or(0);

            out.write_all(&field_def.data_type().type_tag().to_le_bytes())?;      // 1 byte  - field type tag
            out.write_all(&u8::try_from(field_def.name().len())?.to_le_bytes())?; // 1 byte  - field name length
            out.write_all(field_def.name().as_bytes())?;                          // ? bytes - the name of this field
            leb128::write::unsigned(out, u64::try_from(data_column_size)?)?;      // ? bytes - leb128 column data size
        }

        Ok(())
    }

    #[rustfmt::skip]
    pub(crate) fn write<W: Write>(&self, out: &mut W) -> Result<()> {
        self.write_presence_column(out)?;                                         // ? bytes - presence column with crc
        for buffer in self.column_data.iter() {
            match buffer {
                Buffer::I64(buffer_impl) => buffer_impl.write_data(out)?,         // |
                Buffer::F64(buffer_impl) => buffer_impl.write_data(out)?,         // |
                Buffer::Bool(buffer_impl) => buffer_impl.write_data(out)?,        // |
                Buffer::String(buffer_impl) => buffer_impl.write_data(out)?,      //  - > ? bytes - data column with crc
            };
        }

        Ok(())
    }
}

pub struct RowBuilder<'a> {
    schema: &'a Schema,
    column_data: &'a mut Vec<Buffer>,
    field_index: usize,
}

impl<'a> RowBuilder<'a> {
    fn new(schema: &'a Schema, column_data: &'a mut Vec<Buffer>) -> Self {
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
                ColumnWriter::I64ColumnWriter(ColumnWriterImpl::new(&field_def, buffer_impl)),
            ),
            (Some(field_def), Some(Buffer::F64(ref mut buffer_impl))) => Some(
                ColumnWriter::F64ColumnWriter(ColumnWriterImpl::new(&field_def, buffer_impl)),
            ),
            (Some(field_def), Some(Buffer::Bool(ref mut buffer_impl))) => Some(
                ColumnWriter::BoolColumnWriter(ColumnWriterImpl::new(&field_def, buffer_impl)),
            ),
            (Some(field_def), Some(Buffer::String(ref mut buffer_impl))) => Some(
                ColumnWriter::StringColumnWriter(ColumnWriterImpl::new(&field_def, buffer_impl)),
            ),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum ColumnWriter<'a> {
    I64ColumnWriter(ColumnWriterImpl<'a, I64Encoder>),
    F64ColumnWriter(ColumnWriterImpl<'a, F64Encoder>),
    BoolColumnWriter(ColumnWriterImpl<'a, BoolEncoder>),
    StringColumnWriter(ColumnWriterImpl<'a, StringEncoder>),
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
        &self.field_definition
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
            Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
            ]),
        );
        let mut buf = Vec::new();
        assert_matches!(section.write_presence_column(&mut buf), Ok(()) => {
            assert_eq!(buf,
                       &[0x00, // crc
                         0x00,
                         0x00,
                         0x00]);
        });

        let a_vals = vec![Some(&42), Some(&0), None, Some(&-20)];
        let b_vals = vec![Some(&true), None, Some(&false), Some(&false)];
        let c_vals = vec![
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
                        assert!(cwi.write(a_vals[i]).is_ok());
                    }
                    ColumnWriter::BoolColumnWriter(cwi) => {
                        assert!(cwi.write(b_vals[i]).is_ok());
                    }
                    ColumnWriter::StringColumnWriter(cwi) => {
                        assert!(cwi.write(c_vals[i].as_ref()).is_ok());
                    }
                    ColumnWriter::F64ColumnWriter(_) => {}
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
            Schema::with_fields(
                (0..20)
                    .map(|i| FieldDefinition::new(i.to_string(), DataType::Bool))
                    .collect(),
            ),
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
                       &[0b00001111, 0b11111111, 0b11111111,
                         0b00001111, 0b11111111, 0b11111111,
                         0xDD, // crc
                         0xCB,
                         0x18,
                         0x17]);
        });
    }

    #[test]
    fn test_write_huge_presence_column() {
        let mut section = Section::new(
            SectionType::TrackPoints,
            Schema::with_fields(
                (0..80)
                    .map(|i| FieldDefinition::new(i.to_string(), DataType::Bool))
                    .collect(),
            ),
        );

        #[rustfmt::skip]
        let vals = &[
            Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 1
            Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 2
            None,        None,        Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 3
            Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), None,        None,        None,        // 4
            Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 5
            None,        Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 6
            Some(&true), Some(&true), Some(&true), Some(&true), None,        None,        Some(&true), Some(&true), // 7
            None,        None,        None,        None,        None,        None,        None,        None,        // 8
            Some(&true), Some(&true), Some(&true), Some(&true), None,        Some(&true), None,        None,        // 9
            None,        None,        None,        Some(&true), Some(&true), Some(&true), Some(&true), Some(&true), // 10
        ];

        let mut rowbuilder = section.open_row_builder();
        let mut i = 0;
        while let Some(cw) = rowbuilder.next_column_writer() {
            match cw {
                ColumnWriter::BoolColumnWriter(cwi) => assert!(cwi.write(vals[i]).is_ok()),
                _ => assert!(false, "unexpected column writer type here"),
            }
            i += 1;
        }

        let mut buf = Vec::new();
        assert_matches!(section.write_presence_column(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0b11111000, // 10
                         0b00101111, // 9
                         0b00000000, // 8
                         0b11001111, // 7
                         0b11111110, // 6
                         0b11111111, // 5
                         0b00011111, // 4
                         0b11111100, // 3
                         0b11111111, // 2
                         0b11111111, // 1
                         0x92, // crc
                         0x0E,
                         0x6F,
                         0xC2]);
        });
    }

    #[test]
    fn test_types_table() {
        let mut section = Section::new(
            SectionType::TrackPoints,
            Schema::with_fields(vec![
                FieldDefinition::new("m", DataType::I64),
                FieldDefinition::new("k", DataType::Bool),
                FieldDefinition::new("long name!", DataType::String),
                FieldDefinition::new("i", DataType::I64),
                FieldDefinition::new("f", DataType::F64),
            ]),
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
                ColumnWriter::F64ColumnWriter(cwi) => {
                    assert!(cwi.write(Some(&0.0042)).is_ok());
                }
            }
        }

        let mut buf = Vec::new();
        assert_matches!(section.write_types_table(&mut buf), Ok(()) => {
            #[rustfmt::skip]
            assert_eq!(buf,
                       &[0x05, // entry count = 5
                         0x00, // first entry type: i64 = 0
                         0x01, // name len = 1
                         b'm', // name = "m"
                         0x06, // data size = 6
                         0x05, // second entry type: bool = 5
                         0x01, // name len = 1
                         b'k', // name = "k"
                         0x05, // data size = 5
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
                         0x0B, // data size = 11
                         0x00, // fourth entry type: i64 = 0
                         0x01, // name len = 1
                         b'i', // name = "i"
                         0x06, // data size = 6
                         0x01, // fifth entry type: f64 = 1
                         0x01, // name len = 1
                         b'f', // name = "f"
                         0x07, // data size = 7
                       ]);
        });
    }

    #[test]
    fn test_writing_a_section() {
        enum V {
            I64(i64),
            F64(f64),
            Bool(bool),
            String(String),
        }

        let mut v = Vec::new();
        let mut h = HashMap::new();
        h.insert("a", V::I64(1));
        h.insert("b", V::Bool(false));
        h.insert("c", V::String("Ride".to_string()));
        h.insert("d", V::F64(0.0));
        v.push(h);
        let mut h = HashMap::new();
        h.insert("a", V::I64(2));
        h.insert("c", V::String("with".to_string()));
        v.push(h);
        let mut h = HashMap::new();
        h.insert("a", V::I64(4));
        h.insert("b", V::Bool(true));
        h.insert("c", V::String("GPS".to_string()));
        h.insert("d", V::F64(2112.90125));
        v.push(h);

        let mut section = Section::new(
            SectionType::TrackPoints,
            Schema::with_fields(vec![
                FieldDefinition::new("a", DataType::I64),
                FieldDefinition::new("b", DataType::Bool),
                FieldDefinition::new("c", DataType::String),
                FieldDefinition::new("d", DataType::F64),
            ]),
        );

        let fields = section.schema().fields().to_vec();

        for entry in v {
            let mut rowbuilder = section.open_row_builder();

            for field_def in fields.iter() {
                assert_matches!(rowbuilder.next_column_writer(), Some(cw) => {
                    match cw {
                        ColumnWriter::I64ColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
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
                                    .get(field_def.name())
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
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        V::String(v) => Some(v),
                                        _ => None,
                                    })
                                    .flatten(),
                            ).is_ok());
                        }
                        ColumnWriter::F64ColumnWriter(cwi) => {
                            assert!(cwi.write(
                                entry
                                    .get(field_def.name())
                                    .map(|v| match v {
                                        V::F64(v) => Some(v),
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
                0b00001111,
                0b00000101,
                0b00001111,
                0x9A, // crc
                0xFC,
                0x27,
                0xEC,

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
                // None
                0x01, // true
                0x35, // crc
                0x86,
                0x89,
                0xFB,

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
                0x48,

                // Data Column 4 = F64
                0x0, // 0.0
                // None
                0x94, // 2112.90125
                0xCA,
                0x8C,
                0xDB,
                0xCE,
                0x00,
                0xF0, // crc
                0xA4,
                0x8A,
                0xDD]);
        });
    }
}
