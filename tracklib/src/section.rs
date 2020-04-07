use std::io::{Write};
use snafu::{Snafu, ResultExt};
use std::collections::btree_map::{self, BTreeMap};
use std::convert::{TryFrom};
use std::cmp;
use serde::ser::{Serialize, Serializer, SerializeSeq, SerializeMap};
use crate::rwtfile::{DataField};
use crate::flagscolumn::{self, FlagsColumn};
use crate::utils::{write};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Column {} tried to change type", name))]
    ColumnTypeChange{name: String},
    #[snafu(display("Column {} tried to reused index {}", name, index))]
    IndexAlreadyUsed{name: String, index: usize},
    #[snafu(display("Couldn't write types table: {}", source))]
    WriteTypesTable{source: std::io::Error},
    #[snafu(display("Couldn't write column {}: {}", name, source))]
    WriteDataColumn{name: String, source: std::io::Error},
    #[snafu(display("Couldn't write flags column: {}", source))]
    WriteFlagsColumn{source: flagscolumn::Error},
    #[snafu(display("Couldn't write section header: {}", source))]
    WriteHeader{source: std::io::Error},
    #[snafu(display("Couldn't write: {}", source))]
    WriteBytes{source: std::io::Error},
    #[snafu(display("Couldn't write data column - number of points"))]
    WriteDataColumnNumberOfPoints{},
    #[snafu(display("Number truncation error: {}", source))]
    NumberTruncation{source: std::num::TryFromIntError},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;


#[derive(Debug)]
pub enum Column {
    Numbers(BTreeMap<usize, i64>),
    LongFloat(BTreeMap<usize, f64>),
    ShortFloat(BTreeMap<usize, f64>),
    Base64(BTreeMap<usize, Vec<u8>>),
    String(BTreeMap<usize, String>),
    Bool(BTreeMap<usize, bool>),
    IDs(BTreeMap<usize, Vec<u64>>),
}

impl Column {
    fn type_tag(&self) -> u8 {
        match self {
            Column::Numbers(_)    => 0x00,
            Column::LongFloat(_)  => 0x01,
            Column::ShortFloat(_) => 0x02,
            Column::Base64(_)     => 0x03,
            Column::String(_)     => 0x04,
            Column::Bool(_)       => 0x05,
            Column::IDs(_)        => 0x06,
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum SectionType {
    TrackPoints,
    CoursePoints,
    Continuation,
}

impl SectionType {
    pub(crate) fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x00 => Some(SectionType::TrackPoints),
            0x01 => Some(SectionType::CoursePoints),
            0x02 => Some(SectionType::Continuation),
            // 0xff is reserved for the RWTF Trailer
            _ => None
        }
    }

    fn type_tag(&self) -> u8 {
        match self {
            SectionType::TrackPoints  => 0x00,
            SectionType::CoursePoints => 0x01,
            SectionType::Continuation => 0x02,
        }
    }
}

#[derive(Debug)]
pub struct Section {
    pub(crate) section_type: SectionType,
    pub(crate) max: usize,
    pub(crate) flags: FlagsColumn,
    pub(crate) columns: BTreeMap<String, Column>,
}

macro_rules! add_x {
    ($name: ident, $variant: path, $type: ty) => {
        pub(crate) fn $name(&mut self, index: usize, k: &str, v: $type) -> Result<()> {
            // N.B. It would be nicer to use the entry API here, but in
            // this case, that would require allocating a new String for
            // every call to this method, even when the field is already
            // known. So, instead we get this:
            match self.columns.get_mut(k) {
                Some(column) => {
                    match column {
                        $variant(m) => {
                            match m.entry(index) {
                                // if this index is unused then just add there
                                btree_map::Entry::Vacant(inner_entry) => {
                                    inner_entry.insert(v);
                                    self.max = cmp::max(self.max, index); // possibly increase max index
                                    self.flags.set(index, k);
                                    Ok(())
                                },
                                // if this index IS used then return err
                                btree_map::Entry::Occupied(_) => {
                                    IndexAlreadyUsed{name: k,
                                                     index: index}.fail()
                                },
                            }
                        },
                        _ => {
                            ColumnTypeChange{name: k}.fail()
                        }
                    }
                },
                None => {
                    let mut m = BTreeMap::new();
                    m.insert(index, v);
                    self.columns.insert(k.into(), $variant(m));
                    self.max = cmp::max(self.max, index);
                    self.flags.set(index, k);
                    Ok(())
                }
            }
        }
    }
}

impl Section {
    pub(crate) fn new(section_type: SectionType) -> Self {
        Section{section_type: section_type,
                max: 0,
                flags: FlagsColumn::new(),
                columns: BTreeMap::new()}
    }

    add_x!(add_number, Column::Numbers, i64);
    add_x!(add_long_float, Column::LongFloat, f64);
    add_x!(add_short_float, Column::ShortFloat, f64);
    add_x!(add_base64, Column::Base64, Vec<u8>);
    add_x!(add_string, Column::String, String);
    add_x!(add_bool, Column::Bool, bool);
    add_x!(add_ids, Column::IDs, Vec<u64>);

    pub fn len(&self) -> usize {
        self.flags.len()
    }

    pub(crate) fn type_tag(&self) -> u8 {
        self.section_type.type_tag()
    }

    pub fn columns(&self) -> &BTreeMap<String, Column> {
        &self.columns
    }

    fn write_types_table<W: Write>(&self, out: &mut W) -> Result<usize> {
        let mut buf = Vec::new();

        // Write 1 byte - the number of entries in the types table
        write(&mut buf, &u8::try_from(self.columns.len()).context(NumberTruncation{})?.to_le_bytes()).context(WriteTypesTable{})?;

        for name in self.flags.fields() {
            if let Some(column) = self.columns.get(name) {
                // Write 1 byte - the Type Tag for this type
                write(&mut buf, &column.type_tag().to_le_bytes()).context(WriteTypesTable{})?;
                // Write 1 byte - the length of the name of this type
                write(&mut buf, &u8::try_from(name.len()).context(NumberTruncation{})?.to_le_bytes()).context(WriteTypesTable{})?;
                // Write name.len() bytes - the name of this type
                write(&mut buf, name.as_bytes()).context(WriteTypesTable{})?;
            } else {
                panic!("TODO")
            }
        }

        // Write 2 bytes - CRC
        let crc = crc::crc16::checksum_usb(&buf).to_le_bytes();
        write(&mut buf, &crc).context(WriteTypesTable{})?;

        // Write buf -> out
        let written = write(out, &buf).context(WriteTypesTable{})?;

        Ok(written)
    }

    fn write_data<W: Write>(&self, out: &mut W) -> Result<usize> {
        let mut buf = Vec::new();

        // Write the "Flags" column
        self.flags.write(&mut buf).context(WriteFlagsColumn)?;

        // Write all other columns
        for name in self.flags.fields() {
            if let Some(column) = self.columns.get(name) {
                match column {
                    Column::Numbers(m) => {
                        let mut last = 0;
                        for index in 0..=self.max {
                            let delta = match m.get(&index) {
                                Some(v) => {
                                    let value = *v;
                                    let delta = value - last;
                                    last = value;
                                    delta
                                }
                                None => 0
                            };

                            // Write the signed delta from the previous value
                            leb128::write::signed(&mut buf, delta).with_context(|| WriteDataColumn{name: name.clone()})?;
                        }
                    }
                    Column::LongFloat(m) => {
                        let mut last = 0;
                        for index in 0..=self.max {
                            let delta = match m.get(&index) {
                                Some(v) => {
                                    let value = (*v * 10000000.0) as i64;
                                    let delta = value - last;
                                    last = value;
                                    delta
                                }
                                None => 0
                            };

                            // Write the signed delta from the previous value
                            leb128::write::signed(&mut buf, delta).with_context(|| WriteDataColumn{name: name.clone()})?;
                        }
                    }
                    Column::ShortFloat(m) => {
                        let mut last = 0;
                        for index in 0..=self.max {
                            let delta = match m.get(&index) {
                                Some(v) => {
                                    let value = (*v * 1000.0) as i64;
                                    let delta = value - last;
                                    last = value;
                                    delta
                                }
                                None => 0
                            };

                            // Write the signed delta from the previous value
                            leb128::write::signed(&mut buf, delta).with_context(|| WriteDataColumn{name: name.clone()})?;
                        }
                    }
                    Column::Base64(m) => {
                        for index in 0..=self.max {
                            let empty = Vec::with_capacity(0);
                            let v = m.get(&index).unwrap_or(&empty);

                            // Write the length of the bytes
                            leb128::write::unsigned(&mut buf, u64::try_from(v.len()).context(NumberTruncation{})?).with_context(|| WriteDataColumn{name: name.clone()})?;
                            // Write the bytes themselves
                            write(&mut buf, &v).with_context(|| WriteDataColumn{name: name.clone()})?;
                        }
                    }
                    Column::String(m) => {
                        let empty = "".to_string();
                        for index in 0..=self.max {
                            let v = m.get(&index).unwrap_or(&empty);

                            // Write the length of the string
                            leb128::write::unsigned(&mut buf, u64::try_from(v.len()).context(NumberTruncation{})?).with_context(|| WriteDataColumn{name: name.clone()})?;
                            // Write the string itself
                            write(&mut buf, v.as_bytes()).with_context(|| WriteDataColumn{name: name.clone()})?;
                        }
                    }
                    Column::Bool(m) => {
                        for index in 0..=self.max {
                            let b = m.get(&index).unwrap_or(&false);
                            let v = *b as u8;

                            // write a 0 for false and a 1 for true
                            write(&mut buf, &v.to_le_bytes()).with_context(|| WriteDataColumn{name: name.clone()})?;
                        }
                    }
                    Column::IDs(m) => {
                        let empty = Vec::with_capacity(0);
                        for index in 0..=self.max {
                            let v = m.get(&index).unwrap_or(&empty);

                            // Write the length of the vec
                            leb128::write::unsigned(&mut buf, u64::try_from(v.len()).context(NumberTruncation{})?).with_context(|| WriteDataColumn{name: name.clone()})?;
                            // Write the ids themselves
                            for id in v {
                                leb128::write::unsigned(&mut buf, *id).with_context(|| WriteDataColumn{name: name.clone()})?;
                            }
                        }
                    }
                }
            } else {
                panic!("TODO")
            }
        }

        // Write 4 bytes - Data CRC
        let crc = crc::crc32::checksum_ieee(&buf).to_le_bytes();
        write(&mut buf, &crc).with_context(|| WriteDataColumn{name: "crc"})?;

        // Write buf -> out
        let written = write(out, &buf).with_context(|| WriteDataColumn{name: "full"})?;

        Ok(written)
    }

    fn write_header<W: Write>(&self, out: &mut W, section_size: u64) -> Result<usize> {
        let mut buf = Vec::new();

        // Write 1 byte - this section type
        write(&mut buf, &self.type_tag().to_le_bytes()).context(WriteHeader{})?;

        // Write 3 bytes - number of points in this section
        let len = self.len();
        if len < 2usize.pow(24) {
            write(&mut buf, &len.to_le_bytes()[..3]).context(WriteHeader{})?;
        } else {
            WriteDataColumnNumberOfPoints{}.fail()?;
        }

        // Write 8 bytes - total size of this section (including this header)
        write(&mut buf, &section_size.to_le_bytes()).context(WriteHeader{})?;

        // Write 2 bytes - CRC
        let crc = crc::crc16::checksum_usb(&buf).to_le_bytes();
        write(&mut buf, &crc).context(WriteHeader{})?;

        // Write buf -> out
        let written = write(out, &buf).context(WriteHeader{})?;

        Ok(written)
    }

    pub fn write<W: Write>(&self, out: &mut W) -> Result<usize> {
        let mut written = 0;

        let mut buf = Vec::new();

        if self.len() > 0 {
            written += self.write_types_table(&mut buf)?;
            written += self.write_data(&mut buf)?;
        }

        let header_size: u64 = 12;
        let data_size = u64::try_from(buf.len()).context(NumberTruncation{})?;
        written += self.write_header(out, header_size + data_size)?;
        written += write(out, &buf).context(WriteBytes{})?;

        Ok(written)
    }
}

pub struct Point<'a> {
    section: &'a Section,
    index: usize,
}

impl<'a> Point<'a> {
    fn new(section: &'a Section, index: usize) -> Self {
        Point{section,
              index}
    }
}

impl<'a> Serialize for Point<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(None)?;
        for field in self.section.flags.fields.keys() {
            if let Some(column) = self.section.columns.get(field) {
                let maybe_data = match column {
                    Column::Numbers(m) => m.get(&self.index).map(|v| DataField::Number(*v)),
                    Column::LongFloat(m) => m.get(&self.index).map(|v| DataField::LongFloat(*v)),
                    Column::ShortFloat(m) => m.get(&self.index).map(|v| DataField::ShortFloat(*v)),
                    Column::Base64(m) => m.get(&self.index).map(|v| DataField::Base64(base64::encode(v))),
                    Column::String(m) => m.get(&self.index).map(|v| DataField::String(v.to_string())),
                    Column::Bool(m) => m.get(&self.index).map(|v| DataField::Bool(*v)),
                    Column::IDs(m) => m.get(&self.index).map(|v| DataField::IDs(v.to_vec())),
                };

                if let Some(data) = maybe_data {
                    map.serialize_entry(field, &data)?;
                }
            }
        }
        map.end()
    }
}

impl Serialize for Section {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let len = self.len();
        let mut seq = serializer.serialize_seq(Some(len))?;
        for i in 0..len {
            seq.serialize_element(&Point::new(&self, i))?;
        }
        seq.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max() {
        let mut s = Section::new(SectionType::TrackPoints);

        for i in 0..=20 {
            assert!(s.add_number(i, "foo", 0).is_ok());
        }
        assert_eq!(s.max, 20);

        for i in 0..=20 {
            assert!(s.add_base64(i, "bar", vec![1,2,3]).is_ok());
        }
        assert_eq!(s.max, 20);

        assert!(s.add_base64(302, "bar", vec![1,2,3]).is_ok());
        assert_eq!(s.max, 302);
    }

    #[test]
    fn test_cant_overwrite() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_number(0, "foo", 0).is_ok());
        assert!(s.add_number(0, "foo", 0).is_err());
    }

    #[test]
    fn test_cant_change_column_type() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_number(0, "foo", 0).is_ok());
        assert!(s.add_base64(1, "foo", vec![1]).is_err());
        assert!(s.add_short_float(2, "foo", 0.0).is_err());
        assert!(s.add_number(3, "foo", 0).is_ok());
    }

    #[test]
    fn test_len() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert_eq!(s.len(), 0);

        assert!(s.add_number(0, "foo", 0).is_ok());
        assert_eq!(s.len(), 1);

        assert!(s.add_number(500, "foo", 0).is_ok());
        assert_eq!(s.len(), 501);
    }

    #[test]
    fn test_write_types_table() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_number(1, "foo", 5).is_ok());
        assert!(s.add_base64(1, "bazar", vec![0,1,2,3,4]).is_ok());

        let mut buf = vec![];
        let written = s.write_types_table(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x02, // 2 entries in the table
                         0x00, // column 1 type is Column::Numbers
                         0x03, // column 1 name len is 3
                         b'f', b'o', b'o',
                         0x03, // column 2 type is Column::Base64
                         0x05, // column 2 name len is 5
                         b'b', b'a', b'z', b'a', b'r',
                         0xE5, // CRC
                         0x24];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_write_large_types_table() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_number(0, "a", 5).is_ok());
        assert!(s.add_number(1, "a", 7).is_ok());
        assert!(s.add_number(0, "b", 5).is_ok());
        assert!(s.add_base64(0, "c", vec![0]).is_ok());
        assert!(s.add_base64(0, "d", vec![]).is_ok());
        assert!(s.add_base64(0, "e_long_name", vec![0]).is_ok());
        assert!(s.add_short_float(0, "f", 0.1).is_ok());
        assert!(s.add_long_float(0, "g", 0.2).is_ok());
        assert!(s.add_number(500, "h", 10).is_ok());
        assert!(s.add_number(500, "i", 11).is_ok());
        assert!(s.add_number(500, "j10", 12).is_ok());

        let mut buf = vec![];
        let written = s.write_types_table(&mut buf);
        assert!(written.is_ok());
        let expected = vec![0x0A, // 10 entries in the table
                            0x00, // column 1 type is Column::Numbers
                            0x01, // column 1 name len is 1
                            b'a', // column 1 name

                            0x00, // column 2 type is Column::Numbers
                            0x01, // column 2 name len is 1
                            b'b', // column 2 name

                            0x03, // column 3 type is Column::Base64
                            0x01, // column 3 name len is 1
                            b'c', // column 3 name

                            0x03, // column 4 type is Column::Base64
                            0x01, // column 4 name len is 1
                            b'd', // column 4 name

                            0x03, // column 5 type is Column::Base64
                            0x0B, // column 5 name len is 11
                            b'e', b'_', b'l', b'o', b'n', b'g', b'_', b'n', b'a', b'm', b'e',

                            0x02, // column 6 type is Column::ShortFloat
                            0x01, // column 6 name len is 1
                            b'f', // column 6 name

                            0x01, // column 7 type is Column::LongFloat
                            0x01, // column 7 name len is 1
                            b'g', // column 7 name

                            0x00, // column 8 type is Column::Numbers
                            0x01, // column 8 name len is 1
                            b'h', // column 8 name

                            0x00, // column 9 type is Column::Numbers
                            0x01, // column 9 name len is 1
                            b'i', // column 9 name

                            0x00, // column 10 type is Column::Numbers
                            0x03, // column 10 name len is 1
                            b'j', b'1', b'0', // column 10 name

                            0x87, // CRC
                            0x12];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_type_with_multibyte_character() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_number(1, "I♥NY", 5).is_ok());

        let mut buf = vec![];
        let written = s.write_types_table(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x01, // 1 entry in the table
                         0x00, // column 1 type is Column::Numbers
                         0x06, // column 1 name len is 6
                         0x49, // "I"
                         0xE2, // heart
                         0x99,
                         0xA5,
                         0x4E, // "N"
                         0x59, // "Y"
                         0xA3, // CRC
                         0xF5];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_write_header_empty() {
        let s = Section::new(SectionType::TrackPoints);

        let mut buf = vec![];
        let written = s.write_header(&mut buf, 10);
        assert!(written.is_ok());
        let expected = &[0x00, // SectionType::TrackPoints
                         0x00, // zero points
                         0x00,
                         0x00,
                         0x0A, // total section size
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x1B, // CRC
                         0x82];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_write_header_with_points() {
        let mut s = Section::new(SectionType::CoursePoints);
        assert!(s.add_number(100, "a", 20).is_ok());

        let mut buf = vec![];
        let written = s.write_header(&mut buf, 10);
        assert!(written.is_ok());
        let expected = &[0x01, // SectionType::CoursePoints
                         0x65, // 101 points
                         0x00,
                         0x00,
                         0x0A, // total section size
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x09, // CRC
                         0x8C];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_write_data_single_column() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_number(0, "a", 20).is_ok());
        assert!(s.add_number(1, "a", 25).is_ok());
        assert!(s.add_number(2, "a", 28).is_ok());
        assert!(s.add_number(3, "a", 10).is_ok());
        assert!(s.add_number(6, "a", 12).is_ok());

        let mut buf = vec![];
        let written = s.write_data(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x01, // flags column
                         0x01,
                         0x01,
                         0x01,
                         0x00,
                         0x00,
                         0x01,
                         20, // initial value
                         5, // delta from prev
                         3, // delta from prev
                         0x6E, // delta from prev (in hex as u8)
                         0, // no value at index 4
                         0, // no value at index 5
                         2, // delta from last value
                         0xA7, // 4-byte crc
                         0x60,
                         0xA8,
                         0xB6];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_write_data_two_columns() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_number(0, "a", 20).is_ok());
        assert!(s.add_number(0, "b", 42).is_ok());
        assert!(s.add_number(1, "a", 25).is_ok());
        assert!(s.add_number(1, "b", 52).is_ok());

        let mut buf = vec![];
        let written = s.write_data(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x03, // flags column
                         0x03,
                         20, // a initial value
                         5, // delta from prev
                         42, // b initial value
                         10, // delta from prev
                         0xC8, // 4-byte crc
                         0x8E,
                         0xF8,
                         0x26];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_write_data_zeros_for_missing_values() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_number(10, "a", 20).is_ok());

        let mut buf = vec![];
        let written = s.write_data(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x00, // flags column
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x01,
                         0x00, // now the data column
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         0x00,
                         20, // a = 20
                         0xF2, // 4-byte crc
                         0x29,
                         0x56,
                         0x29];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_write_data_base64() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_base64(0, "a", "foo".as_bytes().to_vec()).is_ok());
        assert!(s.add_base64(1, "a", "bazar".as_bytes().to_vec()).is_ok());

        let mut buf = vec![];
        let written = s.write_data(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x01, // flags column
                         0x01,
                         0x03, // now the data column - len of first byte vec
                         b'f',
                         b'o',
                         b'o',
                         0x05, // len of second byte vec
                         b'b',
                         b'a',
                         b'z',
                         b'a',
                         b'r',
                         0x7E, // 4-byte crc
                         0x1A,
                         0xAB,
                         0x3A];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }

    #[test]
    fn test_write_data_numbers_and_base64() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_base64(0, "a", "foo".as_bytes().to_vec()).is_ok());
        assert!(s.add_base64(1, "a", "bazar".as_bytes().to_vec()).is_ok());
        assert!(s.add_number(0, "b", 42).is_ok());
        assert!(s.add_number(1, "b", 45).is_ok());
        assert!(s.add_number(2, "b", 50).is_ok());

        let mut buf = vec![];
        let written = s.write_data(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x03, // flags column
                         0x03,
                         0x02,
                         0x03, // now the data column - len of first byte vec
                         b'f',
                         b'o',
                         b'o',
                         0x05, // len of second byte vec
                         b'b',
                         b'a',
                         b'z',
                         b'a',
                         b'r',
                         0x00, // last row of "a" column is empty
                         42,
                         3,
                         5,
                         0x89, // 4-byte crc
                         0x58,
                         0x64,
                         0xBE];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
    }
}
