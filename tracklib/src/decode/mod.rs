use std::iter::FromIterator;
use std::time::{UNIX_EPOCH, Duration};
use std::collections::{BTreeMap};
use nom::*;
use ::crc::crc32::{checksum_ieee};
use ::crc::crc16::{checksum_usb};

mod varint;
mod crc;

use varint::{take_signed_leb128, take_unsigned_leb128};
use crate::flagscolumn::{FlagsColumn};
use crate::rwtfile::{RWTFMAGIC, RWTFTRAILER, RWTFHeader, RWTFile};
use crate::metadata::{RWTFMetadata, TrackType};
use crate::section::{Column, Section, SectionType};
use crate::decode::crc::{CRC};

trait Parsable {
    type Return;

    fn parse(i: &[u8]) -> IResult<&[u8], Self::Return>;
}

//////////////////////////////
//          Header          //
//////////////////////////////
#[derive(Debug)]
struct ParsedHeader {
    metadata_table_offset: u16,
    data_offset: u16,
    crc: CRC<u16>
}

impl Parsable for RWTFHeader {
    type Return = (Self, ParsedHeader);

    fn parse(i: &[u8]) -> IResult<&[u8], Self::Return> {
        do_parse!(i,
                  tag!(RWTFMAGIC) >>
                  file_version: le_u8 >>
                  le_u24 >>
                  creator_version: le_u8 >>
                  le_u24 >>
                  metadata_table_offset: le_u16 >>
                  data_offset: le_u16 >>
                  le_u16 >>
                  crc: le_u16 >>
                  ((RWTFHeader{file_version,
                               creator_version},
                    ParsedHeader{metadata_table_offset,
                                 data_offset,
                                 crc: CRC::new(crc, checksum_usb(&i[0..22]))})))
    }
}

//////////////////////////////
//         Metadata         //
//////////////////////////////
#[derive(Debug)]
enum RWTFMetadataEntry {
    TrackType(TrackType),
    CreatedAt(u64),
    Unknown,
}

fn parse_metadata_table_entry_data(i: &[u8], tag: u8) -> IResult<&[u8], RWTFMetadataEntry> {
    match tag {
        0x00 => {
            match do_parse!(i,
                            _size: le_u16 >>
                            track_type_tag: le_u8 >>
                            id: le_u32 >>
                            (TrackType::from_tag(track_type_tag, id))) {
                Ok((rest, tt)) => match tt {
                    Some(tt) => Ok((rest, RWTFMetadataEntry::TrackType(tt))),
                    None => Err(Err::Error(Context::Code(i, ErrorKind::Custom(0)))),
                },
                Err(_) => Err(Err::Error(Context::Code(i, ErrorKind::Custom(0)))),
            }
        }
        0x01 => {
            do_parse!(i,
                      _size: le_u16 >>
                      timestamp: le_u64 >>
                      (RWTFMetadataEntry::CreatedAt(timestamp)))
        }
        _ => {
            let (rest, size) = le_u16(i)?;
            let (rest, _data) = take!(rest, size)?;
            // todo: do something with data
            Ok((rest, RWTFMetadataEntry::Unknown))
        }
    }
}

fn parse_metadata_table_entry(i: &[u8]) -> IResult<&[u8], RWTFMetadataEntry> {
    do_parse!(i,
              tag: le_u8 >>
              entry: apply!(parse_metadata_table_entry_data, tag) >>
              (entry))
}

impl Parsable for RWTFMetadata {
    type Return = (Self, CRC<u16>);

    fn parse(i: &[u8]) -> IResult<&[u8], Self::Return> {
        let (rest, entries) = do_parse!(i,
                                        count: le_u8 >>
                                        entries: many_m_n!(count as usize, count as usize, parse_metadata_table_entry) >>
                                        (entries))?;

        let diff = i.offset(rest);
        let (rest, crc) = le_u16(rest)?;

        let mut created_at = None;
        let mut track_type = None;

        for entry in entries {
            match entry {
                RWTFMetadataEntry::TrackType(tt) => {
                    track_type = Some(tt)
                },
                RWTFMetadataEntry::CreatedAt(time) => {
                    created_at = UNIX_EPOCH.checked_add(Duration::new(time, 0));
                },
                RWTFMetadataEntry::Unknown => {},
            }
        }

        Ok((rest, (RWTFMetadata::new(created_at, track_type),
                   CRC::new(crc, checksum_usb(&i[..diff])))))
    }
}

//////////////////////////////
//       Flags Column       //
//////////////////////////////
impl FlagsColumn {
    fn parse_flags_column<'a, 'b>(i: &'a [u8], types_table: &'b TypesTable, points: u32) -> IResult<&'a [u8], FlagsColumn> {
        let width = (types_table.entries.len() + 7) / 8;

        let fields = BTreeMap::from_iter(types_table.entries.iter().enumerate().map(|(i, entry)| (entry.name.clone(), i)));

        let mut data = BTreeMap::new();
        let mut remainder = i;
        for i in 0..points {
            let (rest, bitfield_bytes) = take!(remainder, width)?;
            remainder = rest;

            let mut bitfield_array = [0; 8];
            for i in 0..8 {
                bitfield_array[i] = *bitfield_bytes.get(i).unwrap_or(&0);
            }

            let bitfield_integer = u64::from_le_bytes(bitfield_array);

            if bitfield_integer > 0 {
                data.insert(i as usize, bitfield_integer);
            }
        }

        Ok((remainder, FlagsColumn{fields: fields,
                                   data: data,
                                   max: (points - 1) as usize}))
    }
}

//////////////////////////////
//         Section          //
//////////////////////////////
fn parse_section_type(i: &[u8]) -> IResult<&[u8], SectionType> {
    let (rest, tag) = le_u8(i)?;
    match SectionType::from_tag(tag) {
        Some(st) => Ok((rest, st)),
        None => Err(Err::Error(Context::Code(i, ErrorKind::Custom(0)))),
    }
}

#[derive(Debug)]
enum ColumnType {
    Numbers,
    LongFloat,
    ShortFloat,
    Base64,
    String,
    Bool,
    IDs,
}

impl ColumnType {
    fn from_tag(tag: u8) -> Option<Self> {
        match tag {
            0x00 => Some(ColumnType::Numbers),
            0x01 => Some(ColumnType::LongFloat),
            0x02 => Some(ColumnType::ShortFloat),
            0x03 => Some(ColumnType::Base64),
            0x04 => Some(ColumnType::String),
            0x05 => Some(ColumnType::Bool),
            0x06 => Some(ColumnType::IDs),
            _ => None
        }
    }
}

#[derive(Debug)]
pub struct SectionHeader {
    section_type: SectionType,
    points: u32,
    size: u64,
    crc: CRC<u16>,
}

fn parse_section_header(i: &[u8]) -> IResult<&[u8], SectionHeader> {
    let (rest, section_type) = parse_section_type(i)?;
    let (rest, points) = le_u24(rest)?;
    let (rest, size) = le_u64(rest)?;

    let diff = i.offset(rest);
    let (rest, crc) = le_u16(rest)?;

    Ok((rest, SectionHeader{section_type,
                            points,
                            size,
                            crc: CRC::new(crc, checksum_usb(&i[..diff]))}))
}

#[derive(Debug)]
struct TypesTableEntry {
    column_type: ColumnType,
    name: String,
}

fn parse_column_type(i: &[u8]) -> IResult<&[u8], ColumnType> {
    let (rest, tag) = le_u8(i)?;
    match ColumnType::from_tag(tag) {
        Some(c) => Ok((rest, c)),
        None => Err(Err::Error(Context::Code(i, ErrorKind::Custom(0)))),
    }
}

fn parse_types_table_entry(i: &[u8]) -> IResult<&[u8], TypesTableEntry> {
    do_parse!(i,
              column_type: parse_column_type >>
              name_len: le_u8 >>
              name: take!(name_len) >>
              (TypesTableEntry{column_type,
                               name: String::from_utf8_lossy(name).into_owned()}))
}

#[derive(Debug)]
pub struct TypesTable {
    entries: Vec<TypesTableEntry>,
    crc: CRC<u16>,
}

fn parse_types_table(i: &[u8]) -> IResult<&[u8], TypesTable> {
    let (rest, entries) = do_parse!(i,
                                    count: le_u8 >>
                                    entries: many_m_n!(count as usize, count as usize, parse_types_table_entry) >>
                                    (entries))?;
    let diff = i.offset(rest);
    let (rest, crc) = le_u16(rest)?;

    Ok((rest, TypesTable{entries,
                         crc: CRC::new(crc, checksum_usb(&i[..diff]))}))
}

fn parse_number_row<'a>(i: &'a [u8]) -> IResult<&'a [u8], i64> {
    take_signed_leb128(i)
}

fn parse_bytes_row<'a>(i: &'a [u8]) -> IResult<&'a [u8], &'a [u8]> {
    do_parse!(i,
              len: take_unsigned_leb128 >>
              bytes: take!(len) >>
              (bytes))

}

fn parse_bool_row<'a>(i: &'a [u8]) -> IResult<&'a [u8], bool> {
    do_parse!(i,
              b: le_u8 >>
              ({
                  if b == 0 {
                      false
                  } else {
                      true
                  }
              }))
}

fn parse_ids_row<'a>(i: &'a [u8]) -> IResult<&'a [u8], Vec<u64>> {
    do_parse!(i,
              count: take_unsigned_leb128 >>
              entries: many_m_n!(count as usize, count as usize, take_unsigned_leb128) >>
              (entries))
}

fn parse_column<'a>(i: &'a [u8], column: &TypesTableEntry, flags: &FlagsColumn) -> IResult<&'a [u8], Column> {
    match column.column_type {
        ColumnType::Numbers => {
            let mut m = BTreeMap::new();
            let mut remainder = i;
            let mut last = 0;
            for index in 0..flags.len() {
                if flags.is_present(index, &column.name) {
                    let (rest, delta) = parse_number_row(remainder)?;
                    remainder = rest;
                    let v = last + delta;
                    last = v;
                    m.insert(index, v);
                } else {
                    // skip forward one byte
                    remainder = &remainder[1..];
                }
            }

            Ok((remainder, Column::Numbers(m)))
        }
        ColumnType::LongFloat => {
            let mut m = BTreeMap::new();
            let mut remainder = i;
            let mut last = 0;
            for index in 0..flags.len() {
                if flags.is_present(index, &column.name) {
                    let (rest, delta) = parse_number_row(remainder)?;
                    remainder = rest;
                    let v = last + delta;
                    last = v;
                    m.insert(index, v as f64 / 10000000.0);
                } else {
                    // skip forward one byte
                    remainder = &remainder[1..];
                }
            }

            Ok((remainder, Column::LongFloat(m)))
        }
        ColumnType::ShortFloat => {
            let mut m = BTreeMap::new();
            let mut remainder = i;
            let mut last = 0;
            for index in 0..flags.len() {
                if flags.is_present(index, &column.name) {
                    let (rest, delta) = parse_number_row(remainder)?;
                    remainder = rest;
                    let v = last + delta;
                    last = v;
                    m.insert(index, v as f64 / 1000.0);
                } else {
                    // skip forward one byte
                    remainder = &remainder[1..];
                }
            }

            Ok((remainder, Column::ShortFloat(m)))
        }
        ColumnType::Base64 => {
            let mut m = BTreeMap::new();
            let mut remainder = i;
            for index in 0..flags.len() {
                if flags.is_present(index, &column.name) {
                    let (rest, bytes) = parse_bytes_row(remainder)?;
                    remainder = rest;
                    m.insert(index, bytes.to_vec());
                } else {
                    // skip forward one byte
                    remainder = &remainder[1..];
                }
            }

            Ok((remainder, Column::Base64(m)))
        }
        ColumnType::String => {
            let mut m = BTreeMap::new();
            let mut remainder = i;
            for index in 0..flags.len() {
                if flags.is_present(index, &column.name) {
                    let (rest, bytes) = parse_bytes_row(remainder)?;
                    remainder = rest;
                    m.insert(index, String::from_utf8_lossy(bytes).into_owned());
                } else {
                    // skip forward one byte
                    remainder = &remainder[1..];
                }
            }

            Ok((remainder, Column::String(m)))
        }
        ColumnType::Bool => {
            let mut m = BTreeMap::new();
            let mut remainder = i;
            for index in 0..flags.len() {
                if flags.is_present(index, &column.name) {
                    let (rest, b) = parse_bool_row(remainder)?;
                    remainder = rest;
                    m.insert(index, b);
                } else {
                    // skip forward one byte
                    remainder = &remainder[1..];
                }
            }

            Ok((remainder, Column::Bool(m)))
        }
        ColumnType::IDs => {
            let mut m = BTreeMap::new();
            let mut remainder = i;
            for index in 0..flags.len() {
                if flags.is_present(index, &column.name) {
                    let (rest, b) = parse_ids_row(remainder)?;
                    remainder = rest;
                    m.insert(index, b);
                } else {
                    // skip forward one byte
                    remainder = &remainder[1..];
                }
            }

            Ok((remainder, Column::IDs(m)))
        }
    }
}

impl Parsable for Section {
    type Return = Option<Self>;

    fn parse(i: &[u8]) -> IResult<&[u8], Self::Return> {
        let (rest, section_header) = alt!(i,
                                          tag!(&RWTFTRAILER) => { |_| None } |
                                          parse_section_header => {|header| Some(header)})?;

        if let Some(header) = section_header {
            let (rest, types_table) = parse_types_table(rest)?;

            let data_column_start = i.offset(rest);
            let (mut rest, flags) = FlagsColumn::parse_flags_column(&rest, &types_table, header.points)?;

            let mut m = BTreeMap::new();
            for column in types_table.entries.iter() {
                let (new_rest, data) = parse_column(&rest, &column, &flags)?;
                rest = new_rest;
                m.insert(column.name.clone(), data);
            }

            let data_column_end = i.offset(rest);
            let (rest, crc) = le_u32(&rest)?;
            let _actual_crc = CRC::new(crc, checksum_ieee(&i[data_column_start..data_column_end])); // TODO: use this

            Ok((rest, Some(Section{section_type: header.section_type,
                                   max: flags.max(),
                                   flags: flags,
                                   columns: m})))
        } else {
            Ok((rest, None))
        }
    }
}

//////////////////////////////
//         RWTFile          //
//////////////////////////////
impl Parsable for RWTFile {
    type Return = Self;

    fn parse(i: &[u8]) -> IResult<&[u8], Self::Return> {
        let (_rest, (header, header_details)) = RWTFHeader::parse(i)?;
        let (_rest, (metadata, _metadata_crc)) = RWTFMetadata::parse(&i[header_details.metadata_table_offset as usize..])?;
        // TODO: use metadata_crc

        let mut remainder = &i[header_details.data_offset as usize..];

        let mut track_points = None;
        let mut course_points = None;

        loop {
            let (rest, section) = Section::parse(remainder)?;
            remainder = rest;

            if let Some(section) = section {
                match section.section_type {
                    SectionType::TrackPoints => track_points = Some(section),
                    SectionType::CoursePoints => course_points = Some(section),
                    SectionType::Continuation => panic!("SectionType::Continuation unsupported"),
                }
            } else {
                // parsing section returned None
                break;
            }
        }

        Ok((remainder, RWTFile{header,
                               metadata,
                               track_points: track_points.unwrap_or(Section::new(SectionType::TrackPoints)),
                               course_points: course_points.unwrap_or(Section::new(SectionType::CoursePoints))}))
    }
}

pub fn parse_rwtf(i: &[u8]) -> IResult<&[u8], RWTFile> {
    RWTFile::parse(i)
}

#[cfg(test)]
mod tests {

}
