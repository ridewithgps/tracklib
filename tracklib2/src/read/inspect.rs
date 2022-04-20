use super::data_table::{parse_data_table, DataTableEntry};
use super::header::{parse_header, Header};
use super::metadata::parse_metadata;
use super::section::Section;
use crate::types::{FieldValue, MetadataEntry, TrackType};
use chrono::offset::TimeZone;
use chrono::Utc;
use humansize::{file_size_opts, FileSize};
use nom::Offset;
use term_table::row::Row;
use term_table::table_cell::{Alignment, TableCell};
use term_table::Table;

fn bold(s: &str) -> String {
    format!("\x1b[1m{s}\x1b[0m")
}

fn italic(s: &str) -> String {
    format!("\x1b[3m{s}\x1b[0m")
}

fn strikethrough(s: &str) -> String {
    format!("\x1b[9m{s}\x1b[0m")
}

fn format_header(header: &Header) -> String {
    let mut table = Table::new();

    table.add_row(Row::new(vec![TableCell::new_with_alignment(
        bold("Header"),
        2,
        Alignment::Center,
    )]));
    table.add_row(Row::new(vec![
        TableCell::new("File Version"),
        TableCell::new_with_alignment(
            format!("{:#04X}", header.file_version()),
            1,
            Alignment::Right,
        ),
    ]));
    table.add_row(Row::new(vec![
        TableCell::new("Creator Version"),
        TableCell::new_with_alignment(
            format!("{:#04X}", header.creator_version()),
            1,
            Alignment::Right,
        ),
    ]));
    table.add_row(Row::new(vec![
        TableCell::new("Metadata Offset"),
        TableCell::new_with_alignment(
            format!("{:#04X}", header.metadata_offset()),
            1,
            Alignment::Right,
        ),
    ]));
    table.add_row(Row::new(vec![
        TableCell::new("Data Offset"),
        TableCell::new_with_alignment(
            format!("{:#04X}", header.data_offset()),
            1,
            Alignment::Right,
        ),
    ]));

    table.render()
}

fn try_format_metadata(input: &[u8]) -> String {
    let mut table = Table::new();

    table.add_row(Row::new(vec![TableCell::new_with_alignment(
        bold("Metadata"),
        2,
        Alignment::Center,
    )]));

    match parse_metadata(input) {
        Ok((_, metadata_entries)) => {
            for entry in metadata_entries {
                let (entry_type, s) = match entry {
                    MetadataEntry::TrackType(track_type) => {
                        let (type_name, id) = match track_type {
                            TrackType::Trip(id) => ("Trip", id),
                            TrackType::Route(id) => ("Route", id),
                            TrackType::Segment(id) => ("Segment", id),
                        };
                        ("Track Type", format!("{type_name} {id}"))
                    }
                    MetadataEntry::CreatedAt(created_at) => {
                        let s = match i64::try_from(created_at) {
                            Ok(secs) => {
                                format!(
                                    "{} ({})",
                                    Utc.timestamp(secs, 0).to_rfc3339(),
                                    italic(&secs.to_string())
                                )
                            }
                            Err(_) => {
                                format!("Invalid Timestamp ({})", italic(&created_at.to_string()))
                            }
                        };
                        ("Created At", s)
                    }
                };
                table.add_row(Row::new(vec![
                    TableCell::new_with_alignment(entry_type, 1, Alignment::Left),
                    TableCell::new_with_alignment(s, 1, Alignment::Right),
                ]));
            }
        }
        Err(e) => {
            table.add_row(Row::new(vec![TableCell::new_with_alignment(
                format!("{e:?}"),
                1,
                Alignment::Left,
            )]));
        }
    }

    table.render()
}

fn try_format_data_table(
    input: &[u8],
    data_table_offset: usize,
) -> (Option<(&[u8], Vec<DataTableEntry>)>, String) {
    match parse_data_table(&input[data_table_offset..]) {
        Ok((data_start, data_table)) => {
            let data_start_offset = input.offset(data_start);
            let mut out = String::new();

            for (i, data_table_entry) in data_table.iter().enumerate() {
                const CRC_BYTES: usize = 4;
                let presence_column_bytes_required =
                    (data_table_entry.schema_entries().len() + 7) / 8;
                let presence_column_size =
                    presence_column_bytes_required * data_table_entry.rows() + CRC_BYTES;

                let mut table = Table::new();
                table.add_row(Row::new(vec![TableCell::new_with_alignment(
                    bold(&format!("Data Section {i}")),
                    3,
                    Alignment::Center,
                )]));
                table.add_row(Row::new(vec![
                    TableCell::new("Encoding"),
                    TableCell::new_with_alignment(
                        format!("{:?}", data_table_entry.section_encoding()),
                        2,
                        Alignment::Right,
                    ),
                ]));

                table.add_row(Row::new(vec![
                    TableCell::new("Rows"),
                    TableCell::new_with_alignment(
                        format!("{}", data_table_entry.rows()),
                        2,
                        Alignment::Right,
                    ),
                ]));

                table.add_row(Row::new(vec![
                    TableCell::new("Size"),
                    TableCell::new_with_alignment(
                        format!(
                            "{} ({})",
                            data_table_entry
                                .size()
                                .file_size(file_size_opts::BINARY)
                                .unwrap(),
                            italic(&data_table_entry.size().to_string())
                        ),
                        2,
                        Alignment::Right,
                    ),
                ]));

                table.add_row(Row::new(vec![
                    TableCell::new(format!("Offset ({})", italic("relative"))),
                    TableCell::new_with_alignment(
                        format!("{:#04X}", data_table_entry.offset()),
                        2,
                        Alignment::Right,
                    ),
                ]));

                table.add_row(Row::new(vec![
                    TableCell::new(format!("Offset ({})", italic("absolute"))),
                    TableCell::new_with_alignment(
                        format!("{:#04X}", data_table_entry.offset() + data_start_offset),
                        2,
                        Alignment::Right,
                    ),
                ]));

                for schema_entry in data_table_entry.schema_entries() {
                    table.add_row(Row::new(vec![
                        TableCell::new_with_alignment("Column", 1, Alignment::Left),
                        TableCell::new_with_alignment(
                            bold(schema_entry.field_definition().name()),
                            2,
                            Alignment::Center,
                        ),
                    ]));

                    table.add_row(Row::new(vec![
                        TableCell::new(""),
                        TableCell::new_with_alignment("Type", 1, Alignment::Left),
                        TableCell::new_with_alignment(
                            format!("{:?}", schema_entry.field_definition().data_type()),
                            1,
                            Alignment::Right,
                        ),
                    ]));

                    table.add_row(Row::new(vec![
                        TableCell::new(""),
                        TableCell::new_with_alignment("Size", 1, Alignment::Left),
                        TableCell::new_with_alignment(
                            format!(
                                "{} ({})",
                                schema_entry
                                    .size()
                                    .file_size(file_size_opts::BINARY)
                                    .unwrap(),
                                italic(&schema_entry.size().to_string())
                            ),
                            1,
                            Alignment::Right,
                        ),
                    ]));

                    table.add_row(Row::new(vec![
                        TableCell::new(""),
                        TableCell::new_with_alignment(
                            format!("Offset ({})", italic("relative")),
                            1,
                            Alignment::Right,
                        ),
                        TableCell::new_with_alignment(
                            format!("{:#04X}", schema_entry.offset()),
                            1,
                            Alignment::Right,
                        ),
                    ]));

                    table.add_row(Row::new(vec![
                        TableCell::new(""),
                        TableCell::new_with_alignment(
                            format!("Offset ({})", italic("absolute")),
                            1,
                            Alignment::Right,
                        ),
                        TableCell::new_with_alignment(
                            format!(
                                "{:#04X}",
                                schema_entry.offset()
                                    + presence_column_size
                                    + data_table_entry.offset()
                                    + data_start_offset
                            ),
                            1,
                            Alignment::Right,
                        ),
                    ]));
                }

                out.push_str(&table.render());
                out.push_str("\n\n");
            }

            (Some((data_start, data_table)), out)
        }
        Err(e) => {
            let mut table = Table::new();
            table.add_row(Row::new(vec![TableCell::new_with_alignment(
                bold("Data Table"),
                1,
                Alignment::Center,
            )]));
            table.add_row(Row::new(vec![TableCell::new_with_alignment(
                format!("{e:?}"),
                1,
                Alignment::Left,
            )]));

            (None, table.render())
        }
    }
}

fn format_val(value: Option<FieldValue>) -> String {
    if let Some(val) = value {
        match val {
            FieldValue::I64(v) => format!("{v}"),
            FieldValue::F64(v) => format!("{v}"),
            FieldValue::U64(v) => format!("{v}"),
            FieldValue::Bool(v) => format!("{v}"),
            FieldValue::String(v) => v,
            FieldValue::BoolArray(v) => format!("{v:?}"),
            FieldValue::U64Array(v) => format!("{v:?}"),
            FieldValue::ByteArray(v) => format!("{v:#04X?}"),
        }
    } else {
        strikethrough("None")
    }
}

fn try_format_section(data_start: &[u8], entry_num: usize, entry: &DataTableEntry) -> String {
    let mut table = Table::new();

    let data = &data_start[entry.offset()..];
    let section = Section::new(data, entry);

    match section.reader() {
        Ok(mut section_reader) => {
            table.add_row(Row::new(vec![TableCell::new_with_alignment(
                bold(&format!("Data {entry_num}")),
                entry.schema_entries().len() + 1,
                Alignment::Center,
            )]));

            table.add_row(Row::new(
                [TableCell::new_with_alignment("#", 1, Alignment::Center)]
                    .into_iter()
                    .chain(entry.schema_entries().iter().map(|schema_entry| {
                        TableCell::new_with_alignment(
                            schema_entry.field_definition().name(),
                            1,
                            Alignment::Center,
                        )
                    }))
                    .collect::<Vec<_>>(),
            ));

            let mut i = 0;
            while let Some(columniter) = section_reader.open_column_iter() {
                table.add_row(Row::new(
                    [format!("{i}")]
                        .into_iter()
                        .chain(columniter.map(|row_result| {
                            row_result
                                .map(|(_, val)| format_val(val))
                                .unwrap_or_else(|e| format!("{e:?}"))
                        }))
                        .collect::<Vec<_>>(),
                ));
                i += 1;
            }
        }
        Err(e) => {
            table.add_row(Row::new(vec![TableCell::new_with_alignment(
                format!("{e:?}"),
                1,
                Alignment::Left,
            )]));
        }
    }
    table.render()
}

pub fn inspect(input: &[u8]) -> Result<String, String> {
    let mut out = String::new();

    // Header
    let (_, header) = parse_header(input).map_err(|e| format!("Error Parsing Header: {:?}", e))?;
    out.push_str(&format_header(&header));
    out.push_str("\n\n");

    // Metadata
    out.push_str(&try_format_metadata(
        &input[usize::from(header.metadata_offset())..],
    ));
    out.push_str("\n\n");

    // Data Table
    let (maybe_data_table, data_table_out) =
        try_format_data_table(input, usize::from(header.data_offset()));
    out.push_str(&data_table_out);

    // Data
    if let Some((data_start, data_table_entries)) = maybe_data_table {
        for (i, data_table_entry) in data_table_entries.iter().enumerate() {
            out.push_str(&try_format_section(data_start, i, data_table_entry));
            out.push_str("\n\n");
        }
    }

    Ok(out)
}
