use crate::error::{Result, TracklibError};
use crate::read::decoders::*;
use crate::read::presence_column::parse_presence_column;
use crate::read::schema::SchemaEntry;
use crate::schema::*;
use crate::types::FieldValue;

#[cfg_attr(test, derive(Debug))]
enum ColumnDecoder<'a> {
    I64 {
        field_definition: &'a FieldDefinition,
        decoder: I64Decoder<'a>,
    },
    U64 {
        field_definition: &'a FieldDefinition,
        decoder: U64Decoder<'a>,
    },
    F64 {
        field_definition: &'a FieldDefinition,
        decoder: F64Decoder<'a>,
    },
    Bool {
        field_definition: &'a FieldDefinition,
        decoder: BoolDecoder<'a>,
    },
    String {
        field_definition: &'a FieldDefinition,
        decoder: StringDecoder<'a>,
    },
    BoolArray {
        field_definition: &'a FieldDefinition,
        decoder: BoolArrayDecoder<'a>,
    },
    U64Array {
        field_definition: &'a FieldDefinition,
        decoder: U64ArrayDecoder<'a>,
    },
    ByteArray {
        field_definition: &'a FieldDefinition,
        decoder: ByteArrayDecoder<'a>,
    },
}

#[cfg_attr(test, derive(Debug))]
pub struct SectionReader<'a> {
    decoders: Vec<ColumnDecoder<'a>>,
    rows: usize,
    schema_entries: Vec<(usize, &'a SchemaEntry)>,
}

impl<'a> SectionReader<'a> {
    pub(crate) fn new(
        input: &'a [u8],
        schema_entries: Vec<(usize, &'a SchemaEntry)>,
        columns: usize,
        rows: usize,
    ) -> Result<Self> {
        let (column_data, presence_column) = parse_presence_column(columns, rows)(input)?;

        let decoders = schema_entries
            .iter()
            .map(|(presence_column_index, schema_entry)| {
                let column_data = &column_data[schema_entry.offset()..schema_entry.offset() + schema_entry.size()];
                let presence_column_view =
                    presence_column
                        .view(*presence_column_index)
                        .ok_or(TracklibError::ParseIncompleteError {
                            needed: nom::Needed::Unknown,
                        })?;
                let field_definition = schema_entry.field_definition();
                let decoder = match field_definition.data_type() {
                    DataType::I64 => ColumnDecoder::I64 {
                        field_definition,
                        decoder: I64Decoder::new(column_data, presence_column_view)?,
                    },
                    DataType::U64 => ColumnDecoder::U64 {
                        field_definition,
                        decoder: U64Decoder::new(column_data, presence_column_view)?,
                    },
                    DataType::F64 { scale } => ColumnDecoder::F64 {
                        field_definition,
                        decoder: F64Decoder::new(column_data, presence_column_view, *scale)?,
                    },
                    DataType::Bool => ColumnDecoder::Bool {
                        field_definition,
                        decoder: BoolDecoder::new(column_data, presence_column_view)?,
                    },
                    DataType::String => ColumnDecoder::String {
                        field_definition,
                        decoder: StringDecoder::new(column_data, presence_column_view)?,
                    },
                    DataType::BoolArray => ColumnDecoder::BoolArray {
                        field_definition,
                        decoder: BoolArrayDecoder::new(column_data, presence_column_view)?,
                    },
                    DataType::U64Array => ColumnDecoder::U64Array {
                        field_definition,
                        decoder: U64ArrayDecoder::new(column_data, presence_column_view)?,
                    },
                    DataType::ByteArray => ColumnDecoder::ByteArray {
                        field_definition,
                        decoder: ByteArrayDecoder::new(column_data, presence_column_view)?,
                    },
                };
                Ok(decoder)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            decoders,
            rows,
            schema_entries,
        })
    }

    pub fn schema(&self) -> Schema {
        Schema::with_fields(
            self.schema_entries
                .iter()
                .map(|(_, schema_entry)| schema_entry.field_definition().clone())
                .collect(),
        )
    }

    pub fn rows_remaining(&self) -> usize {
        self.rows
    }

    pub fn open_column_iter<'r>(&'r mut self) -> Option<ColumnIter<'r, 'a>> {
        if self.rows > 0 {
            self.rows -= 1;
            Some(ColumnIter::new(&mut self.decoders))
        } else {
            None
        }
    }
}

#[cfg_attr(test, derive(Debug))]
pub struct ColumnIter<'a, 'b> {
    decoders: &'a mut Vec<ColumnDecoder<'b>>,
    index: usize,
}

impl<'a, 'b> ColumnIter<'a, 'b> {
    fn new(decoders: &'a mut Vec<ColumnDecoder<'b>>) -> Self {
        Self { decoders, index: 0 }
    }
}

impl<'a, 'b> Iterator for ColumnIter<'a, 'b> {
    type Item = Result<(&'b FieldDefinition, Option<FieldValue>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(decoder_enum) = self.decoders.get_mut(self.index) {
            self.index += 1;
            match decoder_enum {
                ColumnDecoder::I64 {
                    field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(FieldValue::I64)))
                        .map_err(|e| e),
                ),
                ColumnDecoder::U64 {
                    field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(FieldValue::U64)))
                        .map_err(|e| e),
                ),
                ColumnDecoder::F64 {
                    field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(FieldValue::F64)))
                        .map_err(|e| e),
                ),
                ColumnDecoder::Bool {
                    field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(FieldValue::Bool)))
                        .map_err(|e| e),
                ),
                ColumnDecoder::String {
                    field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(FieldValue::String)))
                        .map_err(|e| e),
                ),
                ColumnDecoder::BoolArray {
                    field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(FieldValue::BoolArray)))
                        .map_err(|e| e),
                ),
                ColumnDecoder::U64Array {
                    field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(FieldValue::U64Array)))
                        .map_err(|e| e),
                ),
                ColumnDecoder::ByteArray {
                    field_definition,
                    ref mut decoder,
                } => Some(
                    decoder
                        .decode()
                        .map(|maybe_v| (*field_definition, maybe_v.map(FieldValue::ByteArray)))
                        .map_err(|e| e),
                ),
            }
        } else {
            None
        }
    }
}
