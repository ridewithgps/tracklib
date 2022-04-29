pub mod encrypted;
pub mod standard;
pub mod writer;

use super::crcwriter::CrcWriter;
use crate::consts::SCHEMA_VERSION;
use crate::error::Result;
use crate::schema::*;
use crate::types::SectionEncoding;
use std::io::Write;

pub enum Section {
    Standard(standard::Section),
    Encrypted(encrypted::Section),
}

pub trait SectionWrite {
    fn schema(&self) -> &Schema;
    fn encoding(&self) -> SectionEncoding;
    fn rows_written(&self) -> usize;
    fn open_row_builder(&mut self) -> writer::RowBuilder;
}

pub(crate) trait SectionInternal: SectionWrite {
    fn section_encoding_tag(&self) -> u8;
    fn data_size_overhead(&self) -> usize;
    fn buffers(&self) -> &[writer::Buffer];
    fn write<W: Write>(&self, out: &mut W) -> Result<()>;

    fn write_encoding<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_all(&[self.section_encoding_tag()])?;
        Ok(())
    }

    fn write_rows<W: Write>(&self, out: &mut W) -> Result<()> {
        leb128::write::unsigned(out, u64::try_from(SectionWrite::rows_written(self))?)?;
        Ok(())
    }

    fn write_data_size<W: Write>(&self, out: &mut W) -> Result<()> {
        const CRC_BYTES: usize = 4;
        let presence_bytes_required = (SectionWrite::schema(self).fields().len() + 7) / 8;
        let presence_bytes = (presence_bytes_required * SectionWrite::rows_written(self)) + CRC_BYTES;
        let data_bytes: usize = self.buffers().iter().map(|buffer| buffer.data_size()).sum();
        let full_size = data_bytes + presence_bytes + self.data_size_overhead();

        leb128::write::unsigned(out, u64::try_from(full_size)?)?;
        Ok(())
    }

    #[rustfmt::skip]
    fn write_schema<W: Write>(&self, out: &mut W) -> Result<()> {
        out.write_all(&SCHEMA_VERSION.to_le_bytes())?;                                           // 1 byte  - schema version
        out.write_all(&u8::try_from(SectionWrite::schema(self).fields().len())?.to_le_bytes())?; // 1 byte  - number of entries

        for (i, field_def) in SectionWrite::schema(self).fields().iter().enumerate() {
            let data_column_size = self
                .buffers()
                .get(i)
                .map(|buffer| buffer.data_size())
                .unwrap_or(0);

            match field_def.data_type() {                                                        // ? bytes - type tag
                DataType::I64 => out.write_all(&[0x00])?,
                DataType::F64 { scale } => out.write_all(&[0x01, *scale])?,
                DataType::U64 => out.write_all(&[0x02])?,
                DataType::Bool => out.write_all(&[0x10])?,
                DataType::String => out.write_all(&[0x20])?,
                DataType::BoolArray => out.write_all(&[0x21])?,
                DataType::U64Array => out.write_all(&[0x22])?,
                DataType::ByteArray => out.write_all(&[0x23])?,
            }
            out.write_all(&u8::try_from(field_def.name().len())?.to_le_bytes())?;                // 1 byte  - field name length
            out.write_all(field_def.name().as_bytes())?;                                         // ? bytes - the name of this field
            leb128::write::unsigned(out, u64::try_from(data_column_size)?)?;                     // ? bytes - leb128 column data size
        }

        Ok(())
    }

    fn write_presence_column<W: Write>(&self, out: &mut W) -> Result<()> {
        let mut crcwriter = CrcWriter::new32(out);
        let bytes_required = (SectionWrite::schema(self).fields().len() + 7) / 8;

        for row_i in 0..SectionWrite::rows_written(self) {
            let mut row = vec![0; bytes_required];
            let mut mask: u8 = 1;
            let mut bit_index = (SectionWrite::schema(self).fields().len() + 7) & !7; // next multiple of 8
            for buffer in self.buffers().iter() {
                if let Some(true) = buffer.is_present(row_i) {
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
    fn write_data<W: Write>(&self, out: &mut W) -> Result<()> {
        self.write_presence_column(out)?;                                         // ? bytes - presence column with crc
        for buffer in self.buffers().iter() {
            buffer.write_data(out)?;                                              // ? bytes - data column with crc
        }

        Ok(())
    }
}
