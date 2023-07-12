pub mod encrypted;
pub mod reader;
pub mod standard;

use crate::read::data_table::DataTableEntry;
use crate::schema::Schema;
use crate::types::SectionEncoding;

#[cfg_attr(test, derive(Debug))]
pub enum Section<'a> {
    Standard(standard::Section<'a>),
    Encrypted(encrypted::Section<'a>),
}

pub trait SectionRead {
    fn encoding(&self) -> SectionEncoding;
    fn schema(&self) -> Schema;
    fn rows(&self) -> usize;
}

impl<'a> Section<'a> {
    pub(crate) fn new(input: &'a [u8], data_table_entry: &'a DataTableEntry) -> Self {
        match data_table_entry.section_encoding() {
            SectionEncoding::Standard => Self::Standard(standard::Section::new(input, data_table_entry)),
            SectionEncoding::Encrypted => Self::Encrypted(encrypted::Section::new(input, data_table_entry)),
        }
    }
}
