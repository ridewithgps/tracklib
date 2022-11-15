pub mod bitstream;
mod crc;
mod data_table;
mod decoders;
mod header;
#[cfg(feature = "inspect")]
pub mod inspect;
mod metadata;
mod presence_column;
mod schema;
pub mod section;
pub mod track;
