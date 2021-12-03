use super::metadata::{self, MetadataEntry};
use super::section::Section;
use std::io::Write;

struct TrackWriter<W: Write> {
    out: W,
    metadata_entries: Vec<MetadataEntry>,
    sections: Vec<Section>,
}

impl<W: Write> TrackWriter<W> {
    fn new(out: W) -> Self {
        Self {
            out,
            metadata_entries: Vec::new(),
            sections: Vec::new(),
        }
    }

    fn add_metadata_entry(&mut self, entry: metadata::MetadataEntry) {
        self.metadata_entries.push(entry);
    }

    fn add_section(&mut self, section: Section) {
        self.sections.push(section);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
