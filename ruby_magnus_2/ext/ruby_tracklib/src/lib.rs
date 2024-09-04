use magnus::{function, method, Error, Module, Object, Ruby};

mod read;
mod schema;
mod write;

#[magnus::init]
fn init(handle: &Ruby) -> Result<(), Error> {
    let module = handle.define_module("Tracklib")?;
    let trackreader = module.define_class("TrackReader", handle.class_object())?;
    trackreader.define_singleton_method("new", function!(read::TrackReader::create, 1))?;
    trackreader.define_method("file_version", method!(read::TrackReader::file_version, 0))?;
    trackreader.define_method("creator_version", method!(read::TrackReader::creator_version, 0))?;
    trackreader.define_method("metadata", method!(read::TrackReader::metadata, 0))?;
    trackreader.define_method("section_count", method!(read::TrackReader::section_count, 0))?;
    trackreader.define_method("section_encoding", method!(read::TrackReader::section_encoding, 1))?;
    trackreader.define_method("section_schema", method!(read::TrackReader::section_schema, 1))?;
    trackreader.define_method("section_rows", method!(read::TrackReader::section_rows, 1))?;
    trackreader.define_method("section_data", method!(read::TrackReader::section_data, -1))?;
    trackreader.define_method("section_column", method!(read::TrackReader::section_column, -1))?;

    let schema = module.define_class("Schema", handle.class_object())?;
    schema.define_singleton_method("new", function!(schema::Schema::create, 1))?;

    let section = module.define_class("Section", handle.class_object())?;
    section.define_singleton_method("standard", function!(write::Section::create_standard, 2))?;
    section.define_singleton_method("encrypted", function!(write::Section::create_encrypted, 3))?;

    module.define_module_function("write_track", function!(write::write_track, 2))?;

    Ok(())
}
