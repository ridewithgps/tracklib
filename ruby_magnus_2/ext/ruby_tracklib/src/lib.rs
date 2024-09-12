use magnus::{function, method, Error, Module, Object, Ruby};

mod geometry;
mod polyline;
mod read;
mod schema;
mod simplify;
mod surface;
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
    trackreader.define_method("section_polyline", method!(read::TrackReader::section_polyline, -1))?;
    trackreader.define_method(
        "section_data_simplified",
        method!(read::TrackReader::section_data_simplified, -1),
    )?;
    trackreader.define_method(
        "section_column_simplified",
        method!(read::TrackReader::section_column_simplified, -1),
    )?;
    trackreader.define_method(
        "section_simplified_polyline",
        method!(read::TrackReader::section_simplified_polyline, -1),
    )?;

    let schema = module.define_class("Schema", handle.class_object())?;
    schema.define_singleton_method("new", function!(schema::Schema::create, 1))?;

    let section = module.define_class("Section", handle.class_object())?;
    section.define_singleton_method("standard", function!(write::Section::create_standard, 2))?;
    section.define_singleton_method("encrypted", function!(write::Section::create_encrypted, 3))?;

    module.define_module_function("write_track", function!(write::write_track, 2))?;

    let road_class_mapping = module.define_class("RoadClassMapping", handle.class_object())?;
    road_class_mapping.define_singleton_method("new", function!(surface::ruby::RoadClassMapping::create, 1))?;
    road_class_mapping.define_method(
        "add_road_class",
        method!(surface::ruby::RoadClassMapping::add_road_class, 2),
    )?;

    let surface_mapping = module.define_class("SurfaceMapping", handle.class_object())?;
    surface_mapping.define_singleton_method("new", function!(surface::ruby::SurfaceMapping::create, 1))?;
    surface_mapping.define_method("add_surface", method!(surface::ruby::SurfaceMapping::add_surface, 2))?;
    surface_mapping.define_method(
        "add_road_class_mapping",
        method!(surface::ruby::SurfaceMapping::add_road_class_mapping, 1),
    )?;

    let polyline_options = module.define_class("PolylineOptions", handle.class_object())?;
    polyline_options.define_singleton_method("new", function!(polyline::ruby::PolylineOptions::create, 1))?;

    Ok(())
}
