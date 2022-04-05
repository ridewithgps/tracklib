mod schema;
mod track_reader;
use rutie::{Module, Object};

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn Init_Tracklib() {
    Module::from_existing("Tracklib").define(|module| {
        module
            .define_nested_class("TrackReader", None)
            .define(|class| {
                class.def_self("new", track_reader::trackreader_new);
                class.def("metadata", track_reader::trackreader_metadata);
                class.def("file_version", track_reader::trackreader_file_version);
                class.def("creator_version", track_reader::trackreader_creator_version);

                class.def("section_count", track_reader::trackreader_section_count);
                class.def("section_encoding", track_reader::trackreader_section_encoding);
                class.def("section_schema", track_reader::trackreader_section_schema);
                class.def("section_rows", track_reader::trackreader_section_rows);
                class.def("section_data", track_reader::trackreader_section_data);
            });
    });
}
