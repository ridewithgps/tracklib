mod read;
mod schema;
mod write;
use rutie::{Module, Object};

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn Init_Tracklib() {
    Module::from_existing("Tracklib").define(|module| {
        module
            .define_nested_class("TrackReader", None)
            .define(|class| {
                class.def_self("new", read::trackreader_new);
                class.def("metadata", read::trackreader_metadata);
                class.def("file_version", read::trackreader_file_version);
                class.def("creator_version", read::trackreader_creator_version);

                class.def("section_count", read::trackreader_section_count);
                class.def("section_encoding", read::trackreader_section_encoding);
                class.def("section_schema", read::trackreader_section_schema);
                class.def("section_rows", read::trackreader_section_rows);
                class.def("section_data", read::trackreader_section_data);
                class.def("section_column", read::trackreader_section_column);
            });

        module.define_nested_class("Schema", None).define(|class| {
            class.def_self("new", schema::schema_new);
        });

        module.define_nested_class("Section", None).define(|class| {
            class.def_self("standard", write::section_standard);
            class.def_self("encrypted", write::section_encrypted);
        });

        module.define_module_function("write_track", write::write_track);
    });
}
