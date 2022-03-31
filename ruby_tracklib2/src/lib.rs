mod track_reader;
use rutie::{Class, Object};

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn Init_Tracklib() {
    Class::new("TrackReader", None).define(|klass| {
        klass.def_self("new", track_reader::trackreader_new);
        klass.def("metadata", track_reader::trackreader_metadata);
        klass.def("file_version", track_reader::trackreader_file_version);
        klass.def("creator_version", track_reader::trackreader_creator_version);
    });
}
