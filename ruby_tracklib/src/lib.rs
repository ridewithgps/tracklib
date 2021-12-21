mod rwtfile;

use rutie::{Class, Object};

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn Init_Tracklib() {
    Class::new("RWTFile", Some(&Class::from_existing("Object"))).define(|itself| {
        itself.def_self("from_bytes", rwtfile::rwtf_from_bytes);
        itself.def_self("from_h", rwtfile::rwtf_from_hash);
        itself.def("to_bytes", rwtfile::rwtf_to_bytes);
        itself.def("to_h", rwtfile::rwtf_to_hash);
        itself.def("metadata", rwtfile::rwtf_metadata);
        itself.def("inspect", rwtfile::rwtf_inspect);
    });
}
