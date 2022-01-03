mod polyline;
mod rwtfile;
mod surface;

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
        itself.def("simplify_track_points", rwtfile::rwtf_simplify_track_points);
        itself.def("inspect", rwtfile::rwtf_inspect);
        itself.def("to_s", rwtfile::rwtf_inspect);
    });

    Class::new("RoadClassMapping", Some(&Class::from_existing("Object"))).define(|itself| {
        itself.def_self("new", surface::road_class_mapping_new);
        itself.def("add_road_class", surface::road_class_mapping_add_road_class);
        itself.def("to_s", surface::road_class_mapping_to_s);
    });

    Class::new("SurfaceMapping", Some(&Class::from_existing("Object"))).define(|itself| {
        itself.def_self("new", surface::surface_mapping_new);
        itself.def("add_surface", surface::surface_mapping_add_surface);
        itself.def("add_road_class_mapping", surface::surface_mapping_add_road_class_mapping);
        itself.def("to_s", surface::surface_mapping_to_s);
    });

    Class::new("FieldEncodeOptionsVec", Some(&Class::from_existing("Object"))).define(|itself| {
        itself.def_self("new", polyline::field_encode_options_vec_new);
        itself.def("add_y", polyline::field_encode_options_vec_add_field_y);
        itself.def("add_x", polyline::field_encode_options_vec_add_field_x);
        itself.def("add_d", polyline::field_encode_options_vec_add_field_d);
        itself.def("add_e", polyline::field_encode_options_vec_add_field_e);
        itself.def("add_s", polyline::field_encode_options_vec_add_field_s);
        itself.def("to_s", polyline::field_encode_options_vec_to_s);
    });
}
