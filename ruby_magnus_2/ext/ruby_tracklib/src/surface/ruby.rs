use super::rust;
use magnus::{typed_data::Obj, wrap};
use std::cell::RefCell;

#[wrap(class = "Tracklib::RoadClassMapping", size)]
pub struct RoadClassMapping {
    inner: RefCell<rust::RoadClassMapping>,
}

impl RoadClassMapping {
    pub fn create(bbox: [f64; 4]) -> Self {
        Self {
            inner: RefCell::new(rust::RoadClassMapping::new(bbox)),
        }
    }

    pub fn add_road_class(rb_self: Obj<Self>, road_class_id: rust::RoadClassId, surface_id: rust::SurfaceTypeId) {
        rb_self.inner.borrow_mut().add_road_class(road_class_id, surface_id);
    }
}

#[wrap(class = "Tracklib::SurfaceMapping", size)]
pub struct SurfaceMapping {
    inner: RefCell<rust::SurfaceMapping>,
}

impl SurfaceMapping {
    pub fn create(unknown_surface_id: rust::SurfaceTypeId) -> Self {
        Self {
            inner: RefCell::new(rust::SurfaceMapping::new(unknown_surface_id)),
        }
    }

    pub fn add_surface(rb_self: Obj<Self>, surface_id: rust::SurfaceTypeId, group: String) {
        rb_self.inner.borrow_mut().add_surface(surface_id, group);
    }

    pub fn add_road_class_mapping(rb_self: Obj<Self>, road_class_mapping: &RoadClassMapping) {
        rb_self
            .inner
            .borrow_mut()
            .add_road_class_mapping(road_class_mapping.inner.borrow().clone());
    }

    pub(crate) fn inner(&self) -> std::cell::Ref<rust::SurfaceMapping> {
        self.inner.borrow()
    }
}
