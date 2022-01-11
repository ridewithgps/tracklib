use lazy_static::lazy_static;
use rutie::{AnyException, AnyObject, Class, Integer, NilClass, Object, RString, VM, VerifiedObject, class, methods, wrappable_struct};
use tracklib::{FieldEncodeOptions, PointField};
use std::convert::TryFrom;

fn add_field_helper(v: &mut Vec<FieldEncodeOptions>, field: PointField, precision: Result<Integer, AnyException>) {
    let prec = u32::try_from(precision.map_err(|e| VM::raise_ex(e)).unwrap().to_i32())
        .map_err(|_| VM::raise(Class::from_existing("Exception"), "Precision must be a u32")).unwrap();
    v.push(FieldEncodeOptions::new(field, prec))
}

pub struct FieldEncodeOptionsVecInner {
    inner: Vec<FieldEncodeOptions>,
}

wrappable_struct!(
    FieldEncodeOptionsVecInner,
    FieldEncodeOptionsVecInnerWrapper,
    FIELD_ENCODE_OPTIONS_VEC_INNER_WRAPPER
);

class!(RubyFieldEncodeOptionsVec);

methods!(
    RubyFieldEncodeOptionsVec,
    itself,

    fn field_encode_options_vec_new() -> AnyObject {
        let inner = FieldEncodeOptionsVecInner {
            inner: Vec::new()
        };

        Class::from_existing("TracklibFieldEncodeOptionsVec").wrap_data(inner, &*FIELD_ENCODE_OPTIONS_VEC_INNER_WRAPPER)
    }

    fn field_encode_options_vec_add_field_y(precision: Integer) -> NilClass {
        let inner = &mut itself.get_data_mut(&*FIELD_ENCODE_OPTIONS_VEC_INNER_WRAPPER).inner;
        add_field_helper(inner, PointField::Y, precision);
        NilClass::new()
    }

    fn field_encode_options_vec_add_field_x(precision: Integer) -> NilClass {
        let inner = &mut itself.get_data_mut(&*FIELD_ENCODE_OPTIONS_VEC_INNER_WRAPPER).inner;
        add_field_helper(inner, PointField::X, precision);
        NilClass::new()
    }

    fn field_encode_options_vec_add_field_d(precision: Integer) -> NilClass {
        let inner = &mut itself.get_data_mut(&*FIELD_ENCODE_OPTIONS_VEC_INNER_WRAPPER).inner;
        add_field_helper(inner, PointField::D, precision);
        NilClass::new()
    }

    fn field_encode_options_vec_add_field_e(precision: Integer) -> NilClass {
        let inner = &mut itself.get_data_mut(&*FIELD_ENCODE_OPTIONS_VEC_INNER_WRAPPER).inner;
        add_field_helper(inner, PointField::E, precision);
        NilClass::new()
    }

    fn field_encode_options_vec_add_field_s(default_surface: Integer, default_road_class: Integer, precision: Integer) -> NilClass {
        let default_surface_id = default_surface.map_err(|e| VM::raise_ex(e)).unwrap().to_i64();
        let default_road_class_id = default_road_class.map_err(|e| VM::raise_ex(e)).unwrap().to_i64();
        let inner = &mut itself.get_data_mut(&*FIELD_ENCODE_OPTIONS_VEC_INNER_WRAPPER).inner;
        add_field_helper(inner, PointField::S{default_surface_id, default_road_class_id}, precision);
        NilClass::new()
    }

    fn field_encode_options_vec_to_s() -> RString {
        let inner = &itself.get_data(&*FIELD_ENCODE_OPTIONS_VEC_INNER_WRAPPER).inner;

        RString::new_utf8(&format!("{:?}", inner))
    }
);

impl RubyFieldEncodeOptionsVec {
    pub fn inner(&self) -> &[FieldEncodeOptions] {
        &self.get_data(&*FIELD_ENCODE_OPTIONS_VEC_INNER_WRAPPER).inner
    }
}

impl VerifiedObject for RubyFieldEncodeOptionsVec {
    fn is_correct_type<T: Object>(object: &T) -> bool {
        Class::from_existing("TracklibFieldEncodeOptionsVec").case_equals(object)
    }

    fn error_message() -> &'static str {
        "Error converting to FieldEncodeOptionsVec"
    }
}
