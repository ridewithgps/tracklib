use std::collections::{HashMap};
use std::convert::{TryFrom};
use std::io::{BufWriter};
use lazy_static::lazy_static;
use rutie::{
    class, methods, wrappable_struct, AnyObject, Array, Boolean, Class, Encoding, Float, Hash, Integer,
    Module, Object, RString, VM,
};
use rutie_serde::{ruby_class, rutie_serde_methods};
use tracklib::{parse_rwtf, DataField, RWTFile, RWTFMetadata, TrackType};

fn any_to_float(o: AnyObject) -> f64 {
    match o.try_convert_to::<Float>() {
        Ok(f) => f.to_f64(),
        Err(float_e) => o
            .try_convert_to::<Integer>()
            .map_err(|_| VM::raise_ex(float_e))
            .unwrap()
            .to_i64() as f64,
    }
}

fn any_to_int(o: AnyObject) -> i64 {
    match o.try_convert_to::<Integer>() {
        Ok(f) => f.to_i64(),
        Err(float_e) => o
            .try_convert_to::<Float>()
            .map_err(|_| VM::raise_ex(float_e))
            .unwrap()
            .to_f64() as i64,
    }
}

fn any_to_str(o: AnyObject) -> String {
    o.try_convert_to::<RString>()
        .map_err(|e| VM::raise_ex(e))
        .unwrap()
        .to_string()
}

fn any_to_bool(o: AnyObject) -> bool {
    o.try_convert_to::<Boolean>()
        .map_err(|e| VM::raise_ex(e))
        .unwrap()
        .to_bool()
}

fn any_to_ids(o: AnyObject) -> Vec<u64> {
    o.try_convert_to::<Array>()
        .map_err(|e| VM::raise_ex(e))
        .unwrap()
        .into_iter()
        .map(|ele| {
            ele.try_convert_to::<Integer>()
                .map_err(|e| VM::raise_ex(e))
                .unwrap()
                .to_u64()
        })
        .collect()
}

fn convert_config(config: Hash) -> HashMap<String, String> {
    let mut hm = HashMap::new();

    config.each(|rwtf_type_name, array_of_field_names| {
        let type_name_obj = rwtf_type_name
            .try_convert_to::<RString>()
            .map_err(|e| VM::raise_ex(e))
            .unwrap();
        let array_obj = array_of_field_names
            .try_convert_to::<Array>()
            .map_err(|e| VM::raise_ex(e))
            .unwrap();

        for field_name in array_obj.into_iter() {
            let field_name_obj = field_name
                .try_convert_to::<RString>()
                .map_err(|e| VM::raise_ex(e))
                .unwrap();

            hm.insert(field_name_obj.to_string(), type_name_obj.to_string());
        }
    });

    hm
}

fn add_points(points: Array, mut callback: impl FnMut(usize, &str, AnyObject)) {
    for (i, maybe_point) in points.into_iter().enumerate() {
        let point = maybe_point
            .try_convert_to::<Hash>()
            .map_err(|e| VM::raise_ex(e))
            .unwrap();

        point.each(|k, v| {
            let field_name_obj = k
                .try_convert_to::<RString>()
                .map_err(|e| VM::raise_ex(e))
                .unwrap();
            let name = field_name_obj.to_str();
            let mut store_field = true;

            if v.is_nil() {
                store_field = false;
            }

            if name.is_empty() {
                store_field = false;
            }

            match v.try_convert_to::<RString>() {
                Ok(s) => if s.to_str().is_empty() {
                    store_field = false;
                }
                Err(_) => {}
            }

            match v.try_convert_to::<Array>() {
                Ok(a) => if a.length() == 0 {
                    store_field = false;
                }
                Err(_) => {}
            }

            match v.try_convert_to::<Integer>() {
                Ok(i) => if i.to_i64().abs() > 2i64.pow(48) {
                    store_field = false;
                }
                Err(_) => match v.try_convert_to::<Float>() {
                    Ok(f) => if f.to_f64().abs() > 2f64.powi(48) {
                        store_field = false;
                    }
                    Err(_) => {}
                }
            }

            if store_field {
                callback(i, name, v);
            }
        });
    }
}

pub struct Inner {
    inner: RWTFile,
}

wrappable_struct!(Inner, InnerWrapper, INNER_WRAPPER);

class!(RubyRWTFile);

methods!(
    RubyRWTFile,
    itself,

    fn rwtf_from_bytes(bytes: RString) -> AnyObject {
        let source = bytes.map_err(|e| VM::raise_ex(e)).unwrap();
        let (_, rwtf) = parse_rwtf(source.to_bytes_unchecked())
            .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{}", e)))
            .unwrap();
        let inner = Inner { inner: rwtf };

        Class::from_existing("RWTFile").wrap_data(inner, &*INNER_WRAPPER)
    }

    fn rwtf_to_bytes() -> RString {
        let mut writer = BufWriter::new(Vec::new());
        itself.get_data(&*INNER_WRAPPER).inner.write(&mut writer)
            .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{}", e)))
            .unwrap();

        let encoding = Encoding::find("ASCII-8BIT")
            .map_err(|e| VM::raise_ex(e))
            .unwrap();

        let buf = writer.into_inner()
            .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{}", e)))
            .unwrap();

        RString::from_bytes(&buf, &encoding)
    }

    fn rwtf_from_hash(input: Hash, config_input: Hash, metadata: Hash) -> AnyObject {
        let source = input.map_err(|e| VM::raise_ex(e)).unwrap();
        let config = config_input.map_err(|e| VM::raise_ex(e)).unwrap();
        let track_points_config = convert_config(config.at(&RString::new_utf8("track_points"))
                                                 .try_convert_to::<Hash>()
                                                 .map_err(|e| VM::raise_ex(e))
                                                 .unwrap());
        let course_points_config = convert_config(config.at(&RString::new_utf8("course_points"))
                                                  .try_convert_to::<Hash>()
                                                  .map_err(|e| VM::raise_ex(e))
                                                  .unwrap());

        let mut rwtf = if let Some(md) = metadata.ok() {
            let tt_metadata = md.at(&RString::new_utf8("track_type"))
                .try_convert_to::<Hash>()
                .map_err(|e| VM::raise_ex(e))
                .unwrap();

            let track_type = tt_metadata.at(&RString::new_utf8("type"))
                .try_convert_to::<RString>()
                .map_err(|e| VM::raise_ex(e))
                .unwrap();

            let id = u32::try_from(tt_metadata.at(&RString::new_utf8("id"))
                                   .try_convert_to::<Integer>()
                                   .map_err(|e| VM::raise_ex(e))
                                   .unwrap()
                                   .to_u64())
                .map_err(|e| VM::raise(Class::from_existing("Exception"), &format!("{}", e)))
                .unwrap();

            let tt = match track_type.to_str() {
                "trip" => TrackType::Trip(id),
                "route" => TrackType::Route(id),
                "segment" => TrackType::Segment(id),
                _ => {
                    VM::raise(
                        Class::from_existing("Exception"),
                        &format!("unknown track_type metadata: {}", track_type.to_str()),
                    );
                    unreachable!();
                }
            };

            RWTFile::with_track_type(tt)
        } else {
            RWTFile::new()
        };

        let maybe_track_points = source
            .at(&RString::new_utf8("track_points"))
            .try_convert_to::<Array>();

        if let Ok(track_points) = maybe_track_points {
            add_points(track_points, |i, k, v| {
                let data = if let Some(field_type) = track_points_config.get(k) {
                    match field_type.as_str() {
                        "LongFloat"  => DataField::LongFloat(any_to_float(v)),
                        "ShortFloat" => DataField::ShortFloat(any_to_float(v)),
                        "Number"     => DataField::Number(any_to_int(v)),
                        "Base64"     => DataField::Base64(any_to_str(v).replace("\n", "")),
                        "String"     => DataField::String(any_to_str(v)),
                        "Bool"       => DataField::Bool(any_to_bool(v)),
                        "IDs"        => DataField::IDs(any_to_ids(v)),
                        _ => {
                            VM::raise(
                                Class::from_existing("Exception"),
                                &format!("unknown track_points type: {}", field_type),
                            );
                            unreachable!();
                        }
                    }
                } else {
                    VM::raise(Module::from_existing("Tracklib").get_nested_class("UnknownFieldError"),
                              &format!("unknown track_points field: {}", k));
                    unreachable!();
                };

                rwtf.add_track_point(i, k, data)
                    .map_err(|e| {
                        VM::raise(Class::from_existing("Exception"), &format!("{}", e))
                    })
                    .unwrap();
            });
        }

        let maybe_course_points = source
            .at(&RString::new_utf8("course_points"))
            .try_convert_to::<Array>();

        if let Ok(course_points) = maybe_course_points {
            add_points(course_points, |i, k, v| {
                let data = if let Some(field_type) = course_points_config.get(k) {
                    match field_type.as_str() {
                        "LongFloat"  => DataField::LongFloat(any_to_float(v)),
                        "ShortFloat" => DataField::ShortFloat(any_to_float(v)),
                        "Number"     => DataField::Number(any_to_int(v)),
                        "Base64"     => DataField::Base64(any_to_str(v).replace("\n", "")),
                        "String"     => DataField::String(any_to_str(v)),
                        "Bool"       => DataField::Bool(any_to_bool(v)),
                        "IDs"        => DataField::IDs(any_to_ids(v)),
                        _ => {
                            VM::raise(
                                Class::from_existing("Exception"),
                                &format!("unknown course_points type: {}", field_type),
                            );
                            unreachable!();
                        }
                    }
                } else {
                    VM::raise(Module::from_existing("Tracklib").get_nested_class("UnknownFieldError"),
                              &format!("unknown course_points field: {}", k));
                    unreachable!();
                };

                rwtf.add_course_point(i, k, data)
                    .map_err(|e| {
                        VM::raise(Class::from_existing("Exception"), &format!("{}", e))
                    })
                    .unwrap();
            });
        }

        let inner = Inner { inner: rwtf };
        Class::from_existing("RWTFile").wrap_data(inner, &*INNER_WRAPPER)
    }

    fn rwtf_inspect() -> RString {
        let rwtf = &itself.get_data(&*INNER_WRAPPER).inner;

        RString::new_utf8(&format!("RWTFile<track_points: {}, course_points: {}>",
                                   rwtf.track_points.len(),
                                   rwtf.course_points.len()))
    }
);

rutie_serde_methods!(
    RubyRWTFile,
    itself,
    ruby_class!(Exception),

    fn rwtf_to_hash() -> &RWTFile {
        &itself.get_data(&*INNER_WRAPPER).inner
    }

    fn rwtf_metadata() -> &RWTFMetadata {
        &itself.get_data(&*INNER_WRAPPER).inner.metadata()
    }
);

#[allow(non_snake_case)]
#[no_mangle]
pub extern "C" fn Init_Tracklib() {
    Class::new("RWTFile", Some(&Class::from_existing("Object"))).define(|itself| {
        itself.def_self("from_bytes", rwtf_from_bytes);
        itself.def_self("from_h", rwtf_from_hash);
        itself.def("to_bytes", rwtf_to_bytes);
        itself.def("to_h", rwtf_to_hash);
        itself.def("metadata", rwtf_metadata);
        itself.def("inspect", rwtf_inspect);
    });
}
