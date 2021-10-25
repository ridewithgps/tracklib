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
        Ok(i) => i.to_i64(),
        Err(int_e) => o
            .try_convert_to::<Float>()
            .map_err(|_| VM::raise_ex(int_e))
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

#[derive(Debug, Copy, Clone)]
enum ColumnType {
    Numbers,
    LongFloat,
    ShortFloat,
    Base64,
    String,
    Bool,
    IDs,
}

impl ColumnType {
    fn from_str(name: &str) -> Option<Self> {
        match name {
            "Number"     => Some(ColumnType::Numbers),
            "LongFloat"  => Some(ColumnType::LongFloat),
            "ShortFloat" => Some(ColumnType::ShortFloat),
            "Base64"     => Some(ColumnType::Base64),
            "String"     => Some(ColumnType::String),
            "Bool"       => Some(ColumnType::Bool),
            "IDs"        => Some(ColumnType::IDs),
            _ => None
        }
    }

    fn exponent(&self) -> u8 {
        match self {
            ColumnType::Numbers => 48,
            ColumnType::LongFloat => 24,
            ColumnType::ShortFloat => 38,
            _ => {
                VM::raise(Class::from_existing("Exception"),
                          &format!("can't handle numeric value for non-numeric field"));
                unreachable!();
            }
        }
    }

    fn max_integer(&self) -> i64 {
        2i64.pow(u32::from(self.exponent()))
    }

    fn max_float(&self) -> f64 {
        2f64.powi(i32::from(self.exponent()))
    }
}

fn convert_config(config: Hash) -> HashMap<String, ColumnType> {
    let mut hm = HashMap::new();

    config.each(|rwtf_type_name, array_of_field_names| {
        let type_name_obj = rwtf_type_name
            .try_convert_to::<RString>()
            .map_err(|e| VM::raise_ex(e))
            .unwrap();
        let type_name_str = type_name_obj.to_str();

        if let Some(column_type) = ColumnType::from_str(type_name_str) {
            let array_obj = array_of_field_names
                .try_convert_to::<Array>()
                .map_err(|e| VM::raise_ex(e))
                .unwrap();

            for field_name in array_obj.into_iter() {
                let field_name_obj = field_name
                    .try_convert_to::<RString>()
                    .map_err(|e| VM::raise_ex(e))
                    .unwrap();

                hm.insert(field_name_obj.to_string(), column_type);
            }
        } else {
            VM::raise(Class::from_existing("Exception"),
                      &format!("unknown rwtf_field_config type: {}", type_name_str));
            unreachable!();
        }
    });

    hm
}

fn is_empty_string(v: &AnyObject) -> bool {
    match v.try_convert_to::<RString>() {
        Ok(s) => s.to_str().is_empty(),
        Err(_) => false
    }
}

fn is_empty_array(v: &AnyObject) -> bool {
    match v.try_convert_to::<Array>() {
        Ok(a) => a.length() == 0,
        Err(_) => false
    }
}

fn is_number_and_too_large(v: &AnyObject, field_type: ColumnType) -> bool {
    // First we have to try to cast `v` to a numeric type (Integer or Float)
    // and then call to_i64/f64(). This will raise an exception if the number
    // is too large to turn into a primitive.
    let is_number_result = VM::protect(|| {
        match v.try_convert_to::<Integer>() {
            Ok(i) => {
                let _ = i.to_i64(); // force a conversion
                Boolean::new(true)
            }
            Err(_) => match v.try_convert_to::<Float>() {
                Ok(f) => {
                    let _ = f.to_f64(); // force a conversion
                    Boolean::new(true)
                }
                Err(_) => Boolean::new(false)
            }
        }.to_any_object()
    });

    match is_number_result {
        Ok(is_number) => {
            // Here we know that no exception was raised during the attempted primitive conversion.
            // We also know that `is_number` is a Boolean, so this unsafe cast is fine.
            if unsafe { is_number.to::<Boolean>().to_bool() } {
                match v.try_convert_to::<Integer>() {
                    Ok(i) => i.to_i64().abs() > field_type.max_integer(),
                    Err(_) => match v.try_convert_to::<Float>() {
                        Ok(f) => f.to_f64().abs() > field_type.max_float(),
                        Err(_) => false
                    }
                }
            } else {
                false
            }
        }
        Err(_) => {
            VM::clear_error_info(); // clear ruby VM error register
            true // this IS a number and it IS too large
        }
    }
}

fn add_points(source: &Hash,
              section_points_config: &HashMap<String, ColumnType>,
              section_type: &str,
              mut callback: impl FnMut(usize, &str, DataField)) {
    let maybe_section_points = source
        .at(&RString::new_utf8(section_type))
        .try_convert_to::<Array>();

    if let Ok(section_points) = maybe_section_points {
        for (i, maybe_point) in section_points.into_iter().enumerate() {
            let point = maybe_point
                .try_convert_to::<Hash>()
                .map_err(|e| VM::raise_ex(e))
                .unwrap();

            point.each(|k: AnyObject, v: AnyObject| {
                let field_name_obj = k
                    .try_convert_to::<RString>()
                    .map_err(|e| VM::raise_ex(e))
                    .unwrap();
                let name = field_name_obj.to_str();

                if !name.is_empty() {
                    if let Some(field_type) = section_points_config.get(name) {
                        if !v.is_nil()
                            && !is_empty_string(&v)
                            && !is_empty_array(&v)
                            && !is_number_and_too_large(&v, *field_type)
                        {
                            let data = match field_type {
                                ColumnType::LongFloat  => DataField::LongFloat(any_to_float(v)),
                                ColumnType::ShortFloat => DataField::ShortFloat(any_to_float(v)),
                                ColumnType::Numbers    => DataField::Number(any_to_int(v)),
                                ColumnType::Base64     => DataField::Base64(any_to_str(v).replace("\n", "")),
                                ColumnType::String     => DataField::String(any_to_str(v)),
                                ColumnType::Bool       => DataField::Bool(any_to_bool(v)),
                                ColumnType::IDs        => DataField::IDs(any_to_ids(v)),
                            };

                            callback(i, name, data);
                        }
                    } else {
                        VM::raise(Module::from_existing("Tracklib").get_nested_class("UnknownFieldError"),
                                  &format!("unknown {} field: {}", section_type, name));
                        unreachable!();
                    }
                }
            });
        }
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

        add_points(&source, &track_points_config, "track_points",  |i, name, data| {
            rwtf.add_track_point(i, name, data)
                .map_err(|e| {
                    VM::raise(Class::from_existing("Exception"), &format!("{}", e))
                })
                .unwrap();
        });

        add_points(&source, &course_points_config, "course_points",  |i, name, data| {
            rwtf.add_course_point(i, name, data)
                .map_err(|e| {
                    VM::raise(Class::from_existing("Exception"), &format!("{}", e))
                })
                .unwrap();
        });

        let inner = Inner { inner: rwtf };
        Class::from_existing("RWTFile").wrap_data(inner, &*INNER_WRAPPER)
    }

    fn rwtf_inspect() -> RString {
        let rwtf = &itself.get_data(&*INNER_WRAPPER).inner;

        RString::new_utf8(&format!("RWTFile<track_points: {}, course_points: {}>",
                                   rwtf.track_points.len(),
                                   rwtf.course_points.len()))
    }

    fn rwtf_simplify() -> RString {
        let rwtf = &itself.get_data(&*INNER_WRAPPER).inner;

        let simplified = tracklib::simplification::simplify(&rwtf);
        let polyline = polyline::encode_coordinates(simplified, 5).unwrap();

        RString::new_utf8(&polyline)
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
        itself.def("simplify", rwtf_simplify);
    });
}
