use jni::objects::{JClass, JList, JMap, JObject, JValue};
use jni::sys::{jbyteArray, jobject};
use jni::JNIEnv;
use tracklib::{parse_rwtf, Column};

mod error;
use crate::error::{Error, Result};

fn java_parse_rwtf(env: &JNIEnv, input: jbyteArray) -> Result<jobject> {
    // parse the input
    let bytes = env.convert_byte_array(input)?;
    let (_, rwtf) = parse_rwtf(&bytes)?;
    let track_points = rwtf.track_points;

    // create a list in the jvm and start adding to it
    let java_list = JList::from_env(env, env.new_object("java/util/ArrayList", "()V", &[])?)?;
    for i in 0..track_points.len() {
        let java_map = JMap::from_env(env, env.new_object("java/util/HashMap", "()V", &[])?)?;

        for (column_name, column) in track_points.columns() {
            let java_datafield = env.new_object("com/ridewithgps/tracklib/DataField", "()V", &[])?;

            let add_result = match column {
                Column::Numbers(m) => m.get(&i).map(|v| {
                    env.call_method(java_datafield,
                                    "setNumberValue",
                                    "(J)V",
                                    &[JValue::Long(*v)])
                }),
                Column::LongFloat(m) => m.get(&i).map(|v| {
                    env.call_method(java_datafield,
                                    "setLongFloatValue",
                                    "(D)V",
                                    &[JValue::Double(*v)])
                }),
                Column::ShortFloat(m) => m.get(&i).map(|v| {
                    env.call_method(java_datafield,
                                    "setShortFloatValue",
                                    "(D)V",
                                    &[JValue::Double(*v)])
                }),
                Column::Base64(m) => m.get(&i).map(|v| {
                    env.call_method(java_datafield,
                                    "setBase64Value",
                                    "(Ljava/lang/String;)V",
                                    &[env.new_string(base64::encode(v))?.into()])
                }),
                Column::String(m) => m.get(&i).map(|v| {
                    env.call_method(java_datafield,
                                    "setStringValue",
                                    "(Ljava/lang/String;)V",
                                    &[env.new_string(v)?.into()])
                }),
                Column::Bool(m) => m.get(&i).map(|v| {
                    env.call_method(java_datafield,
                                    "setBoolValue",
                                    "(Z)V",
                                    &[JValue::Bool(*v as u8)])
                }),
                Column::IDs(m) => m.get(&i).map(|v| {
                    let ids_list = JList::from_env(env, env.new_object("java/util/ArrayList", "()V", &[])?)?;

                    for val in v {
                        let java_long = env.call_static_method("java/lang/Long",
                                                               "parseUnsignedLong",
                                                               "(Ljava/lang/String;)J",
                                                               &[env.new_string(val.to_string())?.into()])?;
                        ids_list.add(env.new_object("java/lang/Long", "(J)V", &[java_long])?)?;
                    }

                    env.call_method(java_datafield,
                                    "setIDsValue",
                                    "(Ljava/util/List;)V",
                                    &[ids_list.into()])
                }),
            };

            match add_result {
                None => {
                    // if there's no data for this field in this point then do nothing
                }
                Some(Ok(_)) => {
                    // java_datafield is correct, add it to the map
                    java_map.put(env.new_string(column_name)?.into(), java_datafield)?;
                }
                Some(Err(e)) => {
                    // we got an error so return it
                    return Err(Error::from(e));
                }
            }
        }

        java_list.add(java_map.into())?;
    }

    Ok(java_list.into_inner())
}

#[no_mangle]
pub extern "C" fn Java_com_ridewithgps_tracklib_RWTF_parse_1rwtf(env: JNIEnv, _class: JClass, input: jbyteArray) -> jobject {
    match java_parse_rwtf(&env, input) {
        Err(e) => {
            if !env.exception_check().expect("Failed to check exception status") {
                env.throw_new("com/ridewithgps/tracklib/ParseException",
                              match e {
                                  error::Error::JNIError(jni_e) => jni_e.to_string(),
                                  error::Error::NomError => "RWTF Parse Error".to_string(),
                              })
                    .expect("Failed to create new ParseException");
            }
            JObject::null().into_inner()
        }
        Ok(obj) => obj
    }
}
