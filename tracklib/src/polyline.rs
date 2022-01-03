use crate::simplification::Point;
use crate::surface::{RoadClassId, SurfaceTypeId};
use std::convert::TryFrom;

#[derive(Debug)]
pub enum PointField {
    Y,
    X,
    D,
    E,
    S {
        default_surface_id: SurfaceTypeId,
        default_road_class_id: RoadClassId,
    },
}

#[derive(Debug)]
pub struct FieldEncodeOptions {
    field: PointField,
    factor: f64,
}

impl FieldEncodeOptions {
    pub fn new(field: PointField, precision: u32) -> Self {
        Self {
            field,
            factor: f64::from(10_u32.pow(precision)),
        }
    }
}

fn scale(n: f64, factor: f64) -> i64 {
    (n * factor).round() as i64
}

fn encode(current: f64, previous: f64, factor: f64) -> Result<String, ()> {
    let current_scaled = scale(current, factor);
    let previous_scaled = scale(previous, factor);
    let diff = current_scaled - previous_scaled;
    let mut v = diff << 1;
    if diff < 0 {
        v = !v;
    }

    let mut output = String::new();
    while v >= 0x20 {
        let from_char = char::from_u32(((0x20 | (v & 0x1f)) + 63) as u32).ok_or(())?;
        output.push(from_char);
        v >>= 5;
    }
    let from_char = char::from_u32((v + 63) as u32).ok_or(())?;
    output.push(from_char);
    Ok(output)
}

pub fn polyline_encode(points: &[Point], fields: &[FieldEncodeOptions]) -> Result<String, ()> {
    let mut output = String::new();
    let mut prev = &Point::default();

    for point in points {
        for field in fields {
            match field.field {
                PointField::Y => output.push_str(&encode(point.y, prev.y, field.factor)?),
                PointField::X => output.push_str(&encode(point.x, prev.x, field.factor)?),
                PointField::D => {} //output.push_str(&encode(point.d, prev.d, field.factor)?),
                PointField::E => output.push_str(&encode(point.e, prev.e, field.factor)?),
                PointField::S {
                    default_surface_id,
                    default_road_class_id,
                } => {
                    output.push_str(&encode(
                        f64::from(
                            i32::try_from(point.s.unwrap_or(default_surface_id)).unwrap_or(0),
                        ),
                        f64::from(i32::try_from(prev.s.unwrap_or(default_surface_id)).unwrap_or(0)),
                        field.factor,
                    )?);
                    output.push_str(&encode(
                        f64::from(
                            i32::try_from(point.r.unwrap_or(default_road_class_id)).unwrap_or(0),
                        ),
                        f64::from(
                            i32::try_from(prev.r.unwrap_or(default_road_class_id)).unwrap_or(0),
                        ),
                        field.factor,
                    )?);
                }
            }
        }

        prev = point;
    }

    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_polyline() {
        let fields = vec![
            FieldEncodeOptions::new(PointField::Y, 5),
            FieldEncodeOptions::new(PointField::X, 5),
        ];
        assert_eq!(
            polyline_encode(
                &vec![
                    Point {
                        x: -120.200,
                        y: 38.500,
                        ..Default::default()
                    },
                    Point {
                        x: -120.950,
                        y: 40.700,
                        ..Default::default()
                    },
                    Point {
                        x: -126.453,
                        y: 43.252,
                        ..Default::default()
                    }
                ],
                &fields
            ),
            Ok("_p~iF~ps|U_ulLnnqC_mqNvxq`@".to_string())
        );
    }
}
