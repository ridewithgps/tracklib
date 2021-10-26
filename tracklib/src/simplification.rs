use crate::{RWTFile, Column};
use std::collections::BTreeMap;
use itertools::Itertools;

pub fn simplify(rwtf: &RWTFile, tolerance: f64) -> geo::LineString<f64> {
    let empty_longfloat_btree = BTreeMap::new();
    let empty_numbers_btree = BTreeMap::new();
    let empty_base64_btree = BTreeMap::new();

    let columns = rwtf.track_points.columns();
    let x_map = if let Some(x_column) = columns.get("x") {
        match x_column {
            Column::LongFloat(x) => x,
            _ => panic!("unexpected x column type"),
        }
    } else {
        &empty_longfloat_btree
    };

    let y_map = if let Some(y_column) = columns.get("y") {
        match y_column {
            Column::LongFloat(y) => y,
            _ => panic!("unexpected y column type"),
        }
    } else {
        &empty_longfloat_btree
    };

    let t_map = if let Some(t_column) = columns.get("t") {
        match t_column {
            Column::Numbers(t) => t,
            _ => panic!("unexpected t column type"),
        }
    } else {
        &empty_numbers_btree
    };

    let ep_map = if let Some(ep_column) = columns.get("ep") {
        match ep_column {
            Column::Base64(ep) => ep,
            _ => panic!("unexpected ep column type"),
        }
    } else {
        &empty_base64_btree
    };

    let all_keys = x_map.keys().chain(y_map.keys());

    let mut points: Vec<(f64, f64)> = Vec::with_capacity(x_map.len());

    let mut has_ep = false;

    for index in all_keys.sorted().dedup() {
        let x = x_map.get(index);
        let y = y_map.get(index);
        let ep = ep_map.get(index);
        let t = t_map.get(index);

        match (x, y) {
            (Some(x), Some(y)) => match ep {
                Some(_ep) => {
                    has_ep = true;
                }
                None => {
                    points.push((*x, *y));
                }
            },
            _ => {}
        }
    }

    let line: geo::LineString<f64> = points.into();

    use geo::algorithm::simplify::Simplify;

    line.simplify(&tolerance)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simplification() {
        use crate::parse_rwtf;

        let bytes = std::fs::read("42382442.rwtf").unwrap();
        let (_, rwtf) = parse_rwtf(&bytes).unwrap();

        println!("point count before simplification: {}", rwtf.track_points.len());
        let simplified = simplify(&rwtf, 0.00003);

        use geo::algorithm::coords_iter::CoordsIter;
        println!("point count after simplification: {}", simplified.coords_count());


        println!("polyline: {}", polyline::encode_coordinates(simplified, 5).unwrap());
        

    }

}
