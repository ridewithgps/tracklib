use crate::polyline::{polyline_encode, FieldEncodeOptions};
use crate::surface::{RoadClassId, SurfaceMapping, SurfaceTypeId};
use crate::{Column, Section};
use itertools::Itertools;
use std::collections::{BTreeMap, HashSet};

fn haversine_distance(prev: &Point, x: f64, y: f64) -> f64 {
    // lifted wholesale from https://github.com/georust/geo/blob/2cf153d59072d18054baf4da8bcaf3e0c088a7d8/geo/src/algorithm/haversine_distance.rs
    const MEAN_EARTH_RADIUS: f64 = 6_371_000.0;

    let theta1 = prev.y.to_radians();
    let theta2 = y.to_radians();
    let delta_theta = (y - prev.y).to_radians();
    let delta_lambda = (x - prev.x).to_radians();
    let a = (delta_theta / 2.0).sin().powi(2) + theta1.cos() * theta2.cos() * (delta_lambda / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    MEAN_EARTH_RADIUS * c
}

trait FarthestPoint {
    fn farthest_point(&self) -> (usize, f64);
}

impl FarthestPoint for &[Point] {
    fn farthest_point(&self) -> (usize, f64) {
        let line = Line::new(self.first().unwrap(), self.last().unwrap());

        self.iter()
            .enumerate()
            .take(self.len() - 1) // Don't include the last index
            .skip(1) // Don't include the first index
            .map(|(index, point)| (index, line.distance_2d(&point)))
            .fold(
                (0, 0.0),
                |(farthest_index, farthest_dist), (index, distance)| {
                    if distance > farthest_dist {
                        (index, distance)
                    } else {
                        (farthest_index, farthest_dist)
                    }
                },
            )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Point {
    pub(crate) index: usize,
    pub(crate) x: f64,
    pub(crate) y: f64,
    pub(crate) d: f64,
    pub(crate) e: f64,
    pub(crate) s: Option<SurfaceTypeId>,
    pub(crate) r: Option<RoadClassId>,
}

impl Default for Point {
    fn default() -> Self {
        Self {
            index: 0,
            x: 0.0,
            y: 0.0,
            d: 0.0,
            e: 0.0,
            s: None,
            r: None,
        }
    }
}

struct Line<'a> {
    start: &'a Point,
    end: &'a Point,
}

impl<'a> Line<'a> {
    fn new(start: &'a Point, end: &'a Point) -> Self {
        Self { start, end }
    }

    fn distance_3d(&self, point: &Point) -> f64 {
        let mut x = self.start.x;
        let mut y = self.start.y;
        let mut e = self.start.e;

        let mut dx = self.end.x - x;
        let mut dy = self.end.y - y;
        let mut de = self.end.e - e;

        if dx != 0.0 || dy != 0.0 || de != 0.0 {
            let t = ((point.x - x) * dx + (point.y - y) * dy + (point.e - e) * de)
                / (dx * dx + dy * dy + de * de);

            if t > 1.0 {
                x = self.end.x;
                y = self.end.y;
                e = self.end.e;
            } else if t > 0.0 {
                x += dx * t;
                y += dy * t;
                e += de * t;
            }
        }

        dx = point.x - x;
        dy = point.y - y;
        de = point.e - e;

        return dx * dx + dy * dy + de * de;
    }

    fn distance_2d(&self, point: &Point) -> f64 {
        let mut x = self.start.x;
        let mut y = self.start.y;

        let mut dx = self.end.x - x;
        let mut dy = self.end.y - y;

        if dx != 0.0 || dy != 0.0 {
            let t = ((point.x - x) * dx + (point.y - y) * dy) / (dx * dx + dy * dy);

            if t > 1.0 {
                x = self.end.x;
                y = self.end.y;
            } else if t > 0.0 {
                x += dx * t;
                y += dy * t;
            }
        }

        dx = point.x - x;
        dy = point.y - y;

        return dx * dx + dy * dy;
    }
}

struct SurfaceGroupIter<'a, 'b> {
    points: &'a [Point],
    mapping: &'b SurfaceMapping,
    group: Option<&'b String>,
}

impl<'a, 'b> SurfaceGroupIter<'a, 'b> {
    fn new(points: &'a [Point], mapping: &'b SurfaceMapping) -> Self {
        Self {
            points,
            mapping,
            group: points
                .first()
                .and_then(|point| mapping.get_surface_group(point)),
        }
    }
}

impl<'a, 'b> Iterator for SurfaceGroupIter<'a, 'b> {
    type Item = &'a [Point];

    fn next(&mut self) -> Option<Self::Item> {
        let mut partition_len = 0;
        for point in self.points.iter() {
            let group = self.mapping.get_surface_group(point);

            if self.group == group {
                partition_len += 1;
            } else {
                self.group = group;
                break;
            }
        }

        if partition_len > 0 {
            let (partition, new_points) = self.points.split_at(partition_len);
            self.points = new_points;
            Some(partition)
        } else {
            None
        }
    }
}

fn simplify_points(points: &[Point], mapping: &SurfaceMapping, tolerance: f64) -> HashSet<usize> {
    fn stack_rdp(points: &[Point], tolerance_sq: f64) -> HashSet<usize> {
        let mut anchors = HashSet::new();
        let mut stack = Vec::new();
        stack.push(points);

        while let Some(slice) = stack.pop() {
            let (farthest_index, farthest_dist) = slice.farthest_point();

            if farthest_dist > tolerance_sq {
                stack.push(&slice[..=farthest_index]);
                stack.push(&slice[farthest_index..]);
            } else {
                anchors.insert(slice.first().unwrap().index);
                anchors.insert(slice.last().unwrap().index);
            }
        }

        anchors
    }

    let tolerance_sq = tolerance * tolerance;
    SurfaceGroupIter::new(points, mapping)
        .map(|points| stack_rdp(points, tolerance_sq))
        .flatten()
        .collect()
}

fn section_to_points(section: &Section) -> Vec<Point> {
    let empty_longfloat_btree = BTreeMap::new();
    let empty_numbers_btree = BTreeMap::new();
    let empty_base64_btree = BTreeMap::new();

    let columns = section.columns();
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

    let e_map = if let Some(e_column) = columns.get("e") {
        match e_column {
            Column::LongFloat(e) => e,
            _ => panic!("unexpected e column type"),
        }
    } else {
        &empty_longfloat_btree
    };

    let s_map = if let Some(s_column) = columns.get("S") {
        match s_column {
            Column::Numbers(s) => s,
            _ => panic!("unexpected S column type"),
        }
    } else {
        &empty_numbers_btree
    };

    let r_map = if let Some(r_column) = columns.get("R") {
        match r_column {
            Column::Numbers(r) => r,
            _ => panic!("unexpected R column type"),
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

    let mut points: Vec<Point> = Vec::with_capacity(x_map.len());

    let mut point_index = 0;
    for index in all_keys.sorted().dedup() {
        let x = x_map.get(index);
        let y = y_map.get(index);
        let ep = ep_map.get(index);
        let e = e_map.get(index);
        let s = s_map.get(index);
        let r = r_map.get(index);

        if let (Some(x), Some(y), Some(e), None) = (x, y, e, ep) {
            let d = if let Some(prev) = points.last() {
                prev.d + haversine_distance(prev, *x, *y)
            } else {
                0.0
            };

            points.push(Point {
                index: point_index,
                x: *x,
                y: *y,
                d,
                e: *e,
                s: s.cloned(),
                r: r.cloned(),
            });
            point_index += 1;
        }
    }

    points
}

pub(crate) fn simplify_and_encode(
    section: &Section,
    mapping: &SurfaceMapping,
    tolerance: f64,
    fields: &[FieldEncodeOptions],
) -> String {
    let points = section_to_points(section);
    let simplified_indexes = simplify_points(&points, mapping, tolerance);
    let simplified_points = simplified_indexes
        .into_iter()
        .sorted()
        .map(|index| points[index].clone())
        .collect::<Vec<_>>();

    polyline_encode(&simplified_points, fields).unwrap()
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use std::iter::FromIterator;
    use crate::{Section, SectionType};

    use super::*;

    #[test]
    fn test_surface_group_iterator_all_points_missing_surface_info() {
        let mut mapping = SurfaceMapping::new(99);
        mapping.add_surface(0, "0".to_string());
        mapping.add_surface(1, "1".to_string());
        mapping.add_surface(2, "2".to_string());

        let points = vec![
            Point {
                s: None,
                r: None,
                ..Default::default()
            },
            Point {
                s: None,
                r: None,
                ..Default::default()
            },
            Point {
                s: None,
                r: None,
                ..Default::default()
            },
        ];

        let groups = SurfaceGroupIter::new(&points, &mapping).collect::<Vec<_>>();

        assert_eq!(groups, vec![points.as_slice()]);
    }

    #[test]
    fn test_surface_group_iterator_all_points_different_surface() {
        let mut mapping = SurfaceMapping::new(99);
        mapping.add_surface(0, "0".to_string());
        mapping.add_surface(1, "1".to_string());
        mapping.add_surface(2, "2".to_string());

        let points = vec![
            Point {
                s: Some(1),
                r: None,
                ..Default::default()
            },
            Point {
                s: Some(2),
                r: None,
                ..Default::default()
            },
            Point {
                s: Some(3),
                r: None,
                ..Default::default()
            },
        ];

        let groups = SurfaceGroupIter::new(&points, &mapping).collect::<Vec<_>>();

        assert_eq!(
            groups,
            vec![
                &[points[0].clone()][..],
                &[points[1].clone()][..],
                &[points[2].clone()][..]
            ]
        );
    }

    #[test]
    fn test_surface_group_iterator_normal_track() {
        let mut mapping = SurfaceMapping::new(99);
        mapping.add_surface(0, "0".to_string());
        mapping.add_surface(1, "1".to_string());
        mapping.add_surface(2, "2".to_string());

        let points = vec![
            Point {
                s: None,
                r: None,
                ..Default::default()
            },
            Point {
                s: Some(1),
                r: None,
                ..Default::default()
            },
            Point {
                s: Some(1),
                r: None,
                ..Default::default()
            },
            Point {
                s: Some(1),
                r: None,
                ..Default::default()
            },
            Point {
                s: Some(2),
                r: None,
                ..Default::default()
            },
            Point {
                s: Some(2),
                r: None,
                ..Default::default()
            },
            Point {
                s: None,
                r: None,
                ..Default::default()
            },
        ];

        let groups = SurfaceGroupIter::new(&points, &mapping).collect::<Vec<_>>();

        assert_eq!(
            groups,
            vec![
                &[points[0].clone()][..],
                &[points[1].clone(), points[2].clone(), points[3].clone()][..],
                &[points[4].clone(), points[5].clone()][..],
                &[points[6].clone()][..]
            ]
        );
    }

    #[test]
    fn test_simplifying_zero_points() {
        let mapping = SurfaceMapping::new(0);
        assert_eq!(simplify_points(&[], &mapping, 0.0), HashSet::new());
    }

    #[test]
    fn test_simplifying_one_point() {
        let mapping = SurfaceMapping::new(0);
        assert_eq!(
            simplify_points(&[Point::default()], &mapping, 0.0),
            HashSet::from_iter([0])
        );
    }

    #[test]
    fn test_simplifying_two_points() {
        let mapping = SurfaceMapping::new(0);
        assert_eq!(
            simplify_points(
                &[
                    Point::default(),
                    Point {
                        index: 1,
                        x: 1.0,
                        ..Default::default()
                    }
                ],
                &mapping,
                0.0
            ),
            HashSet::from_iter([0, 1])
        );
    }

    #[test]
    fn test_simplifying_three_points() {
        let mapping = SurfaceMapping::new(0);
        assert_eq!(
            simplify_points(
                &[
                    Point::default(),
                    Point {
                        index: 1,
                        x: 1.0,
                        ..Default::default()
                    },
                    Point {
                        index: 2,
                        x: 2.0,
                        y: 2.0,
                        ..Default::default()
                    }
                ],
                &mapping,
                0.0
            ),
            HashSet::from_iter([0, 1, 2])
        );
    }

    #[test]
    fn test_section_to_points_compute_distance() {
        let mut s = Section::new(SectionType::TrackPoints);
        assert!(s.add_long_float(0, "x", -122.63074546051908).is_ok());
        assert!(s.add_long_float(0, "y", 45.503551007119015).is_ok());
        assert!(s.add_long_float(0, "e", 1.0).is_ok());

        assert!(s.add_long_float(1, "x", -122.62891022329185).is_ok());
        assert!(s.add_long_float(1, "y", 45.50346836958737).is_ok());
        assert!(s.add_long_float(1, "e", 2.0).is_ok());

        assert!(s.add_long_float(2, "x", -122.62740666028297).is_ok());
        assert!(s.add_long_float(2, "y", 45.503370210451294).is_ok());
        assert!(s.add_long_float(2, "e", 3.0).is_ok());
        assert!(s.add_short_float(2, "d", 3.0).is_ok());

        assert!(s.add_long_float(3, "x", -122.62586624166765).is_ok());
        assert!(s.add_long_float(3, "y", 45.503370178115716).is_ok());
        assert!(s.add_long_float(3, "e", 4.0).is_ok());

        let points = section_to_points(&s);

        assert_eq!(points.len(), 4);
        assert_matches!(points[0], Point{x, y, d, ..} => {
            assert_eq!(x, -122.63074546051908);
            assert_eq!(y, 45.503551007119015);
            assert_eq!(d, 0.0);
        });
        assert_matches!(points[1], Point{x, y, d, ..} => {
            assert_eq!(x, -122.62891022329185);
            assert_eq!(y, 45.50346836958737);
            assert!((143.0 - d).abs() < 1.0);
        });
        assert_matches!(points[2], Point{x, y, d, ..} => {
            assert_eq!(x, -122.62740666028297);
            assert_eq!(y, 45.503370210451294);
            assert!((261.0 - d).abs() < 1.0);
        });
        assert_matches!(points[3], Point{x, y, d, ..} => {
            assert_eq!(x, -122.62586624166765);
            assert_eq!(y, 45.503370178115716);
            assert!((381.0 - d).abs() < 1.0);
        });
    }
}
