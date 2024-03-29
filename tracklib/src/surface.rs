use crate::simplify::Point;
use std::collections::HashMap;
use std::fmt;

pub type SurfaceTypeId = i64;
pub type RoadClassId = i64;

#[derive(Clone)]
pub struct RoadClassMapping {
    bbox: [f64; 4],
    map: HashMap<RoadClassId, SurfaceTypeId>,
}

impl RoadClassMapping {
    pub fn new(bbox: [f64; 4]) -> Self {
        Self {
            bbox,
            map: HashMap::new(),
        }
    }

    pub fn add_road_class(&mut self, road_class_id: RoadClassId, surface_id: SurfaceTypeId) {
        self.map.insert(road_class_id, surface_id);
    }

    fn contains(&self, point: &Point) -> bool {
        self.bbox[0] < point.y
            && self.bbox[1] < point.x
            && self.bbox[2] > point.y
            && self.bbox[3] > point.x
    }

    fn lookup(&self, point: &Point) -> Option<&SurfaceTypeId> {
        match (self.contains(point), point.r) {
            (true, Some(r)) => self.map.get(&r),
            _ => None,
        }
    }
}

impl fmt::Debug for RoadClassMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RoadClassMapping<bbox: {:?}, len: {}>", self.bbox, self.map.len())
    }
}

pub struct SurfaceMapping {
    unknown_surface_id: SurfaceTypeId,
    groups: HashMap<SurfaceTypeId, String>,
    road_class_mappings: Vec<RoadClassMapping>,
}

impl SurfaceMapping {
    pub fn new(unknown_surface_id: SurfaceTypeId) -> Self {
        Self {
            unknown_surface_id,
            groups: HashMap::new(),
            road_class_mappings: Vec::new(),
        }
    }

    pub fn add_surface(&mut self, surface_id: SurfaceTypeId, group: String) {
        self.groups.insert(surface_id, group);
    }

    pub fn add_road_class_mapping(&mut self, road_class_mapping: RoadClassMapping) {
        self.road_class_mappings.push(road_class_mapping);
    }

    pub(crate) fn get_surface_group(&self, point: &Point) -> Option<&String> {
        if let Some(point_surface) = point.s {
            if point_surface == self.unknown_surface_id {
                self.road_class_mappings
                    .iter()
                    .find_map(|road_class_mapping| road_class_mapping.lookup(&point))
                    .and_then(|surface_id| self.groups.get(surface_id))
            } else {
                self.groups.get(&point_surface)
            }
        } else {
            None // This point doesn't have a surface type, therefore it doesn't have a group
        }
    }
}

impl fmt::Debug for SurfaceMapping {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SurfaceMapping<unknown_surface_id: {}, groups: {}, road class mappings: {:?}>", self.unknown_surface_id, self.groups.len(), self.road_class_mappings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_surface_groups() {
        let mut mapping = SurfaceMapping::new(95);
        mapping.add_surface(0, "0".to_string());
        mapping.add_surface(1, "1".to_string());
        mapping.add_surface(2, "2".to_string());

        assert_eq!(
            mapping.get_surface_group(&Point {
                s: Some(1),
                r: Some(0),
                ..Default::default()
            }),
            Some(&"1".to_string())
        );

        assert_eq!(
            mapping.get_surface_group(&Point {
                s: Some(50),
                r: Some(0),
                ..Default::default()
            }),
            None
        );
    }

    #[test]
    fn test_road_class_fallback() {
        let mut mapping = SurfaceMapping::new(95);
        mapping.add_surface(0, "0".to_string());
        mapping.add_surface(1, "1".to_string());

        mapping.add_surface(10, "10".to_string());
        mapping.add_surface(11, "11".to_string());
        mapping.add_surface(12, "12".to_string());

        mapping.add_surface(20, "20".to_string());
        mapping.add_surface(21, "21".to_string());
        mapping.add_surface(22, "22".to_string());
        mapping.add_surface(23, "23".to_string());

        // Most specific mapping
        mapping.add_road_class_mapping({
            let mut rc_mapping = RoadClassMapping::new([-1.0, -1.0, 1.0, 1.0]);
            rc_mapping.add_road_class(10, 0);
            rc_mapping.add_road_class(11, 1);
            rc_mapping
        });

        // Mid-specific mapping
        mapping.add_road_class_mapping({
            let mut rc_mapping = RoadClassMapping::new([-10.0, -10.0, 10.0, 10.0]);
            rc_mapping.add_road_class(10, 10);
            rc_mapping.add_road_class(11, 11);
            rc_mapping.add_road_class(12, 12);
            rc_mapping
        });

        // Least specific mapping
        mapping.add_road_class_mapping({
            let mut rc_mapping = RoadClassMapping::new([-90.0, -180.0, 90.0, 180.0]);
            rc_mapping.add_road_class(10, 20);
            rc_mapping.add_road_class(11, 21);
            rc_mapping.add_road_class(12, 22);
            rc_mapping.add_road_class(13, 23);
            rc_mapping
        });

        // Coordinate inside the most specific mapping
        assert_eq!(
            mapping.get_surface_group(&Point {
                x: 0.0,
                y: 0.0,
                s: Some(95),
                r: Some(10),
                ..Default::default()
            }),
            Some(&"0".to_string())
        );

        // Coordinate inside the middle mapping
        assert_eq!(
            mapping.get_surface_group(&Point {
                x: 2.0,
                y: 0.0,
                s: Some(95),
                r: Some(10),
                ..Default::default()
            }),
            Some(&"10".to_string())
        );

        // Coordinate inside the least specific mapping
        assert_eq!(
            mapping.get_surface_group(&Point {
                x: 20.0,
                y: 0.0,
                s: Some(95),
                r: Some(10),
                ..Default::default()
            }),
            Some(&"20".to_string())
        );

        // Coordinate falls into the most specific mapping, but only the least specific one has an entry for this road class
        assert_eq!(
            mapping.get_surface_group(&Point {
                x: 0.0,
                y: 0.0,
                s: Some(95),
                r: Some(13),
                ..Default::default()
            }),
            Some(&"23".to_string())
        );
    }
}
