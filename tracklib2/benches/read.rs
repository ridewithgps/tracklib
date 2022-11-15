use criterion::{criterion_group, criterion_main, Criterion};
use tracklib2::read::section::SectionRead;

fn sum_x_y(input: &[u8]) {
    let track_reader = tracklib2::read::track::TrackReader::new(input).unwrap();
    for section in track_reader.sections() {
        let mut x_accumulator: f64 = 0.0;
        let mut y_accumulator: f64 = 0.0;
        match section {
            tracklib2::read::section::Section::Standard(section) => {
                let schema = tracklib2::schema::Schema::with_fields(vec![
                    tracklib2::schema::FieldDefinition::new("x", tracklib2::schema::DataType::F64 { scale: 7 }),
                    tracklib2::schema::FieldDefinition::new("y", tracklib2::schema::DataType::F64 { scale: 7 }),
                ]);
                let mut section_reader = section.reader_for_schema(&schema).unwrap();
                while let Some(column_iter) = section_reader.open_column_iter() {
                    for field_result in column_iter {
                        let (field_def, maybe_val) = field_result.unwrap();
                        match maybe_val {
                            Some(tracklib2::types::FieldValue::F64(v)) => {
                                if field_def.name() == "x" {
                                    x_accumulator += v;
                                } else {
                                    y_accumulator += v;
                                }
                            }
                            None => {}
                            _ => panic!("Unexpected field type"),
                        }
                    }
                }
            }
            tracklib2::read::section::Section::Encrypted(section) => {
                panic!("Encrypted section during benchmark");
            }
        }
        //println!("x={}, y={}", x_accumulator, y_accumulator);
    }
}

fn collect_x_y(input: &[u8]) {
    let track_reader = tracklib2::read::track::TrackReader::new(input).unwrap();
    for section in track_reader.sections() {
        match section {
            tracklib2::read::section::Section::Standard(section) => {
                let schema = tracklib2::schema::Schema::with_fields(vec![
                    tracklib2::schema::FieldDefinition::new("x", tracklib2::schema::DataType::F64 { scale: 7 }),
                    tracklib2::schema::FieldDefinition::new("y", tracklib2::schema::DataType::F64 { scale: 7 }),
                ]);
                let mut points: Vec<(f64, f64)> = Vec::with_capacity(section.rows());
                let mut section_reader = section.reader_for_schema(&schema).unwrap();
                while let Some(column_iter) = section_reader.open_column_iter() {
                    let row = column_iter
                        .map(|field_result| {
                            let (_field_def, maybe_val) = field_result.unwrap();
                            match maybe_val {
                                Some(tracklib2::types::FieldValue::F64(v)) => v,
                                None => 0.0,
                                _ => panic!("Unexpected field type"),
                            }
                        })
                        .collect::<Vec<_>>();
                    points.push((row[0], row[1]));
                }

                //dbg!(&points);
            }
            tracklib2::read::section::Section::Encrypted(section) => {
                panic!("Encrypted section during benchmark");
            }
        }
    }
}

pub fn criterion_benchmark(c: &mut Criterion) {
    let data = std::fs::read("example.rwtf").unwrap();
    c.bench_function("sum_x_y", |b| b.iter(|| sum_x_y(&data)));
    c.bench_function("collect_x_y", |b| b.iter(|| collect_x_y(&data)));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
