use super::rust::{PointField, PolylineOption};
use magnus::{wrap, Error, RArray, Ruby};

#[wrap(class = "Tracklib::PolylineOptions", size)]
pub struct PolylineOptions {
    opts: Vec<PolylineOption>,
}

impl PolylineOptions {
    pub fn create(handle: &Ruby, options: RArray) -> Result<Self, Error> {
        let opts = options
            .typecheck::<RArray>()?
            .into_iter()
            .map(|option_array| {
                let field_name = option_array.entry::<String>(0)?;
                let precision = option_array.entry::<u32>(1)?;

                let factor = f64::from(10_u32.pow(precision));

                let field = match field_name.as_str() {
                    "y" => PointField::Y,
                    "x" => PointField::X,
                    "d" => PointField::D,
                    "e" => PointField::E,
                    "S" => {
                        let default = option_array.entry::<u64>(2)?;
                        PointField::S { default }
                    }
                    "R" => {
                        let default = option_array.entry::<u64>(2)?;
                        PointField::R { default }
                    }
                    field_name => {
                        return Err(Error::new(
                            handle.exception_arg_error(),
                            format!("Polyline parameter '{field_name}' is not valid"),
                        ));
                    }
                };

                Ok(PolylineOption::new(field, factor))
            })
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(Self { opts })
    }

    pub(crate) fn inner(&self) -> &[PolylineOption] {
        &self.opts
    }
}
