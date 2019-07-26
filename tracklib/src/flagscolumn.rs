use std::io::{Write};
use std::cmp;
use snafu::{Snafu, ResultExt};
use std::collections::btree_map::{self, BTreeMap};
use crate::utils::{write};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Couldn't write flags column: {}", source))]
    WriteFlagsColumn{source: std::io::Error},
}

pub type Result<T, E = Error> = std::result::Result<T, E>;


#[derive(Debug)]
pub(crate) struct FlagsColumn {
    pub(crate) fields: BTreeMap<String, usize>,
    pub(crate) data: BTreeMap<usize, u64>,
    pub(crate) max: usize,
}

impl FlagsColumn {
    pub(crate) fn new() -> Self {
        FlagsColumn{fields: BTreeMap::new(),
                    data: BTreeMap::new(),
                    max: 0}
    }

    pub(crate) fn fields(&self) -> Vec<&String> {
        let mut f = self.fields
            .iter()
            .collect::<Vec<(&String, &usize)>>();
        f.sort_by_key(|(_name, index)| *index);

        f.iter()
            .map(|(name, _index)| *name)
            .collect()
    }

    pub(crate) fn len(&self) -> usize {
        if self.max > 0 {
            self.max + 1
        } else {
            if self.data.len() > 0 {
                1
            } else {
                0
            }
        }
    }

    pub(crate) fn max(&self) -> usize {
        self.max
    }

    pub(crate) fn set(&mut self, index: usize, name: &str) {
        let next = self.fields.len();

        // N.B. It would be nicer to use the entry API here, but in
        // this case, that would require allocating a new String for
        // every call to this method, even when the field is already
        // known. So, instead we get this:
        let shift = match self.fields.get(name) {
            Some(id) => *id,
            None => {self.fields.insert(name.into(), next); next}
        };

        match self.data.entry(index) {
            btree_map::Entry::Vacant(entry) => {
                entry.insert(1 << shift);
            }
            btree_map::Entry::Occupied(entry) => {
                *entry.into_mut() |= 1 << shift;
            }
        }

        self.max = cmp::max(self.max, index);
    }

    pub fn is_present(&self, index: usize, name: &str) -> bool {
        if let Some(f) = self.data.get(&index) {
            if let Some(shift) = self.fields.get(name) {
                (*f & (1 << *shift)) > 0
            } else {
                // no fields present with this name
                false
            }
        } else {
            // no fields are present for this index
            false
        }


    }

    fn bytes_required(&self) -> usize {
        (self.fields.len() + 7) / 8
    }

    pub(crate) fn write<W: Write>(&self, out: &mut W) -> Result<usize> {
        let mut written = 0;

        let width = self.bytes_required();
        for i in 0..=self.max {
            let f = *self.data.get(&i).unwrap_or(&0);
            let bytes = &f.to_le_bytes()[..width];
            written += write(out, bytes).context(WriteFlagsColumn{})?;
        }

        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_flag() {
        let mut c = FlagsColumn::new();
        c.set(0, "a");
        c.set(1, "a");
        c.set(2, "a");

        let mut buf = vec![];
        let written = c.write(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x01, 0x01, 0x01];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
        assert_eq!(c.fields(), vec!["a"]);
    }

    #[test]
    fn test_multiple_flags() {
        let mut c = FlagsColumn::new();
        c.set(0, "a");
        c.set(1, "b");
        c.set(2, "c");
        c.set(3, "d");

        let mut buf = vec![];
        let written = c.write(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x01, 0x02, 0x04, 0x08];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
        assert_eq!(c.fields(), vec!["a", "b", "c", "d"]);
    }

    #[test]
    fn test_multiple_bytes() {
        let mut c = FlagsColumn::new();
        c.set(0, "a");
        c.set(0, "b");
        c.set(0, "c");
        c.set(0, "d");
        c.set(0, "e");
        c.set(0, "f");
        c.set(0, "g");
        c.set(0, "h");
        c.set(0, "i");
        c.set(0, "j");

        let mut buf = vec![];
        let written = c.write(&mut buf);
        assert!(written.is_ok());
        let expected = &[0xff, 0x03];
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
        assert_eq!(c.fields(), vec!["a","b","c","d","e","f","g","h","i","j"]);
    }

    #[test]
    fn test_multiple_bytes_and_multiple_rows() {
        let mut c = FlagsColumn::new();
        c.set(0, "a");
        c.set(0, "b");
        c.set(0, "c");
        c.set(0, "d");
        c.set(0, "e");
        c.set(0, "f");
        c.set(0, "g");
        c.set(0, "h");
        c.set(0, "i");
        c.set(0, "j");

        c.set(1, "a");
        c.set(1, "b");
        c.set(1, "j");

        c.set(5, "j");

        let mut buf = vec![];
        let written = c.write(&mut buf);
        assert!(written.is_ok());
        let expected = &[0xff, 0x03, // row 0
                         0x03, 0x02, // row 1
                         0x00, 0x00, // row 2
                         0x00, 0x00, // row 3
                         0x00, 0x00, // row 4
                         0x00, 0x02];// row 5
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
        assert_eq!(c.fields(), vec!["a","b","c","d","e","f","g","h","i","j"]);
    }

    #[test]
    fn test_insert_order_matters() {
        // Insert order #1
        let mut c = FlagsColumn::new();
        c.set(0, "a");
        c.set(0, "b");
        c.set(0, "c");
        c.set(1, "c");

        let mut buf = vec![];
        let written = c.write(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x07,
                         0x04]; // c = 4
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
        assert_eq!(c.fields(), vec!["a", "b", "c"]);

        // Same data, different order
        let mut c = FlagsColumn::new();
        c.set(0, "c");
        c.set(0, "b");
        c.set(0, "a");
        c.set(1, "c");

        let mut buf = vec![];
        let written = c.write(&mut buf);
        assert!(written.is_ok());
        let expected = &[0x07,
                         0x01]; // c = 1
        assert_eq!(buf, expected);
        assert_eq!(written.unwrap(), expected.len());
        assert_eq!(c.fields(), vec!["c", "b", "a"]);
    }
}
