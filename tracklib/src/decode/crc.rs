#[derive(Debug)]
pub enum CRC<T> {
    Valid(T),
    Invalid{expected: T, received: T},
}

impl<T: PartialEq> CRC<T> {
    pub(crate) fn new(expected: T, received: T) -> Self {
        if expected == received {
            CRC::Valid(expected)
        } else {
            CRC::Invalid{expected, received}
        }

    }
}
