use std::io::Read;
use tracklib2::read::inspect::inspect;

const USAGE: &'static str = "usage: rwtfinspect <filename> <base64 password>";

fn main() -> Result<(), String> {
    let filename = std::env::args().nth(1).ok_or(USAGE)?;
    let key_material: Vec<u8> = std::env::args()
        .nth(2)
        .map(base64::decode)
        .map(|decode_result| decode_result.unwrap_or_default())
        .unwrap_or_default();

    let data = {
        let raw =
            std::fs::read(&filename).map_err(|e| format!("Error opening {filename}: {e:?}"))?;
        let mut decoder = flate2::read::GzDecoder::new(raw.as_slice());
        if decoder.header().is_some() {
            let mut buf = Vec::new();
            decoder
                .read_to_end(&mut buf)
                .map_err(|e| format!("{e:?}"))?;
            buf
        } else {
            raw
        }
    };
    let table = inspect(&data, &key_material)?;
    println!("{table}");
    Ok(())
}
