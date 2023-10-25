use std::io::{self, BufRead, IsTerminal, Read};
use tracklib::read::inspect::inspect;

const USAGE: &'static str = r#"rwtfinspect
usage: rwtfinspect <filename>
send a password to stdin
"#;

fn main() -> Result<(), String> {
    let filename = if let Some(filename) = std::env::args().nth(1) {
        filename
    } else {
        println!("{}", USAGE);
        return Err(String::from("usage"));
    };

    let mut stdin = io::stdin().lock();
    let key_material = if stdin.is_terminal() {
        vec![]
    } else {
        let mut password = String::new();
        stdin
            .read_line(&mut password)
            .map_err(|e| format!("Failed to read from stdin: {e:?}"))?;
        hex::decode(password.trim_end()).map_err(|e| format!("Failed to decode hex: {e:?}"))?
    };

    let data = {
        let raw =
            std::fs::read(&filename).map_err(|e| format!("Failed to open {filename}: {e:?}"))?;
        let mut decoder = flate2::read::GzDecoder::new(raw.as_slice());
        if decoder.header().is_some() {
            let mut buf = Vec::new();
            decoder
                .read_to_end(&mut buf)
                .map_err(|e| format!("Failed to decode gzip {filename}: {e:?}"))?;
            buf
        } else {
            raw
        }
    };
    let table = inspect(&data, &key_material)?;
    println!("{table}");

    Ok(())
}
