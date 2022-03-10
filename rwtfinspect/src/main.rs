use tracklib2::read::inspect::inspect;

fn main() -> Result<(), String> {
    let filename = std::env::args().nth(1).ok_or_else(|| format!("usage: rwtfinspect <filename>"))?;
    let data = std::fs::read(&filename).map_err(|e| format!("Error opening {filename}: {e:?}"))?;
    let table = inspect(&data)?;
    println!("{table}");
    Ok(())
}
