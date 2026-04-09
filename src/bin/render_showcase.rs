use std::fs;
use std::path::PathBuf;

fn main() -> anyhow::Result<()> {
    let html = remote_party_finder_reborn::dev::showcase::render_showcase_html()?;
    let output = PathBuf::from(
        remote_party_finder_reborn::dev::showcase::showcase_output_relative_path(),
    );

    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::write(&output, html)?;
    println!("Wrote showcase to {}", output.display());
    Ok(())
}
