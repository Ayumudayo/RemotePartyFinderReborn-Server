use std::fs;
use std::path::{Path, PathBuf};

fn copy_file(from: &Path, to: &Path) -> anyhow::Result<()> {
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::copy(from, to)?;
    Ok(())
}

fn stage_showcase_assets(output_dir: &Path) -> anyhow::Result<()> {
    let assets_src = Path::new("assets");
    let assets_dst = output_dir.join("assets");

    fs::create_dir_all(&assets_dst)?;

    for name in [
        "common.css",
        "listings.css",
        "common.js",
        "translations.js",
        "listings.js",
        "minireset.css",
        "icons.svg",
    ] {
        copy_file(&assets_src.join(name), &assets_dst.join(name))?;
    }

    // Match the server's asset alias routes so the standalone file can load locally.
    copy_file(&assets_src.join("pico.min.css"), &assets_dst.join("pico.css"))?;
    copy_file(&assets_src.join("list.min.js"), &assets_dst.join("list.js"))?;
    copy_file(&assets_src.join("d3.v7.min.js"), &assets_dst.join("d3.js"))?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let output = PathBuf::from(
        remote_party_finder_reborn::dev::showcase::showcase_output_relative_path(),
    );

    let output_dir = output.parent().expect("showcase output should have a parent");
    fs::create_dir_all(output_dir)?;

    let html = remote_party_finder_reborn::dev::showcase::render_showcase_html()?
        .replace("\"/assets/", "\"./assets/");
    stage_showcase_assets(output_dir)?;

    fs::write(&output, html)?;
    println!("Wrote showcase to {}", output.display());
    Ok(())
}
