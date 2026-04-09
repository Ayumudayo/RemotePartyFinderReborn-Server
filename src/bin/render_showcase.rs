use std::fs;
use std::path::{Path, PathBuf};

const DIRECT_ASSET_FILES: &[&str] = &[
    "common.css",
    "listings.css",
    "common.js",
    "translations.js",
    "listings.js",
    "minireset.css",
    "icons.svg",
];

const ALIAS_ASSET_FILES: &[(&str, &str)] = &[
    ("pico.min.css", "pico.css"),
    ("list.min.js", "list.js"),
    ("d3.v7.min.js", "d3.js"),
];

fn copy_file(from: &Path, to: &Path) -> anyhow::Result<()> {
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent)?;
    }

    fs::copy(from, to)?;
    Ok(())
}

fn rewrite_asset_urls(html: &str) -> String {
    html.replace("\"/assets/", "\"./assets/")
}

fn stage_showcase_assets(output_dir: &Path, assets_src: &Path) -> anyhow::Result<()> {
    let assets_dst = output_dir.join("assets");

    fs::create_dir_all(&assets_dst)?;

    for name in DIRECT_ASSET_FILES {
        copy_file(&assets_src.join(name), &assets_dst.join(name))?;
    }

    // Match the server's asset alias routes so the standalone file can load locally.
    for (source, target) in ALIAS_ASSET_FILES {
        copy_file(&assets_src.join(source), &assets_dst.join(target))?;
    }

    Ok(())
}

fn write_showcase_bundle(output: &Path, html: &str, assets_src: &Path) -> anyhow::Result<()> {
    let output_dir = output
        .parent()
        .expect("showcase output should have a parent");
    fs::create_dir_all(output_dir)?;
    stage_showcase_assets(output_dir, assets_src)?;
    fs::write(output, rewrite_asset_urls(html))?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let output =
        PathBuf::from(remote_party_finder_reborn::dev::showcase::showcase_output_relative_path());
    let html = remote_party_finder_reborn::dev::showcase::render_showcase_html()?;
    write_showcase_bundle(&output, &html, Path::new("assets"))?;
    println!("Wrote showcase to {}", output.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{rewrite_asset_urls, write_showcase_bundle, ALIAS_ASSET_FILES, DIRECT_ASSET_FILES};
    use std::env;
    use std::fs;
    use std::path::PathBuf;
    use uuid::Uuid;

    fn unique_temp_dir() -> PathBuf {
        env::temp_dir().join(format!("render-showcase-test-{}", Uuid::new_v4()))
    }

    #[test]
    fn rewrite_asset_urls_uses_relative_asset_paths() {
        let original = r#"
            <link rel="stylesheet" href="/assets/pico.css">
            <script src="/assets/list.js"></script>
        "#;
        let rewritten = rewrite_asset_urls(original);

        assert!(rewritten.contains(r#"href="./assets/pico.css""#));
        assert!(rewritten.contains(r#"src="./assets/list.js""#));
        assert!(!rewritten.contains(r#""/assets/"#));
    }

    #[test]
    fn write_showcase_bundle_stages_assets_and_aliases() {
        let temp_root = unique_temp_dir();
        let output = temp_root.join("showcase").join("listings.html");
        let html = remote_party_finder_reborn::dev::showcase::render_showcase_html()
            .expect("showcase html should render");
        let assets_src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets");

        write_showcase_bundle(&output, &html, &assets_src).expect("showcase bundle should write");

        let written_html =
            fs::read_to_string(&output).expect("showcase html should exist after bundling");
        assert!(written_html.contains(r#"href="./assets/pico.css""#));
        assert!(written_html.contains(r#"src="./assets/list.js""#));
        assert!(written_html.contains(r#"./assets/icons.svg#PLD"#));

        let assets_dir = output
            .parent()
            .expect("bundle output should have a parent")
            .join("assets");
        for name in DIRECT_ASSET_FILES {
            assert!(
                assets_dir.join(name).is_file(),
                "expected staged asset file {name} to exist"
            );
        }

        for (_, target) in ALIAS_ASSET_FILES {
            assert!(
                assets_dir.join(target).is_file(),
                "expected staged alias file {target} to exist"
            );
        }

        fs::remove_dir_all(&temp_root).expect("temp showcase bundle should clean up");
    }
}
