pub mod api;
pub mod base64_sestring;
pub mod config;
pub mod domain;
pub mod ffxiv;
pub mod infra;
pub mod parse_resolver;
pub mod sestring_ext;
pub mod template;
pub mod web;
pub mod ws;

pub use domain::listing;
pub use domain::listing::container as listing_container;
pub use domain::player;
pub use domain::stats;
pub use infra::fflogs;
pub use infra::mongo;
pub use infra::report_parse;

#[cfg(test)]
mod test;

#[cfg(test)]
mod tests {
    #[test]
    fn library_exports_template_module() {
        let _ = crate::template::listings::ParseDisplay::default();
    }
}
