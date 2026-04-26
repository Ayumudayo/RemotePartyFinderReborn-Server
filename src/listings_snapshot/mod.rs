mod lease;
mod materialized;
mod payload;
mod revision;

pub use lease::*;
pub use materialized::*;
pub use payload::*;
pub use revision::*;

fn is_duplicate_key_error(error: &mongodb::error::Error) -> bool {
    error.to_string().contains("E11000") || error.to_string().contains("duplicate key")
}

#[cfg(test)]
mod tests;
