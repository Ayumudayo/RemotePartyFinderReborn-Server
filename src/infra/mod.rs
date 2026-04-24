//! Infrastructure 레이어 - 외부 시스템 연동
//!
//! - `mongo`: MongoDB 데이터베이스
//! - `fflogs`: FFLogs API 및 캐시

pub mod fflogs;
pub mod mongo;
pub mod report_parse;
