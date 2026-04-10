#![cfg_attr(
    not(test),
    deny(clippy::unwrap_used, clippy::unreachable, clippy::panic)
)]
#![forbid(unsafe_code)]

pub fn git_sha() -> &'static str {
    option_env!("BUILD_GIT_SHA").unwrap_or("unknown")
}

pub fn git_tag() -> &'static str {
    option_env!("BUILD_GIT_TAG").unwrap_or("unknown")
}

pub fn build_id() -> String {
    format!("{} {}", git_sha(), git_tag())
}
