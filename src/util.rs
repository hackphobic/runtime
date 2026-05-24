use std::time::Duration;

pub(crate) const DEFAULT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

pub(crate) fn dedup_preserve_order(mut v: Vec<String>) -> Vec<String> {
    use std::collections::HashSet;
    let mut seen = HashSet::new();
    v.retain(|x| seen.insert(x.clone()));
    v
}