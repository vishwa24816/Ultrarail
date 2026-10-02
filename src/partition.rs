//! Partitioning: route(scope,key,sender,receiver) -> partition id.
//! Key rule: routing hashes the CLIENT-supplied fields, never the generated tx_id,
//! so a retry always lands on the same partition as the original.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unix minute bucket — coarse enough that retries share it, fine enough to spread load.
pub fn bucket() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() / 60)
        .unwrap_or(0)
}

pub fn route(scope: &str, key: &str, sender: &str, receiver: &str, bucket: u64, n: usize) -> usize {
    let mut h = DefaultHasher::new();
    bucket.hash(&mut h);
    scope.hash(&mut h);
    key.hash(&mut h);
    sender.hash(&mut h);
    receiver.hash(&mut h);
    (h.finish() as usize) % n.max(1)
}

/// Structured tx id: `{bucket}-{sender}-{receiver}-{unique8}`. Sanitized to alnum/_/-/:.
pub fn tx_id(bucket: u64, sender: &str, receiver: &str) -> String {
    let clean = |s: &str| {
        s.chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == ':' { c } else { '_' })
            .take(32)
            .collect::<String>()
    };
    let unique = uuid::Uuid::now_v7().to_string().replace('-', "");
    format!("{}-{}-{}-{}", bucket, clean(sender), clean(receiver), &unique[..8])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn routing_deterministic() {
        let a = route("payments", "k1", "user:1", "m:9", 12345, 8);
        let b = route("payments", "k1", "user:1", "m:9", 12345, 8);
        assert_eq!(a, b);
    }

    #[test]
    fn routing_spreads() {
        let mut counts = HashMap::new();
        for i in 0..1000 {
            let p = route("payments", &format!("k{i}"), &format!("user:{}", i % 100), "m:9", 12345, 8);
            *counts.entry(p).or_insert(0) += 1;
        }
        assert_eq!(counts.len(), 8);
        assert!(counts.values().all(|&c| c > 50), "{counts:?}");
    }

    #[test]
    fn tx_id_embeds_fields() {
        let id = tx_id(999, "user:1", "m:9");
        assert!(id.starts_with("999-user:1-m:9-"), "{id}");
    }
}
