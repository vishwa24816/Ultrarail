//! Validated boot config. One struct, unknown PAYMENT_* keys rejected,
//! partition-count changes abort with drain instructions (meta.json guard).

use std::path::Path;

use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Config {
    pub partitions: usize,
    pub journal_dir: String,
    pub listen_addr: String,
    pub prom_addr: String,
    pub idem_ttl_secs: u64,
    pub ack_timeout_secs: u64,
    pub rail_timeout_ms: u64,
    pub reconcile_timeout_secs: u64,
    pub shutdown_deadline_secs: u64,
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("bad value for {0}: {1}")]
    BadValue(&'static str, String),
    #[error("unknown config key(s): {0} — typo?")]
    UnknownKeys(String),
    #[error("partition count changed {old} -> {new}: drain journals before changing PARTITIONS")]
    PartitionChange { old: usize, new: usize },
    #[error("io: {0}")]
    Io(String),
}

fn env_u64(key: &'static str, default: u64) -> Result<u64, ConfigError> {
    match std::env::var(key) {
        Ok(s) => s.parse().map_err(|_| ConfigError::BadValue(key, s)),
        Err(_) => Ok(default),
    }
}

fn cpus_half() -> usize {
    std::thread::available_parallelism().map(|n| (n.get() / 2).max(2)).unwrap_or(2)
}

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        // Reject unknown PAYMENT_* keys (typo guard).
        let known = [
            "PAYMENT_PARTITIONS",
            "PAYMENT_JOURNAL_DIR",
            "PAYMENT_LISTEN_ADDR",
            "PAYMENT_PROM_ADDR",
            "PAYMENT_IDEM_TTL_SECS",
            "PAYMENT_ACK_TIMEOUT_SECS",
            "PAYMENT_RAIL_TIMEOUT_MS",
            "PAYMENT_RECONCILE_TIMEOUT_SECS",
            "PAYMENT_SHUTDOWN_DEADLINE_SECS",
            "PAYMENT_TLS_CERT",
            "PAYMENT_TLS_KEY",
            "PAYMENT_TLS_OFF",
            "PAYMENT_BANK_KEYS",
            "PAYMENT_CLIENT_KEYS",
            "PAYMENT_TEST_HOOKS",
            "PAYMENT_PANIC_PARTITION",
            "SUPERVISOR_MAX_CRASHES",
            "SUPERVISOR_BACKOFF_MS",
        ];
        let unknown: Vec<String> = std::env::vars()
            .filter(|(k, _)| k.starts_with("PAYMENT_") && !known.contains(&k.as_str()))
            .map(|(k, _)| k)
            .collect();
        if !unknown.is_empty() {
            return Err(ConfigError::UnknownKeys(unknown.join(",")));
        }
        // Legacy unprefixed keys still honored (PARTITIONS, JOURNAL_DIR, ...).
        let partitions = std::env::var("PAYMENT_PARTITIONS")
            .or_else(|_| std::env::var("PARTITIONS"))
            .ok()
            .map(|s| s.parse().map_err(|_| ConfigError::BadValue("PARTITIONS", s)))
            .transpose()?
            .unwrap_or_else(cpus_half);
        let journal_dir = std::env::var("PAYMENT_JOURNAL_DIR")
            .or_else(|_| std::env::var("JOURNAL_DIR"))
            .unwrap_or_else(|_| "./data".into());
        let get = |new: &str, old: &str, d: String| {
            std::env::var(new).or_else(|_| std::env::var(old)).unwrap_or(d)
        };
        Ok(Self {
            partitions: partitions.max(1),
            journal_dir,
            listen_addr: get("PAYMENT_LISTEN_ADDR", "LISTEN_ADDR", "127.0.0.1:3000".into()),
            prom_addr: get("PAYMENT_PROM_ADDR", "PROM_ADDR", "127.0.0.1:9000".into()),
            idem_ttl_secs: env_u64("IDEM_TTL_SECS", 86400)?,
            ack_timeout_secs: env_u64("ACK_TIMEOUT_SECS", 30)?,
            rail_timeout_ms: env_u64("RAIL_TIMEOUT_MS", 2000)?,
            reconcile_timeout_secs: env_u64("RECONCILE_TIMEOUT_SECS", 300)?,
            shutdown_deadline_secs: env_u64("SHUTDOWN_DEADLINE_SECS", 15)?,
        })
    }

    /// Pin partition count on first boot; abort on change.
    pub fn guard_partitions(&self, dir: &Path) -> Result<(), ConfigError> {
        let meta = dir.join("meta.json");
        if !meta.exists() {
            let body = serde_json::json!({"partitions": self.partitions});
            std::fs::write(&meta, body.to_string()).map_err(|e| ConfigError::Io(e.to_string()))?;
            return Ok(());
        }
        let raw = std::fs::read_to_string(&meta).map_err(|e| ConfigError::Io(e.to_string()))?;
        let v: serde_json::Value =
            serde_json::from_str(&raw).map_err(|e| ConfigError::Io(e.to_string()))?;
        let old = v["partitions"].as_u64().unwrap_or(0) as usize;
        if old != self.partitions {
            return Err(ConfigError::PartitionChange { old, new: self.partitions });
        }
        Ok(())
    }

    pub fn apply_env(&self) {
        // Bridge new validated config into the legacy per-module env reads.
        std::env::set_var("PARTITIONS", self.partitions.to_string());
        std::env::set_var("LISTEN_ADDR", &self.listen_addr);
        std::env::set_var("IDEM_TTL_SECS", self.idem_ttl_secs.to_string());
        std::env::set_var("ACK_TIMEOUT_SECS", self.ack_timeout_secs.to_string());
        std::env::set_var("RAIL_TIMEOUT_MS", self.rail_timeout_ms.to_string());
        std::env::set_var("RECONCILE_TIMEOUT_SECS", self.reconcile_timeout_secs.to_string());
    }
}
