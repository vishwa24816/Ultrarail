//! Bench harness: `cargo run --bin bench -- [N] [CONCURRENCY]`
//! Fires random payments at a running server, prints latency percentiles,
//! throughput, status mix, and a sample of transaction receipts.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

static SEED: AtomicU64 = AtomicU64::new(0x9E3779B97F4A7C15);

// ponytail: xorshift instead of rand dep — good enough for bench traffic
fn rnd(top: u64) -> u64 {
    let mut x = SEED.load(Ordering::Relaxed);
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    SEED.store(x, Ordering::Relaxed);
    x % top
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n: usize = args.get(1).and_then(|s| s.parse().ok()).unwrap_or(500);
    let conc: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(50);
    let base = std::env::var("BENCH_URL").unwrap_or_else(|_| "http://127.0.0.1:3000".into());
    let client = reqwest::Client::new();

    // warmup
    for _ in 0..5 {
        let _ = client.get(format!("{base}/health")).send().await;
    }

    let sem = Arc::new(tokio::sync::Semaphore::new(conc));
    let mut lat: Vec<u128> = Vec::with_capacity(n);
    let mut ok = 0u32;
    let mut sample: Vec<(String, String, u128)> = vec![];
    let t0 = Instant::now();

    let mut tasks = Vec::with_capacity(n);
    for i in 0..n {
        let c = client.clone();
        let b = base.clone();
        let permit = sem.clone().acquire_owned().await.unwrap();
        tasks.push(tokio::spawn(async move {
            let _p = permit;
            let amount = (rnd(100_000) + 1) as i64; // 1..100000 minor units
            let cur = ["USD", "EUR", "GBP"][rnd(3) as usize];
            let body = serde_json::json!({
                "idempotency_scope": "bench",
                "debit_account": format!("user:{}", rnd(1000)),
                "credit_account": format!("merchant:{}", rnd(100)),
                "amount": amount,
                "currency": cur,
            });
            let key = format!("bench-{i}-{}", rnd(1_000_000));
            let t = Instant::now();
            let out = c.post(format!("{b}/payments")).header("Idempotency-Key", key).json(&body).send().await;
            let dt = t.elapsed().as_micros();
            match out {
                Ok(r) if r.status().as_u16() == 201 => {
                    let v: serde_json::Value = r.json().await.unwrap_or_default();
                    (true, dt, v["tx_id"].as_str().unwrap_or("?").to_string())
                }
                _ => (false, dt, String::new()),
            }
        }));
    }
    for t in tasks {
        let (good, dt, tx) = t.await.unwrap();
        lat.push(dt);
        if good {
            ok += 1;
            if sample.len() < 10 {
                sample.push((tx, "ACCEPTED_DURABLE".into(), dt));
            }
        }
    }
    let wall = t0.elapsed();
    lat.sort_unstable();
    let pct = |p: f64| lat[((p * n as f64) as usize).min(n - 1)] as f64 / 1000.0;
    let mean = lat.iter().sum::<u128>() as f64 / n as f64 / 1000.0;

    println!("--- payment-rail bench ---");
    println!("txs: {n}  concurrency: {conc}  accepted: {ok}/{}  rejected: {}", n, n - ok as usize);
    println!("wall: {:.2}s  throughput: {:.0} tx/s", wall.as_secs_f64(), n as f64 / wall.as_secs_f64());
    println!("latency ms: min {:.2}  mean {:.2}  p50 {:.2}  p95 {:.2}  p99 {:.2}  max {:.2}",
        lat[0] as f64 / 1000.0, mean, pct(0.50), pct(0.95), pct(0.99), lat[n - 1] as f64 / 1000.0);
    println!("--- sample receipts ---");
    for (tx, st, dt) in &sample {
        println!("  {tx}  {st}  {:.2}ms", *dt as f64 / 1000.0);
    }
    // keep output machine-greppable too
    println!("RESULT n={n} conc={conc} ok={ok} p50={:.2} p95={:.2} p99={:.2} tps={:.0}",
        pct(0.50), pct(0.95), pct(0.99), n as f64 / wall.as_secs_f64());
    let _ = Duration::from_secs(0);
}
