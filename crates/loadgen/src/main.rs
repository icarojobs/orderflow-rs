//! Closed-loop load generator: `concurrency` workers each keep one PlaceOrder in
//! flight, spread over `connections` HTTP/2 channels, until `orders` are sent.
//! Latency is measured per request on the client side (full gRPC round trip).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::Parser;
use hdrhistogram::Histogram;
use proto::order_gateway_client::OrderGatewayClient;
use proto::{OrderType, PlaceOrderRequest, Side};
use tonic::transport::{Channel, Endpoint};

#[derive(Debug, Parser)]
#[command(about = "Load generator for the orderflow gateway")]
struct Args {
    /// Gateway gRPC endpoint.
    #[arg(long, env = "LOADGEN_ADDR", default_value = "http://127.0.0.1:50051")]
    addr: String,
    /// Orders to send (after warm-up).
    #[arg(long, default_value_t = 100_000)]
    orders: u64,
    /// Orders sent before measuring, to warm connections and the book.
    #[arg(long, default_value_t = 5_000)]
    warmup: u64,
    /// Concurrent in-flight requests.
    #[arg(long, default_value_t = 64)]
    concurrency: usize,
    /// HTTP/2 connections shared by the workers.
    #[arg(long, default_value_t = 4)]
    connections: usize,
    #[arg(long, default_value = "BTC-USD")]
    symbol: String,
    /// Share of market orders, in percent; the rest are limit orders around the mid.
    #[arg(long, default_value_t = 10)]
    market_pct: u64,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn random_order(rng: &mut XorShift, symbol: &str, market_pct: u64) -> PlaceOrderRequest {
    let side = if rng.below(2) == 0 { Side::Buy } else { Side::Sell };
    let quantity = 1 + rng.below(20);
    let (order_type, price) = if rng.below(100) < market_pct {
        (OrderType::Market, 0)
    } else {
        // Prices straddle the mid so a good share of limit orders cross.
        (OrderType::Limit, 9_950 + rng.below(101) as i64)
    };
    PlaceOrderRequest {
        symbol: symbol.to_string(),
        side: side.into(),
        order_type: order_type.into(),
        price,
        quantity,
    }
}

struct WorkerResult {
    latencies: Histogram<u64>,
    errors: u64,
}

async fn worker(
    mut client: OrderGatewayClient<Channel>,
    next: Arc<AtomicU64>,
    limit: u64,
    mut rng: XorShift,
    args: Arc<Args>,
) -> anyhow::Result<WorkerResult> {
    // 1µs .. 60s with 3 significant digits.
    let mut latencies = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let mut errors = 0;
    while next.fetch_add(1, Ordering::Relaxed) < limit {
        let order = random_order(&mut rng, &args.symbol, args.market_pct);
        let started = Instant::now();
        let result = client.place_order(order).await;
        let micros = started.elapsed().as_micros() as u64;
        match result {
            Ok(_) => latencies.saturating_record(micros.max(1)),
            Err(_) => errors += 1,
        }
    }
    Ok(WorkerResult { latencies, errors })
}

async fn run_phase(
    clients: &[OrderGatewayClient<Channel>],
    args: &Arc<Args>,
    orders: u64,
    seed: u64,
) -> anyhow::Result<(Histogram<u64>, u64, Duration)> {
    let next = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let mut tasks = Vec::with_capacity(args.concurrency);
    for i in 0..args.concurrency {
        let client = clients[i % clients.len()].clone();
        let rng = XorShift(seed.wrapping_mul(6364136223846793005).wrapping_add(i as u64 + 1) | 1);
        tasks.push(tokio::spawn(worker(client, next.clone(), orders, rng, args.clone())));
    }

    let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3)?;
    let mut errors = 0;
    for task in tasks {
        let result = task.await??;
        merged.add(&result.latencies)?;
        errors += result.errors;
    }
    Ok((merged, errors, started.elapsed()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Arc::new(Args::parse());
    anyhow::ensure!(args.concurrency > 0 && args.connections > 0, "concurrency and connections must be > 0");

    let mut clients = Vec::with_capacity(args.connections);
    for _ in 0..args.connections {
        let channel = Endpoint::from_shared(args.addr.clone())?
            .tcp_nodelay(true)
            .connect()
            .await
            .with_context(|| format!("connecting to {}", args.addr))?;
        clients.push(OrderGatewayClient::new(channel));
    }

    println!(
        "target={} orders={} warmup={} concurrency={} connections={} market_pct={}",
        args.addr, args.orders, args.warmup, args.concurrency, args.connections, args.market_pct
    );
    if args.warmup > 0 {
        run_phase(&clients, &args, args.warmup, args.seed ^ 0xdead_beef).await?;
    }
    let (latencies, errors, elapsed) = run_phase(&clients, &args, args.orders, args.seed).await?;

    let ok = latencies.len();
    let us = |q: f64| latencies.value_at_quantile(q);
    println!("sent={} ok={ok} errors={errors} elapsed={:.3}s", args.orders, elapsed.as_secs_f64());
    println!("throughput={:.0} orders/s", ok as f64 / elapsed.as_secs_f64());
    println!(
        "latency_us p50={} p90={} p99={} p99.9={} max={} mean={:.1}",
        us(0.50),
        us(0.90),
        us(0.99),
        us(0.999),
        latencies.max(),
        latencies.mean()
    );
    Ok(())
}
