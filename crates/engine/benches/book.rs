use std::hint::black_box;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use engine::{OrderBook, OrderRequest, Side};

/// Small deterministic PRNG so every run replays the same order flow.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next() % (hi - lo + 1)
    }
}

/// Book with `levels` price levels per side and `per_level` orders each, spread around 10_000.
fn seeded_book(levels: i64, per_level: usize) -> OrderBook {
    let mut book = OrderBook::new();
    for i in 1..=levels {
        for _ in 0..per_level {
            book.submit(OrderRequest::limit(Side::Buy, 10_000 - i, 10)).unwrap();
            book.submit(OrderRequest::limit(Side::Sell, 10_000 + i, 10)).unwrap();
        }
    }
    book
}

fn random_flow(n: usize) -> Vec<OrderRequest> {
    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    (0..n)
        .map(|_| {
            let side = if rng.next() & 1 == 0 { Side::Buy } else { Side::Sell };
            let qty = rng.range(1, 20);
            if rng.range(0, 9) == 0 {
                OrderRequest::market(side, qty)
            } else {
                // Prices straddle the mid so roughly half the flow crosses.
                OrderRequest::limit(side, rng.range(9_950, 10_050) as i64, qty)
            }
        })
        .collect()
}

fn bench_submit(c: &mut Criterion) {
    let mut group = c.benchmark_group("submit");
    group.throughput(Throughput::Elements(1));

    group.bench_function("limit_resting", |b| {
        let mut book = seeded_book(50, 10);
        let mut price = 9_000;
        b.iter(|| {
            price = if price <= 8_000 { 9_000 } else { price - 1 };
            black_box(book.submit(OrderRequest::limit(Side::Buy, price, 1)).unwrap());
        });
    });

    group.bench_function("limit_crossing_single_fill", |b| {
        b.iter_batched_ref(
            || seeded_book(50, 10),
            |book| black_box(book.submit(OrderRequest::limit(Side::Buy, 10_001, 5)).unwrap()),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("market_sweep_10_levels", |b| {
        b.iter_batched_ref(
            || seeded_book(50, 10),
            |book| black_box(book.submit(OrderRequest::market(Side::Sell, 1_000)).unwrap()),
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn bench_cancel(c: &mut Criterion) {
    c.bench_function("cancel_mid_queue", |b| {
        b.iter_batched_ref(
            || {
                let mut book = seeded_book(50, 10);
                let id = book.submit(OrderRequest::limit(Side::Buy, 9_990, 1)).unwrap().order_id;
                book.submit(OrderRequest::limit(Side::Buy, 9_990, 1)).unwrap();
                (book, id)
            },
            |(book, id)| black_box(book.cancel(*id).unwrap()),
            BatchSize::SmallInput,
        );
    });
}

fn bench_mixed_flow(c: &mut Criterion) {
    const N: usize = 100_000;
    let flow = random_flow(N);
    let mut group = c.benchmark_group("mixed_flow");
    group.throughput(Throughput::Elements(N as u64));
    group.sample_size(20);
    group.bench_function("100k_orders", |b| {
        b.iter_batched_ref(
            || seeded_book(50, 4),
            |book| {
                for req in &flow {
                    black_box(book.submit(*req).unwrap());
                }
            },
            BatchSize::LargeInput,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_submit, bench_cancel, bench_mixed_flow);
criterion_main!(benches);
