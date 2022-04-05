use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rayon;
use rayon::prelude::*;
use std::sync::atomic::{AtomicIsize, Ordering};

const ITER: isize = 32 * 1024;

#[derive(Debug)]
struct ConcurrentCounter {
    base: AtomicIsize,
    cells: Vec<AtomicIsize>,
}

impl ConcurrentCounter {
    fn new() -> Self {
        Self {
            base: AtomicIsize::new(0),
            cells: (0..num_cpus::get())
                .into_iter()
                .map(|_| AtomicIsize::new(0))
                .collect(),
        }
    }

    fn add(&self, value: isize) {
        let mut base = self.base.load(Ordering::SeqCst);
        let mut index = base + value;

        loop {
            match self.base.compare_exchange(
                base,
                base + value,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(b) => base = b,
            }

            let c = &self.cells[index as usize % self.cells.len()];
            let cv = c.load(Ordering::SeqCst);
            index += cv;

            if c.compare_exchange(cv, cv + value, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    fn sum(&self, ordering: Ordering) -> isize {
        let sum: isize = self.cells.iter().map(|c| c.load(ordering)).sum();

        self.base.load(ordering) + sum
    }
}

fn atomic_counter(c: &mut Criterion) {
    let mut group = c.benchmark_group("atomic_counter");
    group.throughput(Throughput::Elements(ITER as u64));
    let max = num_cpus::get();

    for threads in 1..=max {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .unwrap();
                pool.install(|| {
                    b.iter(|| {
                        let counter = AtomicIsize::new(0);
                        (0..ITER).into_par_iter().for_each(|_| {
                            counter.fetch_add(1, Ordering::Relaxed);
                        });
                        assert_eq!(ITER, counter.load(Ordering::Relaxed));
                    })
                });
            },
        );
    }

    group.finish();
}

fn concurrent_counter(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_counter");
    group.throughput(Throughput::Elements(ITER as u64));
    let max = num_cpus::get();

    for threads in 1..=max {
        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .unwrap();
                pool.install(|| {
                    b.iter(|| {
                        let counter = ConcurrentCounter::new();
                        (0..ITER).into_par_iter().for_each(|_| {
                            counter.add(1);
                        });
                        assert_eq!(ITER, counter.sum(Ordering::Relaxed));
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    atomic_counter,
    concurrent_counter
);
criterion_main!(benches);
