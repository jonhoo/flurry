/* Benchmarks from `dashmap` (https://github.com/xacrimon/dashmap),
 * adapted to flurry for comparison:
 *
 * This benchmark suite contains benchmarks for concurrent insertion
 * and retrieval (get).
 * Currently, this file provides two versions of each test, one which
 * follows the original implementation in using `par_iter().for_each()`,
 * which necessitates creating a new guard for each operation since
 * guards are not `Send + Sync`, and one version which uses threads
 * spawned in scopes. The latter version is able to create only one
 * guard per thread, but incurs overhead from the setup of the more
 * heavyweight threading environment.
 *
 * For the associated license information, please refer to dashmap.LICENSE.
 */

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use flurry::{epoch, HashMap};
use rayon;
use rayon::prelude::*;
use std::sync::Arc;

/* DASHMAP */
const ITER: u64 = 32 * 1024;

fn task_insert_flurry_u64_u64_guard_every_it() -> HashMap<u64, u64> {
    let map = HashMap::with_capacity(ITER as usize);
    (0..ITER).into_par_iter().for_each(|i| {
        let guard = epoch::pin();
        map.insert(i, i + 7, &guard);
    });
    map
}

fn insert_flurry_u64_u64_guard_every_it(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_flurry_u64_u64_guard_every_it");
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
                pool.install(|| b.iter(task_insert_flurry_u64_u64_guard_every_it));
            },
        );
    }

    group.finish();
}

fn task_insert_flurry_u64_u64_guard_once(threads: usize) -> HashMap<u64, u64> {
    let map = Arc::new(HashMap::with_capacity(ITER as usize));
    let inc = ITER / (threads as u64);

    rayon::scope(|s| {
        for t in 1..=(threads as u64) {
            let m = map.clone();
            s.spawn(move |_| {
                let start = t * inc;
                let guard = epoch::pin();
                for i in start..(start + inc) {
                    m.insert(i, i + 7, &guard);
                }
            });
        }
    });
    Arc::try_unwrap(map).unwrap()
}

fn insert_flurry_u64_u64_guard_once(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_flurry_u64_u64_guard_once");
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
                pool.install(|| b.iter(|| task_insert_flurry_u64_u64_guard_once(threads)));
            },
        );
    }

    group.finish();
}

fn task_get_flurry_u64_u64_guard_every_it(map: &HashMap<u64, u64>) {
    (0..ITER).into_par_iter().for_each(|i| {
        let guard = epoch::pin();
        assert_eq!(*map.get(&i, &guard).unwrap(), i + 7);
    });
}

fn get_flurry_u64_u64_guard_every_it(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_flurry_u64_u64_guard_every_it");
    group.throughput(Throughput::Elements(ITER as u64));
    let max = num_cpus::get();

    for threads in 1..=max {
        let map = task_insert_flurry_u64_u64_guard_every_it();

        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .unwrap();
                pool.install(|| b.iter(|| task_get_flurry_u64_u64_guard_every_it(&map)));
            },
        );
    }

    group.finish();
}

fn task_get_flurry_u64_u64_guard_once(threads: usize, map: Arc<HashMap<u64, u64>>) {
    let inc = ITER / (threads as u64);

    rayon::scope(|s| {
        for t in 1..=(threads as u64) {
            let m = map.clone();
            s.spawn(move |_| {
                let start = t * inc;
                let guard = epoch::pin();
                for i in start..(start + inc) {
                    if let Some(&v) = m.get(&i, &guard) {
                        assert_eq!(v, i + 7);
                    }
                }
            });
        }
    });
}

fn get_flurry_u64_u64_guard_once(c: &mut Criterion) {
    let mut group = c.benchmark_group("get_flurry_u64_u64_guard_once");
    group.throughput(Throughput::Elements(ITER as u64));
    let max = num_cpus::get();

    for threads in 1..=max {
        let map = Arc::new(task_insert_flurry_u64_u64_guard_every_it());

        group.bench_with_input(
            BenchmarkId::from_parameter(threads),
            &threads,
            |b, &threads| {
                let pool = rayon::ThreadPoolBuilder::new()
                    .num_threads(threads)
                    .build()
                    .unwrap();
                pool.install(|| {
                    b.iter(|| task_get_flurry_u64_u64_guard_once(threads, map.clone()))
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    insert_flurry_u64_u64_guard_every_it,
    get_flurry_u64_u64_guard_every_it,
    insert_flurry_u64_u64_guard_once,
    get_flurry_u64_u64_guard_once,
);
criterion_main!(benches);
