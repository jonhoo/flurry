use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
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

/* HASHBROWN:
 *
 * This benchmark suite contains some benchmarks along a set of dimensions:
 *   Int key distribution: low bit heavy, top bit heavy, and random.
 *   Task: basic functionality: insert, insert_erase, lookup, lookup_fail, iter
 */
const SIZE: usize = 1000;

#[derive(Clone, Copy)]
struct RandomKeys {
    state: usize,
}

impl RandomKeys {
    fn new() -> Self {
        RandomKeys { state: 0 }
    }
}

impl Iterator for RandomKeys {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        // Add 1 then multiply by some 32 bit prime.
        self.state = self.state.wrapping_add(1).wrapping_mul(3_787_392_781);
        Some(self.state)
    }
}

macro_rules! bench_suite {
    ($bench_macro:ident, $bench_fn_name:ident, $group_name:expr $(,)?) => {
        fn $bench_fn_name(c: &mut Criterion) {
            let mut group = c.benchmark_group($group_name);
            group.throughput(Throughput::Elements(SIZE as u64));

            $bench_macro!(group, 0.., "low_guard_once");
            $bench_macro!(group, (0..).map(usize::swap_bytes), "high_guard_once");
            $bench_macro!(group, RandomKeys::new(), "random_guard_once");

            group.finish();
        }
    };
}

macro_rules! bench_insert {
    ($group:ident, $keydist:expr, $bench_id: expr) => {
        $group.bench_function(BenchmarkId::from_parameter($bench_id), |b| {
            b.iter(|| {
                let mut map = HashMap::with_capacity(SIZE as usize);

                let guard = epoch::pin();
                ($keydist).take(SIZE).for_each(|i| {
                    map.insert(i, i, &guard);
                });
                black_box(&mut map);
            });
        });
    };
}

bench_suite!(
    bench_insert,
    insert_flurry_hashbrown,
    "insert_flurry_hashbrown",
);

macro_rules! bench_insert_erase {
    ($group:ident, $keydist:expr, $bench_id: expr) => {
        let base = HashMap::with_capacity(SIZE as usize);
        {
            // NOTE: in testing, I tried running this without the local scope.
            // not dropping the guard and pinning the epoch for the entire benchmark literally
            // crashed multiple programs on my PC, so I advise not to do that...
            let guard = epoch::pin();
            ($keydist).take(SIZE).for_each(|i| {
                base.insert(i, i, &guard);
            });
        }
        let skip = ($keydist).take(SIZE);

        $group.bench_function(BenchmarkId::from_parameter($bench_id), |b| {
            b.iter(|| {
                let mut map = base.clone();
                let mut add_iter = skip.clone();
                let mut remove_iter = $keydist;

                // While keeping the size constant,
                // replace the first keydist with the second.
                let guard = epoch::pin();
                (&mut add_iter)
                    .zip(&mut remove_iter)
                    .take(SIZE)
                    .for_each(|(add, remove)| {
                        map.insert(add, add, &guard);
                        black_box(map.remove(&remove, &guard));
                    });
                black_box(&mut map);
            });
        });
    };
}

bench_suite!(
    bench_insert_erase,
    insert_erase_flurry_hashbrown,
    "insert_erase_flurry_hashbrown",
);

macro_rules! bench_lookup {
    ($group:ident, $keydist:expr, $bench_id: expr) => {
        let map = HashMap::with_capacity(SIZE as usize);
        {
            // see bench_insert_erase for a comment on the local scope
            let guard = epoch::pin();
            ($keydist).take(SIZE).for_each(|i| {
                map.insert(i, i, &guard);
            });
        }

        $group.bench_function(BenchmarkId::from_parameter($bench_id), |b| {
            b.iter(|| {
                let guard = epoch::pin();
                ($keydist).take(SIZE).for_each(|i| {
                    black_box(map.get(&i, &guard));
                });
            });
        });
    };
}

bench_suite!(bench_lookup, get_flurry_hashbrown, "get_flurry_hashbrown",);

macro_rules! bench_lookup_fail {
    ($group:ident, $keydist:expr, $bench_id: expr) => {
        let map = HashMap::with_capacity(SIZE as usize);
        let mut iter = $keydist;
        {
            // see bench_insert_erase for a comment on the local scope
            let guard = epoch::pin();
            (&mut iter).take(SIZE).for_each(|i| {
                map.insert(i, i, &guard);
            });
        }

        $group.bench_function(BenchmarkId::from_parameter($bench_id), |b| {
            b.iter(|| {
                let guard = epoch::pin();
                (&mut iter).take(SIZE).for_each(|i| {
                    black_box(map.get(&i, &guard));
                });
            });
        });
    };
}

bench_suite!(
    bench_lookup_fail,
    get_absent_flurry_hashbrown,
    "get_absent_flurry_hashbrown",
);

macro_rules! bench_iter {
    ($group:ident, $keydist:expr, $bench_id: expr) => {
        let map = HashMap::with_capacity(SIZE as usize);
        {
            // see bench_insert_erase for a comment on the local scope
            let guard = epoch::pin();
            ($keydist).take(SIZE).for_each(|i| {
                map.insert(i, i, &guard);
            });
        }

        $group.bench_function(BenchmarkId::from_parameter($bench_id), |b| {
            b.iter(|| {
                let guard = epoch::pin();
                for k in map.iter(&guard) {
                    black_box(k);
                }
            });
        });
    };
}

bench_suite!(bench_iter, iter_flurry_hashbrown, "iter_flurry_hashbrown",);

criterion_group!(
    benches,
    insert_flurry_u64_u64_guard_every_it,
    get_flurry_u64_u64_guard_every_it,
    insert_flurry_u64_u64_guard_once,
    get_flurry_u64_u64_guard_once,
    insert_flurry_hashbrown,
    insert_erase_flurry_hashbrown,
    get_flurry_hashbrown,
    get_absent_flurry_hashbrown,
    iter_flurry_hashbrown,
);
criterion_main!(benches);
