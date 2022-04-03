/* Benchmarks from `hashbrown` (https://github.com/rust-lang/hashbrown),
 * adapted to flurry for comparison:
 *
 * This benchmark suite contains some benchmarks along a set of dimensions:
 *   Int key distribution: low bit heavy, top bit heavy, and random.
 *   Task: basic functionality: insert, insert_erase, lookup, lookup_fail, iter
 *
 * For the associated license information, please refer to hashbrown.LICENSE.
 */

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use flurry::HashMap;

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
            let map: HashMap<_, _> = HashMap::with_capacity(SIZE as usize);
            b.iter(|| {
                let guard = map.guard();
                map.clear(&guard);
                ($keydist).take(SIZE).for_each(|i| {
                    map.insert(i, i, &guard);
                });
                black_box(&map);
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
        let base: HashMap<_, _> = HashMap::with_capacity(SIZE as usize);
        {
            // NOTE: in testing, I tried running this without the local scope.
            // not dropping the guard and pinning the epoch for the entire benchmark literally
            // crashed multiple programs on my PC, so I advise not to do that...
            let guard = base.guard();
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
                let guard = map.guard();
                (&mut add_iter)
                    .zip(&mut remove_iter)
                    .take(SIZE)
                    .for_each(|(add, remove)| {
                        map.insert(add, add, &guard);
                        black_box(map.remove(&remove, &guard));
                    });
                drop(guard);
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
        let map: HashMap<_, _> = HashMap::with_capacity(SIZE as usize);
        {
            // see bench_insert_erase for a comment on the local scope
            let guard = map.guard();
            ($keydist).take(SIZE).for_each(|i| {
                map.insert(i, i, &guard);
            });
        }

        $group.bench_function(BenchmarkId::from_parameter($bench_id), |b| {
            b.iter(|| {
                let guard = map.guard();
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
        let map: HashMap<_, _> = HashMap::with_capacity(SIZE as usize);
        let mut iter = $keydist;
        {
            // see bench_insert_erase for a comment on the local scope
            let guard = map.guard();
            (&mut iter).take(SIZE).for_each(|i| {
                map.insert(i, i, &guard);
            });
        }

        $group.bench_function(BenchmarkId::from_parameter($bench_id), |b| {
            b.iter(|| {
                let guard = map.guard();
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
        let map: HashMap<_, _> = HashMap::with_capacity(SIZE as usize);
        {
            // see bench_insert_erase for a comment on the local scope
            let guard = map.guard();
            ($keydist).take(SIZE).for_each(|i| {
                map.insert(i, i, &guard);
            });
        }

        $group.bench_function(BenchmarkId::from_parameter($bench_id), |b| {
            b.iter(|| {
                let guard = map.guard();
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
    insert_flurry_hashbrown,
    insert_erase_flurry_hashbrown,
    get_flurry_hashbrown,
    get_absent_flurry_hashbrown,
    iter_flurry_hashbrown,
);
criterion_main!(benches);
