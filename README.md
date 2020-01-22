[![Crates.io](https://img.shields.io/crates/v/flurry.svg)](https://crates.io/crates/flurry)
[![Documentation](https://docs.rs/flurry/badge.svg)](https://docs.rs/flurry/)
[![Build Status](https://dev.azure.com/jonhoo/jonhoo/_apis/build/status/flurry?branchName=master)](https://dev.azure.com/jonhoo/jonhoo/_build/latest?definitionId=15&branchName=master)
[![Codecov](https://codecov.io/github/jonhoo/flurry/coverage.svg?branch=master)](https://codecov.io/gh/jonhoo/flurry)
[![Dependency status](https://deps.rs/repo/github/jonhoo/flurry/status.svg)](https://deps.rs/repo/github/jonhoo/flurry)

A port of Java's [`java.util.concurrent.ConcurrentHashMap`](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html) to Rust.

The port is based on the public domain [source file from JSR166] as of
CVS revision [1.323], and is jointly licensed under MIT and Apache 2.0
to match the [Rust API guidelines]. The Java source files are included
under the `jsr166/` subdirectory for easy reference.

The port was developed as part of a series of [live coding streams]
kicked off by [this tweet].

## TODOs

There are a number of things missing from this port that exist in the
[original Java code](jsr166/src/ConcurrentHashMap.java). Some of these
are indicated by `TODO` blocks in the code, but this is (maybe) a more
complete list. For all of these, PRs are warmly welcome, and I will try
to review them as promptly as I can! Feel free to open draft PRs if you
want suggestions for how to proceed.

 - Add benchmarks for single-core ([from `hashbrown`][hashbrown-bench])
   and concurrent ([from `dashmap`][dashmap-bench]) performance
   measurements.
 - Finish the safety argument for `BinEntry::Moved` (see the `FIXME`
   comment [here][fixme] (and eventually in `remove`)).
 - Use the [sharded counters optimization] (`LongAdder` and
   `CounterCell` [in Java][counters]) in `add_count`.
 - Add [`computeIfAbsent`] and [friends]. I have a suspicion that
   `ReservationNode` could also be used to implement an [`Entry`-API]
   like the one on `std::collections::HashMap`.
 - Implement Java's `tryPresize` method to pre-allocate space for
   the incoming entries in `<FlurryHashMap<K, V>> as Extend<(K, V)>>::extend`.
 - Implement the [`TreeNode` optimization] for large bins. Make sure you
   also read the [implementation notes][tree-impl] on that optimization
   in the big comment in the Java code.
 - Optimize the `FlurryHashMap::extend` method. Note the effect on 
   [initial capacity].
 - Add (optional) serialization and deserialization support.
 - Provide methods that wrap `get`, `insert`, `remove`, and friends so
   that the user does not need to know about `Guard`.

  [hashbrown-bench]: https://github.com/rust-lang/hashbrown/blob/master/benches/bench.rs
  [dashmap-bench]: https://github.com/xacrimon/dashmap/tree/master/benches
  [fixme]: https://github.com/jonhoo/flurry/blob/d3dae0465b37b7f12c4f0d58a16f36fb1d8c1596/src/lib.rs#L492
  [sharded counters optimization]: https://github.com/jonhoo/flurry/blob/d3dae0465b37b7f12c4f0d58a16f36fb1d8c1596/jsr166/src/ConcurrentHashMap.java#L400-L411
  [counters]: https://github.com/jonhoo/flurry/blob/d3dae0465b37b7f12c4f0d58a16f36fb1d8c1596/jsr166/src/ConcurrentHashMap.java#L2296-L2311
  [`computeIfAbsent`]: https://github.com/jonhoo/flurry/blob/d3dae0465b37b7f12c4f0d58a16f36fb1d8c1596/jsr166/src/ConcurrentHashMap.java#L1662
  [friends]: https://github.com/jonhoo/flurry/blob/d3dae0465b37b7f12c4f0d58a16f36fb1d8c1596/jsr166/src/ConcurrentHashMap.java#L1774
  [`Entry`-API]: https://doc.rust-lang.org/std/collections/struct.HashMap.html#method.entry
  [`TreeNode` optimization]: https://github.com/jonhoo/flurry/blob/d3dae0465b37b7f12c4f0d58a16f36fb1d8c1596/jsr166/src/ConcurrentHashMap.java#L327-L339
  [tree-impl]: https://github.com/jonhoo/flurry/blob/d3dae0465b37b7f12c4f0d58a16f36fb1d8c1596/jsr166/src/ConcurrentHashMap.java#L413-L447
  [initial capacity]: https://github.com/jonhoo/flurry/blob/5f93a5514fbc42aeb2b1f4228c097ebd3ea490fe/jsr166/src/ConcurrentHashMap.java#L394-L398
  [numcpu]: https://github.com/jonhoo/flurry/blob/d3dae0465b37b7f12c4f0d58a16f36fb1d8c1596/jsr166/src/ConcurrentHashMap.java#L2397

## License

Licensed under either of

 * Apache License, Version 2.0
   ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license
   ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.

  [source file from JSR166]: http://gee.cs.oswego.edu/dl/concurrency-interest/index.html
  [1.323]: http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/main/java/util/concurrent/ConcurrentHashMap.java?revision=1.323&view=markup
  [Rust API guidelines]: https://rust-lang.github.io/api-guidelines/necessities.html#crate-and-its-dependencies-have-a-permissive-license-c-permissive
  [live coding streams]: https://www.youtube.com/playlist?list=PLqbS7AVVErFj824-6QgnK_Za1187rNfnl
  [this tweet]: https://twitter.com/jonhoo/status/1194969578855714816
  [upstream tests]: https://hg.openjdk.java.net/jdk/jdk13/file/tip/test/jdk/java/util/concurrent/ConcurrentHashMap
