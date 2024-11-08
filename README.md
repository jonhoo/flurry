[![Crates.io](https://img.shields.io/crates/v/flurry.svg)](https://crates.io/crates/flurry)
[![Documentation](https://docs.rs/flurry/badge.svg)](https://docs.rs/flurry/)
[![Codecov](https://codecov.io/github/jonhoo/flurry/coverage.svg?branch=master)](https://codecov.io/gh/jonhoo/flurry)
[![Dependency status](https://deps.rs/repo/github/jonhoo/flurry/status.svg)](https://deps.rs/repo/github/jonhoo/flurry)

A port of Java's [`java.util.concurrent.ConcurrentHashMap`](https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html) to Rust.

The port is based on the public domain [source file from JSR166] as of
CVS revision [1.323], and is jointly licensed under MIT and Apache 2.0
to match the [Rust API guidelines]. The Java source files are included
under the `jsr166/` subdirectory for easy reference.

The port was developed as part of a series of [live coding streams]
kicked off by [this tweet].

## Better Alternatives

Flurry currently suffers performance and memory usage issues under load.
You may wish to consider [papaya] or [dashmap] as alternatives if this is
important to you.

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
  [1.323]: http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/jsr166/src/main/java/util/concurrent/ConcurrentHashMap.java?revision=1.323&view=markup
  [Rust API guidelines]: https://rust-lang.github.io/api-guidelines/necessities.html#crate-and-its-dependencies-have-a-permissive-license-c-permissive
  [live coding streams]: https://www.youtube.com/playlist?list=PLqbS7AVVErFj824-6QgnK_Za1187rNfnl
  [this tweet]: https://twitter.com/jonhoo/status/1194969578855714816
  [upstream tests]: https://hg.openjdk.java.net/jdk/jdk13/file/tip/test/jdk/java/util/concurrent/ConcurrentHashMap
  [papaya]: https://github.com/ibraheemdev/papaya
  [dashmap]: https://github.com/xacrimon/dashmap
