## Benchmarks

Currently, benchmarks following those of [`dashmap`](https://github.com/xacrimon/dashmap/tree/master/benches) and [`hashbrown`](https://github.com/rust-lang/hashbrown/blob/master/benches/bench.rs) are provided. 
To compare against other hashmap implementations, the benchmarks located in the respective repositories may be executed. 
Note that `flurry`, like `dashmap`, uses [`criterion`](https://docs.rs/criterion/0.3.1/criterion/) (and [`rayon`](https://docs.rs/rayon/1.3.0/rayon/) for parallel testing), while `hashbrown` uses [`test::bench`](https://doc.rust-lang.org/test/bench/index.html).

To run the `flurry` benchmarks, just run

```console
$ cargo bench
```

or

```console
$ cargo bench <BENCHNAME>
```

to only run benches containing `<BENCHNAME>` in their names.

To run the original `dashmap` benchmarks:

```console
$ git clone https://github.com/xacrimon/dashmap.git
$ cd dashmap
$ cargo bench [<BENCHNAME>]
```

To run the original `hashbrown` benchmarks:

```console
$ git clone https://github.com/rust-lang/hashbrown.git
$ cd hashbrown
$ cargo bench [<BENCHNAME>]
```