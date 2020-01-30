## Benchmarks

Currently, benchmarks following those of [`dashmap`](https://github.com/xacrimon/dashmap/tree/master/benches) and [`hashbrown`](https://github.com/rust-lang/hashbrown/blob/master/benches/bench.rs) are provided. 
To compare against other hashmap implementations, the benchmarks located in the respective repositories may be executed. 
Note that `flurry`, like `dashmap`, uses [`criterion`](https://docs.rs/criterion/0.3.1/criterion/) (and [`rayon`](https://docs.rs/rayon/1.3.0/rayon/) for parallel testing), while `hashbrown` uses [`test::bench`](https://doc.rust-lang.org/test/bench/index.html).

