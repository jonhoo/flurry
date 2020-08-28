use flurry::{epoch::pin, *};
use rand::{thread_rng, Rng};

#[test]
fn issue90() {
    #[cfg(not(miri))]
    const ITERATIONS: usize = 100_000;
    #[cfg(miri)]
    const ITERATIONS: usize = 100;

    let mut rng = thread_rng();
    let map = HashMap::new();
    let g = pin();
    for _ in 0..ITERATIONS {
        let el = rng.gen_range(0, 1000);
        let _ = map.try_insert(el, el, &g);
    }
}
