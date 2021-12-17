use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::sync::atomic::{AtomicIsize, Ordering};

pub(crate) struct ConcurrentCounter {
    base: AtomicIsize,
    cells: Vec<AtomicIsize>,
}

impl ConcurrentCounter {
    pub(crate) fn new() -> Self {
        Self {
            base: AtomicIsize::new(0),
            cells: (0..crate::map::num_cpus())
                .into_iter()
                .map(|_| AtomicIsize::new(0))
                .collect(),
        }
    }

    pub(crate) fn add(&self, value: isize) {
        let base = self.base.load(Ordering::SeqCst);
        if self
            .base
            .compare_exchange(base, base + value, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            // we experienced contention on base
            loop {
                let c = &self
                    .cells
                    .choose(&mut rand::rngs::SmallRng::from_entropy())
                    .unwrap();
                let cv = c.load(Ordering::SeqCst);
                if c.compare_exchange(cv, cv + value, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    // we've successfully incremented a random counter
                    break;
                }

                // the selected counter also experienced contention, retry base
                let b = self.base.load(Ordering::SeqCst);
                if self
                    .base
                    .compare_exchange(b, b + value, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    // the contention on base has subsided, increment was successful
                    break;
                }

                // both base and a random cell experienced contention, retry another cell
            }
        }
    }

    pub(crate) fn sum(&self) -> isize {
        let sum: isize = self.cells.iter().map(|c| c.load(Ordering::SeqCst)).sum();

        self.base.load(Ordering::SeqCst) + sum
    }
}
