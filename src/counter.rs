use std::sync::atomic::{AtomicIsize, Ordering};

// TODO: finish Java CounterCell port, this is only a bare minimum implementation.
#[derive(Debug)]
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

    pub(crate) fn sum(&self, ordering: Ordering) -> isize {
        let sum: isize = self.cells.iter().map(|c| c.load(ordering)).sum();

        self.base.load(ordering) + sum
    }
}
