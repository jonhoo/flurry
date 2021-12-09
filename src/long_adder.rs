use crossbeam_epoch::{Atomic, Guard, Owned, Shared};
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::sync::atomic::{AtomicBool, AtomicIsize, Ordering};

#[derive(Default)]
pub(crate) struct LongAdder {
    base: AtomicIsize,
    cells: Atomic<Vec<AtomicIsize>>,
    cells_busy: AtomicBool,
}

impl LongAdder {
    pub(crate) fn add(&self, value: isize, guard: &Guard) {
        let cells = self.cells.load(Ordering::SeqCst, guard);
        let base = self.base.load(Ordering::SeqCst);
        if !cells.is_null()
            || self
                .base
                .compare_exchange(base, base + value, Ordering::SeqCst, Ordering::Relaxed)
                .is_err()
        {
            if cells.is_null() {
                self.long_accumulate(value, true, guard);
                return;
            }

            let c = unsafe { cells.deref() }
                .choose(&mut rand::rngs::SmallRng::from_entropy())
                .unwrap();
            let cv = c.load(Ordering::SeqCst);
            let uncontended = c
                .compare_exchange(cv, cv + value, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok();

            if !uncontended {
                self.long_accumulate(value, uncontended, guard);
            }
        }
    }

    fn long_accumulate(&self, value: isize, mut uncontended: bool, guard: &Guard) {
        let mut collide = false;
        loop {
            let cells = self.cells.load(Ordering::SeqCst, guard);
            if !cells.is_null() {
                if !uncontended {
                    uncontended = true;
                    continue;
                }

                // safety: the cells pointer is valid because when we experience
                // contention the cells are initialized and will not be deallocated
                // until the hashmap is deallocated. cells are never used if contention
                // doesn't atleast happen once.
                let cells = unsafe { cells.deref() };
                let c = cells
                    .choose(&mut rand::rngs::SmallRng::from_entropy())
                    .unwrap();
                let cv = c.load(Ordering::SeqCst);
                if c.compare_exchange(cv, cv + value, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    // we've picked a random cell and incremented its count.
                    break;
                }

                if cells.len() >= crate::map::num_cpus() {
                    collide = false;
                    // prevent cells from growing past cpu max.
                    // this ensures the loop keeps retrying with the max amount of cells
                    // this machine has to its disposal. eventually a cell would free up
                    // and increment its count instead of resizing cells indefinitely.
                    continue;
                }

                if !collide {
                    collide = true;
                    continue;
                }

                if self
                    .cells_busy
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    // the selected cell had contention, increase the number of cells!
                    let new_len = cells.len() << 1;

                    let mut new_cells = Vec::with_capacity(new_len);

                    for cell in 0..new_len {
                        match cells.get(cell) {
                            Some(old_cell) => {
                                new_cells.push(AtomicIsize::new(old_cell.load(Ordering::SeqCst)))
                            }
                            None => new_cells.push(AtomicIsize::new(0)),
                        }
                    }

                    let now_garbage =
                        self.cells
                            .swap(Owned::new(new_cells), Ordering::SeqCst, guard);
                    // safety: need to guarantee that now_garbage is no longer
                    // reachable. more specifically, no thread that executes _after_
                    // this line can ever get a reference to now_garbage.
                    //
                    // here are the possible cases:
                    //
                    //  - another thread already has a reference to now_garbage.
                    //    they must have read it before the call to swap.
                    //    because of this, that thread must be pinned to an epoch <=
                    //    the epoch of our guard. since the garbage is placed in our
                    //    epoch, it won't be freed until the _next_ epoch, at which
                    //    point, that thread must have dropped its guard, and with it,
                    //    any reference to `cells`.
                    unsafe { guard.defer_destroy(now_garbage) };

                    self.cells_busy.store(false, Ordering::SeqCst);
                    collide = false;
                    continue;
                }
            } else if cells.is_null()
                && self
                    .cells_busy
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
            {
                self.cells.store(
                    Owned::new(vec![AtomicIsize::new(value), AtomicIsize::new(0)]),
                    Ordering::SeqCst,
                );
                self.cells_busy.store(false, Ordering::SeqCst);
                break;
            } else {
                // fall back on using base
                let b = self.base.load(Ordering::SeqCst);
                if self
                    .base
                    .compare_exchange(b, b + value, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
                {
                    break;
                }
            }
        }
    }

    pub(crate) fn sum(&self, guard: &Guard) -> isize {
        let cells = self.cells.load(Ordering::SeqCst, guard);
        if cells.is_null() {
            return self.base.load(Ordering::SeqCst);
        }

        // safety: the cells pointer is valid because when we experience
        // contention the cells are initialized and will not be deallocated
        // until the hashmap is deallocated. cells are never used if contention
        // doesn't atleast happen once.
        let count: isize = unsafe { cells.deref() }
            .iter()
            .map(|c| c.load(Ordering::SeqCst))
            .sum();

        self.base.load(Ordering::SeqCst) + count
    }
}

impl Drop for LongAdder {
    fn drop(&mut self) {
        // safety: we have &mut self _and_ all references we have returned are bound to the
        // lifetime of their borrow of self, so there cannot be any outstanding references.
        let guard = unsafe { crossbeam_epoch::unprotected() };

        let cells = self.cells.swap(Shared::null(), Ordering::SeqCst, guard);
        if cells.is_null() {
            // cells were never allocated!
            return;
        }

        // safety: same as above + we own the long adder.
        drop(unsafe { cells.into_owned() });
    }
}