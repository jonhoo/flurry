use flize::{Collector, Shield};
use std::ops::{Deref, DerefMut};

pub(crate) type Atomic<T> = flize::Atomic<T>;
pub(crate) type Shared<'shield, T> = flize::Shared<'shield, T>;

pub(crate) trait AtomicExt {
    // fn clone<'c, S: Shield<'c>>(&self, shield: &S) -> Self;
    fn dbg(&self) -> String;
}

impl<T> AtomicExt for Atomic<T> {
    // fn clone<'c, S: Shield<'c>>(&self, shield: &S) -> Self {
    //     use std::sync::atomic::Ordering;
    //     Atomic::new(self.load(Ordering::Relaxed, shield))
    // }

    fn dbg(&self) -> String {
        use std::sync::atomic::Ordering;
        // safety: the loaded pointer is not dereferenced
        self.load(Ordering::Relaxed, unsafe { flize::unprotected() })
            .dbg()
    }
}

pub(crate) trait SharedExt {
    type Of;
    fn boxed(value: Self::Of) -> Self;
    fn into_box(self) -> Box<Self::Of>;
    fn dbg(&self) -> String;
}

impl<'shield, T> SharedExt for Shared<'shield, T> {
    type Of = T;

    fn boxed(value: Self::Of) -> Self {
        // safety: our type wrappers ensure we use `flize::NullTag`, i.e. no pointer tags. thus,
        // there are no alignment requirements on `T`.
        unsafe { Shared::from_ptr(Box::into_raw(Box::new(value))) }
    }

    fn into_box(self) -> Box<Self::Of> {
        // safety: safe if the `Shared` is created through `boxed`
        unsafe { Box::from_raw(self.as_ptr()) }
    }

    fn dbg(&self) -> String {
        format!("{:?}", &self.as_ptr())
    }
}

/// A guard that allows accessing a [`HashMap`](flurry::HashMap).
/// Any reference to a contained element retrieved from a map with a guard is tied to that guard's
/// lifetime. This is because the guard's existence will prevent the referenced element from being
/// destroyed if it is removed from the map for as long as the guard is around.
///
/// For more information, please refer to the crate-level documentation.
#[derive(Clone)]
pub struct Guard<'collector, SH> {
    pub(crate) shield: SH,
    pub(crate) collector: &'collector Collector,
}

impl<'c, SH> std::fmt::Debug for Guard<'c, SH> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard")
            .field("collector", &(self.collector as *const _))
            .finish()
    }
}

impl<'collector, SH> Deref for Guard<'collector, SH>
where
    SH: Shield<'collector>,
{
    type Target = SH;

    fn deref(&self) -> &Self::Target {
        &self.shield
    }
}

impl<'collector, SH> DerefMut for Guard<'collector, SH>
where
    SH: Shield<'collector>,
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.shield
    }
}

impl<'collector, SH> Guard<'collector, SH>
where
    SH: Shield<'collector>,
{
    pub(crate) fn new(shield: SH, collector: &'collector Collector) -> Self {
        Self { shield, collector }
    }

    /// # Safety
    /// There must not be any outstanding references to the object referenced by this shared pointer,
    /// and there must be no way to newly obtain such references.
    /// See also `crossbeam`'s `defer_destroy`.
    pub(crate) unsafe fn defer_destroy<T: 'collector>(&self, shared: Shared<'_, T>) {
        let ptr = shared.as_ptr();
        self.retire(move || drop(Box::from_raw(ptr)));
    }
}
