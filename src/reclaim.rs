pub(crate) use seize::{Collector, Guard, Linked};

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::Ordering;
use std::{fmt, ptr};

pub(crate) struct Atomic<T>(seize::AtomicPtr<T>);

impl<T> Atomic<T> {
    pub(crate) fn null() -> Self {
        Self(seize::AtomicPtr::default())
    }

    pub(crate) fn load<'g>(&self, ordering: Ordering, guard: &'g Guard<'_>) -> Shared<'g, T> {
        guard.protect(&self.0, ordering).into()
    }

    pub(crate) fn store(&self, new: Shared<'_, T>, ordering: Ordering) {
        self.0.store(new.ptr, ordering);
    }

    pub(crate) unsafe fn into_box(self) -> Box<Linked<T>> {
        Box::from_raw(self.0.into_inner())
    }

    pub(crate) fn swap<'g>(
        &self,
        new: Shared<'_, T>,
        ord: Ordering,
        _: &'g Guard<'_>,
    ) -> Shared<'g, T> {
        self.0.swap(new.ptr, ord).into()
    }

    pub(crate) fn compare_exchange<'g>(
        &self,
        current: Shared<'_, T>,
        new: Shared<'g, T>,
        success: Ordering,
        failure: Ordering,
        _: &'g Guard<'_>,
    ) -> Result<Shared<'g, T>, CompareExchangeError<'g, T>> {
        match self
            .0
            .compare_exchange(current.ptr, new.ptr, success, failure)
        {
            Ok(ptr) => Ok(ptr.into()),
            Err(current) => Err(CompareExchangeError {
                current: current.into(),
                new,
            }),
        }
    }
}

impl<T> From<Shared<'_, T>> for Atomic<T> {
    fn from(shared: Shared<'_, T>) -> Self {
        Atomic(shared.ptr.into())
    }
}

impl<T> Clone for Atomic<T> {
    fn clone(&self) -> Self {
        Atomic(self.0.load(Ordering::Relaxed).into())
    }
}

impl<T> fmt::Debug for Shared<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:p}", self.ptr)
    }
}

impl<T> fmt::Debug for Atomic<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:p}", self.0.load(Ordering::SeqCst))
    }
}

pub(crate) struct CompareExchangeError<'g, T> {
    pub(crate) current: Shared<'g, T>,
    pub(crate) new: Shared<'g, T>,
}

pub(crate) struct Shared<'g, T> {
    ptr: *mut Linked<T>,
    _g: PhantomData<&'g ()>,
}

impl<'g, T> Shared<'g, T> {
    pub(crate) fn null() -> Self {
        Shared::from(ptr::null_mut())
    }

    pub(crate) fn boxed(value: T, collector: &Collector) -> Self {
        Shared::from(collector.link_boxed(value))
    }

    pub(crate) unsafe fn into_box(self) -> Box<Linked<T>> {
        Box::from_raw(self.ptr)
    }

    pub(crate) unsafe fn as_ptr(&self) -> *mut Linked<T> {
        self.ptr
    }

    pub(crate) unsafe fn as_ref(&self) -> Option<&'g Linked<T>> {
        self.ptr.as_ref()
    }

    pub(crate) unsafe fn deref(&self) -> &'g Linked<T> {
        &*self.ptr
    }

    pub(crate) fn is_null(&self) -> bool {
        self.ptr.is_null()
    }
}

impl<'g, T> PartialEq<Shared<'g, T>> for Shared<'g, T> {
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl<T> Eq for Shared<'_, T> {}

impl<T> Clone for Shared<'_, T> {
    fn clone(&self) -> Self {
        Shared::from(self.ptr)
    }
}

impl<T> Copy for Shared<'_, T> {}

impl<T> From<*mut Linked<T>> for Shared<'_, T> {
    fn from(ptr: *mut Linked<T>) -> Self {
        Shared {
            ptr,
            _g: PhantomData,
        }
    }
}

impl<T> From<*const Linked<T>> for Shared<'_, T> {
    fn from(ptr: *const Linked<T>) -> Self {
        Shared::from(ptr as *mut _)
    }
}

pub(crate) trait RetireShared {
    unsafe fn retire_shared<T>(&self, shared: Shared<'_, T>);
}

impl RetireShared for Guard<'_> {
    unsafe fn retire_shared<T>(&self, shared: Shared<'_, T>) {
        self.retire(shared.ptr, seize::reclaim::boxed::<T>);
    }
}

pub(crate) unsafe fn unprotected() -> &'static Guard<'static> {
    struct RacyGuard(Guard<'static>);

    unsafe impl Send for RacyGuard {}
    unsafe impl Sync for RacyGuard {}

    static UNPROTECTED: RacyGuard = RacyGuard(unsafe { Guard::unprotected() });
    &UNPROTECTED.0
}

pub(crate) enum GuardRef<'g> {
    Owned(Guard<'g>),
    Ref(&'g Guard<'g>),
}

impl<'g> Deref for GuardRef<'g> {
    type Target = Guard<'g>;

    #[inline]
    fn deref(&self) -> &Guard<'g> {
        match *self {
            GuardRef::Owned(ref guard) | GuardRef::Ref(&ref guard) => guard,
        }
    }
}
