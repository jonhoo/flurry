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

    /// Converts the pointer to a `Box`.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid.
    pub(crate) unsafe fn into_box(self) -> Box<Linked<T>> {
        unsafe { Box::from_raw(self.0.into_inner()) }
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

    /// Converts the pointer to a `Box`.
    ///
    /// # Safety
    ///
    /// This method may be called only if the pointer is valid
    /// and nobody else is holding a reference to the same object.
    pub(crate) unsafe fn into_box(self) -> Box<Linked<T>> {
        unsafe { Box::from_raw(self.ptr) }
    }

    pub(crate) fn as_ptr(&self) -> *mut Linked<T> {
        self.ptr
    }

    /// Dereference the shared pointer if it is not null.
    ///
    /// # Safety
    ///
    /// All concerns of calling `as_ref` on a shared, raw pointer apply.
    pub(crate) unsafe fn as_ref(&self) -> Option<&'g Linked<T>> {
        unsafe { self.ptr.as_ref() }
    }

    /// Dereference the shared pointer.
    ///
    /// # Safety
    ///
    /// All concerns of dereferencing a shared, raw pointer apply.
    pub(crate) unsafe fn deref(&self) -> &'g Linked<T> {
        unsafe { &*self.ptr }
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

pub(crate) trait RetireShared {
    unsafe fn retire_shared<T>(&self, shared: Shared<'_, T>);
}

impl RetireShared for Guard<'_> {
    /// Retire the value, reclaiming it when all outstanding references are dropped.
    ///
    /// # Safety
    ///
    /// An object can only be retired if it is non-null, and is no longer accessible
    /// to any thread. The value also may not be accessed by the current thread after
    /// this guard is dropped.
    unsafe fn retire_shared<T>(&self, shared: Shared<'_, T>) {
        unsafe { self.retire(shared.ptr, seize::reclaim::boxed::<T>) }
    }
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
