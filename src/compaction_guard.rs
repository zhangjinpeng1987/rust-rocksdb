use crocksdb_ffi::{self, DBCompactionGuard};
use libc::{c_void, size_t};
use std::{mem, slice};

/// `CompactionGuard` allows an application to provide guards for compaction.
pub trait CompactionGuard {
    fn get_guards_in_range(&self, start: &[u8], end: &[u8]) -> Vec<Vec<u8>>;
}

#[repr(C)]
pub struct CompactionGuardProxy {
    guard: Box<CompactionGuard>,
}

extern "C" fn destructor(guard: *mut c_void) {
    unsafe {
        Box::from_raw(guard as *mut CompactionGuardProxy);
    }
}

extern "C" fn get_guards_in_range(
    guard: *mut c_void,
    start: *const u8,
    start_len: size_t,
    end: *const u8,
    end_len: size_t,
    total: *mut size_t,
    lens: *mut *mut size_t,
) -> *mut *mut u8 {
    unsafe {
        let guard = &mut *(guard as *mut CompactionGuardProxy);
        let start = slice::from_raw_parts(start, start_len);
        let end = slice::from_raw_parts(end, end_len);
        let mut guards = guard.guard.get_guards_in_range(start, end);

        *total = guards.len();
        if *total > 0 {
            let mut res = libc::malloc(*total) as *mut *mut u8;
            *lens = libc::malloc(*total * mem::size_of::<size_t>()) as *mut size_t;
            for key in guards.drain(..) {
                let cloned = libc::malloc(key.len());
                libc::memcpy(cloned, key.as_ptr() as *const c_void, key.len());
                *res = cloned as *mut u8;
                res = res.add(1);
                **lens = key.len() as size_t;
                *lens = (*lens).add(1);
            }
            res.sub(*total)
        } else {
            0 as *mut *mut u8
        }
    }
}

pub struct CompactionGuardHandle {
    pub inner: *mut DBCompactionGuard,
}

impl Drop for CompactionGuardHandle {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_compactionguard_destory(self.inner);
        }
    }
}

pub unsafe fn new_compaction_gurad(
    g: Box<CompactionGuard>,
) -> Result<CompactionGuardHandle, String> {
    let proxy = Box::into_raw(Box::new(CompactionGuardProxy { guard: g }));
    let res = crocksdb_ffi::crocksdb_compactionguard_create(
        proxy as *mut c_void,
        destructor,
        get_guards_in_range,
    );
    Ok(CompactionGuardHandle { inner: res })
}
