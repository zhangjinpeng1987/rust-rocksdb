// Copyright 2014 Tyler Neely
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use compaction_filter::{CompactionFilter, new_compaction_filter, CompactionFilterHandle};
use comparator::{self, ComparatorCallback, compare_callback};
use libc::{self, c_int, size_t, c_void};
use merge_operator::{self, MergeOperatorCallback, full_merge_callback, partial_merge_callback};
use merge_operator::MergeFn;

use rocksdb_ffi::{self, DBOptions, DBWriteOptions, DBBlockBasedTableOptions, DBReadOptions,
                  DBCompressionType, DBRecoveryMode, DBSnapshot, DBInstance, DBFlushOptions,
                  DBRateLimiter};
use std::ffi::{CStr, CString};
use std::mem;

pub struct BlockBasedOptions {
    inner: *mut DBBlockBasedTableOptions,
}

impl Drop for BlockBasedOptions {
    fn drop(&mut self) {
        unsafe {
            rocksdb_ffi::rocksdb_block_based_options_destroy(self.inner);
        }
    }
}

impl Default for BlockBasedOptions {
    fn default() -> BlockBasedOptions {
        unsafe {
            let block_opts = rocksdb_ffi::rocksdb_block_based_options_create();
            assert!(!block_opts.is_null(),
                    "Could not create rocksdb block based options");
            BlockBasedOptions { inner: block_opts }
        }
    }
}

impl BlockBasedOptions {
    pub fn new() -> BlockBasedOptions {
        BlockBasedOptions::default()
    }

    pub fn set_block_size(&mut self, size: usize) {
        unsafe {
            rocksdb_ffi::rocksdb_block_based_options_set_block_size(self.inner, size);
        }
    }

    pub fn set_lru_cache(&mut self, size: size_t) {
        let cache = rocksdb_ffi::new_cache(size);
        unsafe {
            // because cache is wrapped in shared_ptr, so we don't need to call
            // rocksdb_cache_destroy explicitly.
            rocksdb_ffi::rocksdb_block_based_options_set_block_cache(self.inner, cache);
        }
    }

    pub fn set_bloom_filter(&mut self, bits_per_key: c_int, block_based: bool) {
        unsafe {
            let bloom = if block_based {
                rocksdb_ffi::rocksdb_filterpolicy_create_bloom(bits_per_key)
            } else {
                rocksdb_ffi::rocksdb_filterpolicy_create_bloom_full(bits_per_key)
            };

            rocksdb_ffi::rocksdb_block_based_options_set_filter_policy(self.inner, bloom);
        }
    }

    pub fn set_cache_index_and_filter_blocks(&mut self, v: bool) {
        unsafe {
            rocksdb_ffi::rocksdb_block_based_options_set_cache_index_and_filter_blocks(self.inner,
                                                                                       v as u8);
        }
    }
}

pub struct RateLimiter {
    inner: *mut DBRateLimiter,
}

impl RateLimiter {
    pub fn new(rate_bytes_per_sec: i64,
               refill_period_us: i64,
               fairness: i32) -> RateLimiter {
        let limiter = unsafe {
            rocksdb_ffi::rocksdb_ratelimiter_create(rate_bytes_per_sec,
                                                    refill_period_us,
                                                    fairness)
        };
        RateLimiter {
            inner: limiter,
        }
    }
}

impl Drop for RateLimiter {
    fn drop(&mut self) {
        unsafe { rocksdb_ffi::rocksdb_ratelimiter_destroy(self.inner) }
    }
}

/// The UnsafeSnap must be destroyed by db, it maybe be leaked
/// if not using it properly, hence named as unsafe.
///
/// This object is convenient for wrapping snapshot by yourself. In most
/// cases, using `Snapshot` is enough.
pub struct UnsafeSnap {
    inner: *const DBSnapshot,
}

impl UnsafeSnap {
    pub unsafe fn new(db: *mut DBInstance) -> UnsafeSnap {
        UnsafeSnap { inner: rocksdb_ffi::rocksdb_create_snapshot(db) }
    }

    pub unsafe fn get_inner(&self) -> *const DBSnapshot {
        self.inner
    }
}

pub struct ReadOptions {
    inner: *mut DBReadOptions,
    upper_bound: Vec<u8>,
}

impl Drop for ReadOptions {
    fn drop(&mut self) {
        unsafe { rocksdb_ffi::rocksdb_readoptions_destroy(self.inner) }
    }
}

impl Default for ReadOptions {
    fn default() -> ReadOptions {
        unsafe {
            let opts = rocksdb_ffi::rocksdb_readoptions_create();
            assert!(!opts.is_null(), "Unable to create rocksdb read options");
            ReadOptions {
                inner: opts,
                upper_bound: vec![],
            }
        }
    }
}

impl ReadOptions {
    pub fn new() -> ReadOptions {
        ReadOptions::default()
    }

    // TODO add snapshot setting here
    // TODO add snapshot wrapper structs with proper destructors;
    // that struct needs an "iterator" impl too.
    #[allow(dead_code)]
    pub fn fill_cache(&mut self, v: bool) {
        unsafe {
            rocksdb_ffi::rocksdb_readoptions_set_fill_cache(self.inner, v);
        }
    }

    pub unsafe fn set_snapshot(&mut self, snapshot: &UnsafeSnap) {
        rocksdb_ffi::rocksdb_readoptions_set_snapshot(self.inner, snapshot.inner);
    }

    pub fn set_iterate_upper_bound(&mut self, key: &[u8]) {
        self.upper_bound = Vec::from(key);
        unsafe {
            rocksdb_ffi::rocksdb_readoptions_set_iterate_upper_bound(self.inner,
                                                                     self.upper_bound.as_ptr(),
                                                                     self.upper_bound.len());
        }
    }

    pub unsafe fn get_inner(&self) -> *const DBReadOptions {
        self.inner
    }
}

pub struct WriteOptions {
    pub inner: *mut DBWriteOptions,
}

impl Drop for WriteOptions {
    fn drop(&mut self) {
        unsafe {
            rocksdb_ffi::rocksdb_writeoptions_destroy(self.inner);
        }
    }
}

impl Default for WriteOptions {
    fn default() -> WriteOptions {
        let write_opts = unsafe { rocksdb_ffi::rocksdb_writeoptions_create() };
        assert!(!write_opts.is_null(),
                "Could not create rocksdb write options");
        WriteOptions { inner: write_opts }
    }
}

impl WriteOptions {
    pub fn new() -> WriteOptions {
        WriteOptions::default()
    }

    pub fn set_sync(&mut self, sync: bool) {
        unsafe {
            rocksdb_ffi::rocksdb_writeoptions_set_sync(self.inner, sync);
        }
    }

    pub fn disable_wal(&mut self, disable: bool) {
        unsafe {
            if disable {
                rocksdb_ffi::rocksdb_writeoptions_disable_WAL(self.inner, 1);
            } else {
                rocksdb_ffi::rocksdb_writeoptions_disable_WAL(self.inner, 0);
            }
        }
    }
}

pub struct Options {
    pub inner: *mut DBOptions,
    filter: Option<CompactionFilterHandle>,
}

impl Drop for Options {
    fn drop(&mut self) {
        unsafe {
            rocksdb_ffi::rocksdb_options_destroy(self.inner);
        }
    }
}

impl Default for Options {
    fn default() -> Options {
        unsafe {
            let opts = rocksdb_ffi::rocksdb_options_create();
            assert!(!opts.is_null(), "Could not create rocksdb options");
            Options {
                inner: opts,
                filter: None,
            }
        }
    }
}

impl Options {
    pub fn new() -> Options {
        Options::default()
    }

    pub fn increase_parallelism(&mut self, parallelism: i32) {
        unsafe {
            rocksdb_ffi::rocksdb_options_increase_parallelism(self.inner, parallelism);
        }
    }

    pub fn optimize_level_style_compaction(&mut self, memtable_memory_budget: i32) {
        unsafe {
            rocksdb_ffi::rocksdb_options_optimize_level_style_compaction(self.inner,
                                                                         memtable_memory_budget);
        }
    }

    /// Set compaction filter.
    ///
    /// filter will be dropped when this option is dropped or a new filter is
    /// set.
    ///
    /// By default, compaction will only pass keys written after the most
    /// recent call to GetSnapshot() to filter. However, if `ignore_snapshots`
    /// is set to true, even if the keys were written before the last snapshot
    /// will be passed to filter too. For more details please checkout
    /// rocksdb's documentation.
    ///
    /// See also `CompactionFilter`.
    pub fn set_compaction_filter<S>(&mut self,
                                    name: S,
                                    ignore_snapshots: bool,
                                    filter: Box<CompactionFilter>)
                                    -> Result<(), String>
        where S: Into<Vec<u8>>
    {
        unsafe {
            let c_name = match CString::new(name) {
                Ok(s) => s,
                Err(e) => return Err(format!("failed to convert to cstring: {:?}", e)),
            };
            self.filter = Some(try!(new_compaction_filter(c_name, ignore_snapshots, filter)));
            rocksdb_ffi::rocksdb_options_set_compaction_filter(self.inner,
                                                               self.filter
                                                                   .as_ref()
                                                                   .unwrap()
                                                                   .inner);
            Ok(())
        }
    }

    pub fn create_if_missing(&mut self, create_if_missing: bool) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_create_if_missing(self.inner, create_if_missing);
        }
    }

    pub fn compression(&mut self, t: DBCompressionType) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_compression(self.inner, t);
        }
    }

    pub fn compression_per_level(&mut self, level_types: &[DBCompressionType]) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_compression_per_level(self.inner,
                                                                   level_types.as_ptr(),
                                                                   level_types.len() as size_t)
        }
    }

    pub fn add_merge_operator(&mut self, name: &str, merge_fn: MergeFn) {
        let cb = Box::new(MergeOperatorCallback {
            name: CString::new(name.as_bytes()).unwrap(),
            merge_fn: merge_fn,
        });

        unsafe {
            let mo = rocksdb_ffi::rocksdb_mergeoperator_create(mem::transmute(cb),
                                                               merge_operator::destructor_callback,
                                                               full_merge_callback,
                                                               partial_merge_callback,
                                                               None,
                                                               merge_operator::name_callback);
            rocksdb_ffi::rocksdb_options_set_merge_operator(self.inner, mo);
        }
    }

    pub fn add_comparator(&mut self, name: &str, compare_fn: fn(&[u8], &[u8]) -> i32) {
        let cb = Box::new(ComparatorCallback {
            name: CString::new(name.as_bytes()).unwrap(),
            f: compare_fn,
        });

        unsafe {
            let cmp = rocksdb_ffi::rocksdb_comparator_create(mem::transmute(cb),
                                                             comparator::destructor_callback,
                                                             compare_callback,
                                                             comparator::name_callback);
            rocksdb_ffi::rocksdb_options_set_comparator(self.inner, cmp);
        }
    }


    pub fn set_block_cache_size_mb(&mut self, cache_size: u64) {
        unsafe {
            rocksdb_ffi::rocksdb_options_optimize_for_point_lookup(self.inner, cache_size);
        }
    }

    pub fn set_max_open_files(&mut self, nfiles: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_max_open_files(self.inner, nfiles);
        }
    }

    pub fn set_use_fsync(&mut self, useit: bool) {
        unsafe {
            if useit {
                rocksdb_ffi::rocksdb_options_set_use_fsync(self.inner, 1)
            } else {
                rocksdb_ffi::rocksdb_options_set_use_fsync(self.inner, 0)
            }
        }
    }

    pub fn set_bytes_per_sync(&mut self, nbytes: u64) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_bytes_per_sync(self.inner, nbytes);
        }
    }

    pub fn set_disable_data_sync(&mut self, disable: bool) {
        unsafe {
            if disable {
                rocksdb_ffi::rocksdb_options_set_disable_data_sync(self.inner, 1);
            } else {
                rocksdb_ffi::rocksdb_options_set_disable_data_sync(self.inner, 0);
            }
        }
    }

    pub fn allow_os_buffer(&mut self, is_allow: bool) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_allow_os_buffer(self.inner, is_allow);
        }
    }

    pub fn set_table_cache_num_shard_bits(&mut self, nbits: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_table_cache_numshardbits(self.inner, nbits);
        }
    }

    pub fn set_min_write_buffer_number(&mut self, nbuf: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_min_write_buffer_number_to_merge(self.inner, nbuf);
        }
    }

    pub fn set_max_write_buffer_number(&mut self, nbuf: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_max_write_buffer_number(self.inner, nbuf);
        }
    }

    pub fn set_write_buffer_size(&mut self, size: u64) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_write_buffer_size(self.inner, size);
        }
    }

    pub fn set_max_bytes_for_level_base(&mut self, size: u64) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_max_bytes_for_level_base(self.inner, size);
        }
    }

    pub fn set_max_bytes_for_level_multiplier(&mut self, mul: i32) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_max_bytes_for_level_multiplier(self.inner, mul);
        }
    }

    pub fn set_max_manifest_file_size(&mut self, size: u64) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_max_manifest_file_size(self.inner, size);
        }
    }

    pub fn set_target_file_size_base(&mut self, size: u64) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_target_file_size_base(self.inner, size);
        }
    }

    pub fn set_min_write_buffer_number_to_merge(&mut self, to_merge: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_min_write_buffer_number_to_merge(self.inner, to_merge);
        }
    }

    pub fn set_level_zero_file_num_compaction_trigger(&mut self, n: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_level0_file_num_compaction_trigger(self.inner, n);
        }
    }

    pub fn set_level_zero_slowdown_writes_trigger(&mut self, n: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_level0_slowdown_writes_trigger(self.inner, n);
        }
    }

    pub fn set_level_zero_stop_writes_trigger(&mut self, n: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_level0_stop_writes_trigger(self.inner, n);
        }
    }

    pub fn set_compaction_style(&mut self, style: rocksdb_ffi::DBCompactionStyle) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_compaction_style(self.inner, style);
        }
    }

    pub fn set_max_background_compactions(&mut self, n: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_max_background_compactions(self.inner, n);
        }
    }

    pub fn set_max_background_flushes(&mut self, n: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_max_background_flushes(self.inner, n);
        }
    }

    pub fn set_filter_deletes(&mut self, filter: bool) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_filter_deletes(self.inner, filter);
        }
    }

    pub fn set_disable_auto_compactions(&mut self, disable: bool) {
        unsafe {
            if disable {
                rocksdb_ffi::rocksdb_options_set_disable_auto_compactions(self.inner, 1)
            } else {
                rocksdb_ffi::rocksdb_options_set_disable_auto_compactions(self.inner, 0)
            }
        }
    }

    pub fn set_block_based_table_factory(&mut self, factory: &BlockBasedOptions) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_block_based_table_factory(self.inner, factory.inner);
        }
    }

    pub fn set_report_bg_io_stats(&mut self, enable: bool) {
        unsafe {
            if enable {
                rocksdb_ffi::rocksdb_options_set_report_bg_io_stats(self.inner, 1);
            } else {
                rocksdb_ffi::rocksdb_options_set_report_bg_io_stats(self.inner, 0);
            }
        }
    }

    pub fn set_wal_recovery_mode(&mut self, mode: DBRecoveryMode) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_wal_recovery_mode(self.inner, mode);
        }
    }

    pub fn enable_statistics(&mut self) {
        unsafe {
            rocksdb_ffi::rocksdb_options_enable_statistics(self.inner);
        }
    }

    pub fn get_statistics(&self) -> Option<String> {
        unsafe {
            let value = rocksdb_ffi::rocksdb_options_statistics_get_string(self.inner);


            if value.is_null() {
                return None;
            }

            // Must valid UTF-8 format.
            let s = CStr::from_ptr(value).to_str().unwrap().to_owned();
            libc::free(value as *mut c_void);
            Some(s)
        }
    }

    pub fn set_stats_dump_period_sec(&mut self, period: usize) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_stats_dump_period_sec(self.inner, period);
        }
    }

    pub fn set_num_levels(&mut self, n: c_int) {
        unsafe {
            rocksdb_ffi::rocksdb_options_set_num_levels(self.inner, n);
        }
    }

    pub fn set_ratelimiter(&mut self, rate_bytes_per_sec: i64) {
        let rate_limiter = RateLimiter::new(rate_bytes_per_sec,
                                            100 * 1000 /* 100ms should work for most cases */,
                                            10 /* should be good by leaving it at default 10 */);
        unsafe {
            rocksdb_ffi::rocksdb_options_set_ratelimiter(self.inner, rate_limiter.inner);
        }
    }
}

pub struct FlushOptions {
    pub inner: *mut DBFlushOptions,
}

impl FlushOptions {
    pub fn new() -> FlushOptions {
        unsafe { FlushOptions { inner: rocksdb_ffi::rocksdb_flushoptions_create() } }
    }

    pub fn set_wait(&mut self, wait: bool) {
        unsafe {
            rocksdb_ffi::rocksdb_flushoptions_set_wait(self.inner, wait);
        }
    }
}

impl Drop for FlushOptions {
    fn drop(&mut self) {
        unsafe {
            rocksdb_ffi::rocksdb_flushoptions_destroy(self.inner);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Options;

    #[test]
    fn test_set_max_manifest_file_size() {
        let mut opts = Options::new();
        let size = 20 * 1024 * 1024;
        opts.set_max_manifest_file_size(size)
    }

    #[test]
    fn test_enable_statistics() {
        let mut opts = Options::new();
        opts.enable_statistics();
        opts.set_stats_dump_period_sec(60);
        assert!(opts.get_statistics().is_some());

        let opts = Options::new();
        assert!(opts.get_statistics().is_none());
    }
}
