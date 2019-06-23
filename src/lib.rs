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

extern crate core;
extern crate libc;
#[macro_use]
pub extern crate librocksdb_sys;
#[cfg(test)]
extern crate tempdir;

pub use compaction_filter::CompactionFilter;
pub use compaction_guard::CompactionGuard;
pub use event_listener::{
    CompactionJobInfo, EventListener, FlushJobInfo, IngestionInfo, WriteStallInfo,
};
pub use librocksdb_sys::{
    self as crocksdb_ffi, new_bloom_filter, CompactionPriority, CompactionReason,
    DBBottommostLevelCompaction, DBCompactionStyle, DBCompressionType, DBEntryType, DBInfoLogLevel,
    DBRateLimiterMode, DBRecoveryMode, DBStatisticsHistogramType, DBStatisticsTickerType,
    DBTitanDBBlobRunMode, WriteStallCondition,
};
pub use merge_operator::MergeOperands;
pub use metadata::{ColumnFamilyMetaData, LevelMetaData, SstFileMetaData};
pub use perf_context::{get_perf_level, set_perf_level, IOStatsContext, PerfContext, PerfLevel};
pub use rocksdb::{
    load_latest_options, run_ldb_tool, set_external_sst_file_global_seq_no, BackupEngine, CFHandle,
    Cache, DBIterator, DBVector, Env, ExternalSstFileInfo, Kv, Range, SeekKey, SequentialFile,
    SstFileWriter, Writable, WriteBatch, DB,
};
pub use rocksdb_options::{
    BlockBasedOptions, CColumnFamilyDescriptor, ColumnFamilyOptions, CompactOptions,
    CompactionOptions, DBOptions, EnvOptions, FifoCompactionOptions, HistogramData,
    IngestExternalFileOptions, LRUCacheOptions, RateLimiter, ReadOptions, RestoreOptions,
    WriteOptions,
};
pub use slice_transform::SliceTransform;
pub use table_filter::TableFilter;
pub use table_properties::{
    TableProperties, TablePropertiesCollection, TablePropertiesCollectionView,
    UserCollectedProperties,
};
pub use table_properties_collector::TablePropertiesCollector;
pub use table_properties_collector_factory::TablePropertiesCollectorFactory;
pub use titan::{TitanBlobIndex, TitanDBOptions};

mod compaction_filter;
mod compaction_guard;
pub mod comparator;
mod event_listener;
pub mod merge_operator;
mod metadata;
mod perf_context;
pub mod rocksdb;
pub mod rocksdb_options;
mod slice_transform;
mod table_filter;
mod table_properties;
mod table_properties_collector;
mod table_properties_collector_factory;
mod titan;
mod util;
