// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use rocksdb::Env;
use rocksdb::{create_ctr_encrypted_env, DBOptions, IBlockCipher, Writable, DB};
use std::sync::Arc;
use tempdir::TempDir;

struct SimpleBlockCipher {
    block_size: usize,
}

impl SimpleBlockCipher {
    fn new(block_size: usize) -> Self {
        Self { block_size }
    }
}

impl IBlockCipher for SimpleBlockCipher {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn encrypt(&self, data: &mut [u8]) {
        for i in 0..data.len() {
            if data[i] == 255 {
                data[i] = 0;
            } else {
                data[i] += 1;
            }
        }
    }

    fn decrypt(&self, data: &mut [u8]) {
        for i in 0..data.len() {
            if data[i] == 0 {
                data[i] = 255;
            } else {
                data[i] -= 1;
            }
        }
    }
}

#[test]
fn test_cryption_env() {
    let path = TempDir::new("_rust_rocksdb_cryption_env").expect("");
    let path_str = path.path().to_str().unwrap();
    let default_env = Env::default();
    let simple_block_cipher = SimpleBlockCipher::new(4096);
    let encrypted_env = Arc::new(create_ctr_encrypted_env(
        &default_env,
        Box::new(simple_block_cipher),
    ));

    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    opts.set_env(encrypted_env.clone());
    let db = DB::open(opts, path_str).unwrap();

    let samples = vec![
        (b"key1".to_vec(), b"value1".to_vec()),
        (b"key2".to_vec(), b"value2".to_vec()),
        (b"key3".to_vec(), b"value3".to_vec()),
        (b"key4".to_vec(), b"value4".to_vec()),
    ];
    for &(ref k, ref v) in &samples {
        db.put(k, v).unwrap();

        // check value
        assert_eq!(v.as_slice(), &*db.get(k).unwrap().unwrap());
    }

    // flush to sst file
    db.flush(true).unwrap();

    // check value in db
    for &(ref k, ref v) in &samples {
        assert_eq!(v.as_slice(), &*db.get(k).unwrap().unwrap());
    }

    // close db and open again.
    drop(db);
    let mut opts = DBOptions::new();
    opts.create_if_missing(true);
    opts.set_env(encrypted_env);
    let db = DB::open(opts, path_str).unwrap();

    // check value in db again
    for &(ref k, ref v) in &samples {
        assert_eq!(v.as_slice(), &*db.get(k).unwrap().unwrap());
    }
}
