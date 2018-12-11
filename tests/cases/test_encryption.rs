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
use rocksdb::{create_ctr_encrypted_env, BlockCipher, DBOptions, Writable, DB};
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

impl BlockCipher for SimpleBlockCipher {
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

static CIPHER16: &'static [u8] = &[16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1];

struct CTR16BlockCipher {
    ciphertext: &'static [u8],
}

impl CTR16BlockCipher {
    fn new() -> Self {
        Self {
            ciphertext: CIPHER16,
        }
    }
}

impl BlockCipher for CTR16BlockCipher {
    fn block_size(&self) -> usize {
        16
    }

    fn encrypt(&self, data: &mut [u8]) {
        for i in 0..data.len() {
            data[i] ^= self.ciphertext[i];
        }
    }

    fn decrypt(&self, data: &mut [u8]) {
        self.encrypt(data);
    }
}

#[test]
fn test_simple_encrypted_env() {
    let default_env = Env::default();
    let simple_block_cipher = SimpleBlockCipher::new(128);
    let encrypted_env = Arc::new(create_ctr_encrypted_env(
        &default_env,
        Box::new(simple_block_cipher),
    ));

    test_encrypted_env(encrypted_env);
}

#[test]
fn test_ctr16_encrypted_env() {
    let default_env = Env::default();
    let ctr16_block_cipher = CTR16BlockCipher::new();
    let encrypted_env = Arc::new(create_ctr_encrypted_env(
        &default_env,
        Box::new(ctr16_block_cipher),
    ));

    test_encrypted_env(encrypted_env);
}

fn test_encrypted_env(encrypted_env: Arc<Env>) {
    let path = TempDir::new("_rust_rocksdb_cryption_env").expect("");
    let path_str = path.path().to_str().unwrap();

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
