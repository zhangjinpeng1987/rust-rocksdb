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

use crocksdb_ffi::{self, DBBlockCipher, DBEncryptionProvider, DBEnv};
use libc::{c_char, c_void, size_t};
use rocksdb::Env;
use std::slice;

pub trait BlockCipher {
    fn block_size(&self) -> usize;
    fn encrypt(&self, data: &mut [u8]);
    fn decrypt(&self, data: &mut [u8]);
}

extern "C" fn f_block_size(ctx: *mut c_void) -> size_t {
    let cipher = unsafe { &*(ctx as *mut Box<BlockCipher>) };
    cipher.block_size() as size_t
}

extern "C" fn f_encrypt(ctx: *mut c_void, data: *mut c_char) {
    unsafe {
        let cipher = &*(ctx as *mut Box<BlockCipher>);
        cipher.encrypt(slice::from_raw_parts_mut(
            data as *mut u8,
            cipher.block_size(),
        ));
    }
}

extern "C" fn f_decrypt(ctx: *mut c_void, data: *mut c_char) {
    unsafe {
        let cipher = &*(ctx as *mut Box<BlockCipher>);
        cipher.decrypt(slice::from_raw_parts_mut(
            data as *mut u8,
            cipher.block_size(),
        ));
    }
}

extern "C" fn destroy_block_cipher(cipher: *mut c_void) {
    unsafe {
        Box::from_raw(cipher as *mut Box<BlockCipher>);
    }
}

pub fn create_ctr_encrypted_env(env: &Env, cipher: Box<BlockCipher>) -> Env {
    unsafe {
        let block_ciper = crocksdb_ffi::crocksdb_create_block_cipher(
            Box::into_raw(Box::new(cipher)) as *mut c_void,
            f_block_size,
            f_encrypt,
            f_decrypt,
        );
        let provider = crocksdb_ffi::crocksdb_create_ctr_encryption_provider(block_ciper);
        let inner = crocksdb_ffi::crocksdb_create_encrypted_env(env.inner, provider);
        Env { inner: inner }
    }
}

pub fn destroy_encrypted_env(env: *mut DBEnv) {
    unsafe {
        crocksdb_ffi::crocksdb_env_destroy(env);
    }
}
