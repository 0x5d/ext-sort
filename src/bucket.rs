use core::time;
use std::{
    sync::atomic::{
        AtomicI32,
        Ordering::{Acquire, Relaxed, Release},
    },
    thread::sleep,
};

pub struct Bucket {
    capacity: AtomicI32,
}

impl Bucket {
    pub fn new(capacity: i32) -> Bucket {
        let capacity = AtomicI32::new(capacity);
        Bucket { capacity }
    }

    pub fn take(&self) {
        loop {
            let current = self.capacity.load(Acquire);
            if current > 0 {
                self.capacity.fetch_sub(1, Release);
                return;
            }
            sleep(time::Duration::from_millis(1));
        }
    }

    pub fn put(&self) {
        self.capacity.fetch_add(1, Relaxed);
    }
}
