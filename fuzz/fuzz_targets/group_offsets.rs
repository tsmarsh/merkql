#![no_main]
use libfuzzer_sys::fuzz_target;
use merkql::group::TopicPartition;
use std::collections::HashMap;

fuzz_target!(|data: &[u8]| {
    let _ = bincode::deserialize::<HashMap<TopicPartition, u64>>(data);
});
