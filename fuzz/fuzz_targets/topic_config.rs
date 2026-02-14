#![no_main]
use libfuzzer_sys::fuzz_target;
use merkql::topic::TopicConfig;

fuzz_target!(|data: &[u8]| {
    let _ = bincode::deserialize::<TopicConfig>(data);
});
