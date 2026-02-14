#![no_main]
use libfuzzer_sys::fuzz_target;
use merkql::tree::TreeSnapshot;

fuzz_target!(|data: &[u8]| {
    let _ = bincode::deserialize::<TreeSnapshot>(data);
});
