#![no_main]
use libfuzzer_sys::fuzz_target;
use merkql::record::Record;

fuzz_target!(|data: &[u8]| {
    let _ = Record::deserialize(data);
});
