#![no_main]
use libfuzzer_sys::fuzz_target;
use merkql::hash::Hash;

fuzz_target!(|data: &[u8]| {
    let s = std::str::from_utf8(data).unwrap_or("");
    let _ = Hash::from_hex(s);
});
