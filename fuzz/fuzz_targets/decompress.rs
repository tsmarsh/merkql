#![no_main]
use libfuzzer_sys::fuzz_target;
use merkql::compression::Compression;

fuzz_target!(|data: &[u8]| {
    let _ = Compression::decompress(data);
});
