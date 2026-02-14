#![no_main]
use libfuzzer_sys::fuzz_target;
use merkql::hash::Hash;

fuzz_target!(|data: &[u8]| {
    if data.len() == 32 {
        let mut buf = [0u8; 32];
        buf.copy_from_slice(data);
        let hash = Hash(buf);
        // Verify round-trip: hash bytes -> hex -> parse -> same bytes
        let hex = hash.to_hex();
        let recovered = Hash::from_hex(&hex).unwrap();
        assert_eq!(hash, recovered);
    }
    // Shorter slices: no valid 32-byte entry, nothing to do
});
