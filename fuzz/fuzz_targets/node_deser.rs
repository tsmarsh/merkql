#![no_main]
use libfuzzer_sys::fuzz_target;
use merkql::node::Node;

fuzz_target!(|data: &[u8]| {
    let _ = Node::deserialize(data);
});
