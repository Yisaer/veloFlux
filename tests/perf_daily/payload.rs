use rand::{thread_rng, RngCore};
use serde_json::{Map, Value};
use std::fmt::Write as _;

fn random_alnum_string(rng: &mut impl RngCore, len: usize) -> String {
    // JSON-safe alphabet (no escaping needed) with a decent mix of characters.
    const ALPHABET: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        out.push(ALPHABET[(rng.next_u32() as usize) % ALPHABET.len()] as char);
    }
    out
}

pub fn generate_payload_cases(cols: usize, cases: usize, str_len: usize) -> Vec<Vec<u8>> {
    if cases == 0 {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(cases);
    let mut rng = thread_rng();

    let mut key = String::with_capacity(1 + 5);

    for _case_idx in 0..cases {
        let mut obj = Map::<String, Value>::new();

        for col_idx in 1..=cols {
            key.clear();
            key.push('a');
            write!(&mut key, "{col_idx}").expect("write! to String cannot fail");

            let val = random_alnum_string(&mut rng, str_len);
            obj.insert(key.clone(), Value::String(val));
        }

        out.push(serde_json::to_vec(&Value::Object(obj)).expect("serialize json payload"));
    }

    out
}
