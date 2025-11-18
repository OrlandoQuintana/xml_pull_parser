use std::num::ParseIntError;

/// Your FlatRecord must have a z_key field. 
/// Add this if you haven't already:
/// pub struct FlatRecord {
///     pub h3_res3: String,
///     pub date_id: u64,
///     pub z_key: u64,   // <-- Add this
///     ... other fields ...
/// }

/// Converts an H3 string (hex) into a u32.
/// Resolution 3 will *always* fit into 32 bits.
///
/// Example: "89283082813ffff" → 0x89283082u32
#[inline]
fn h3_str_to_u32(h3: &str) -> Result<u32, ParseIntError> {
    // H3 strings are hex, so parse as base-16
    let full = u64::from_str_radix(h3, 16)?;
    Ok((full & 0xFFFF_FFFF) as u32)
}

/// Bit-interleaving helper (Morton code for 2D).
/// This is the canonical, mathematically proven implementation.
#[inline]
fn part1by1(mut x: u32) -> u64 {
    x &= 0x0000FFFF;
    x = (x | (x << 8)) & 0x00FF00FF;
    x = (x | (x << 4)) & 0x0F0F0F0F;
    x = (x | (x << 2)) & 0x33333333;
    x = (x | (x << 1)) & 0x55555555;
    x as u64
}

/// Produces a 64-bit Morton key from two 32-bit integers.
#[inline]
fn morton2(a: u32, b: u32) -> u64 {
    (part1by1(a) << 1) | part1by1(b)
}

/// Takes a Vec<FlatRecord>, computes Z-order keys, sorts it,
/// and returns the Vec<FlatRecord> in-place z-ordered.
///
/// This should be called IMMEDIATELY after your `.take()` from DashMap.
pub fn z_order_records(mut records: Vec<FlatRecord>) -> Vec<FlatRecord> {
    for rec in &mut records {
        // Convert H3 string → u32
        let h3_u32 = match h3_str_to_u32(&rec.h3_res3) {
            Ok(v) => v,
            Err(_) => {
                // If parsing ever fails (shouldn't for valid H3), fallback hash:
                use std::hash::{Hash, Hasher};
                use std::collections::hash_map::DefaultHasher;
                let mut hasher = DefaultHasher::new();
                rec.h3_res3.hash(&mut hasher);
                (hasher.finish() & 0xFFFF_FFFF) as u32
            }
        };

        // Collapse date_id from u64 → u32 (just for the Morton key)
        let date_u32 = (rec.date_id & 0xFFFF_FFFF) as u32;

        // Compute the Z-order key
        rec.z_key = morton2(h3_u32, date_u32);
    }

    // Sort the records by the Z-order key
    records.sort_by_key(|r| r.z_key);

    records
}