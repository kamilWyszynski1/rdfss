#[macro_use]
extern crate derive_builder;
extern crate reed_solomon_erasure;
pub mod master;
pub mod metadata;
mod schema;
pub mod tracing;
pub mod web;
pub mod worker;

pub mod health {
    tonic::include_proto!("grpc.health.v1"); // The string specified here must match the proto package name
}

const FIELD_SIZE: usize = 256;

/// Galois Field multiplication table
static mut GF_MULT_TABLE: [[u8; FIELD_SIZE]; FIELD_SIZE] = [[0; FIELD_SIZE]; FIELD_SIZE];

/// Galois Field logarithm table
static mut GF_LOG_TABLE: [u8; FIELD_SIZE] = [0; FIELD_SIZE];

/// Galois Field anti-logarithm table
static mut GF_EXP_TABLE: [u8; FIELD_SIZE * 2] = [0; FIELD_SIZE * 2];

/// Initialize Galois Field multiplication and log tables
fn init_galois_field() {
    unsafe {
        let mut x: u8 = 1;
        for i in 0..FIELD_SIZE {
            GF_EXP_TABLE[i] = x;
            GF_LOG_TABLE[x as usize] = i as u8;
            x = x.wrapping_mul(2); // Multiply by the generator
            if x & u8::MAX != 0 {
                x ^= 0x1d; // Modulo irreducible polynomial x^8 + x^4 + x^3 + x^2 + 1
            }
        }
        for i in FIELD_SIZE..(FIELD_SIZE * 2) {
            GF_EXP_TABLE[i] = GF_EXP_TABLE[i - FIELD_SIZE];
        }
        for i in 0..FIELD_SIZE {
            for j in 0..FIELD_SIZE {
                GF_MULT_TABLE[i][j] = galois_multiply_raw(i as u8, j as u8);
            }
        }
    }
}

/// Raw Galois Field multiplication
fn galois_multiply_raw(x: u8, y: u8) -> u8 {
    let mut result = 0;
    let mut a = x;
    let mut b = y;
    while b > 0 {
        if b & 1 != 0 {
            result ^= a;
        }
        a <<= 1;
        if a & u8::MAX != 0 {
            a ^= 0x1d; // Modulo irreducible polynomial
        }
        b >>= 1;
    }
    result
}

/// Generate parity shards based on data shards
fn generate_parity_shards(data_shards: &Vec<Vec<u8>>, parity_count: usize) -> Vec<Vec<u8>> {
    let shard_size = data_shards[0].len();
    let mut parity_shards = vec![vec![0u8; shard_size]; parity_count];

    for (parity_index, parity_shard) in parity_shards.iter_mut().enumerate() {
        for data_index in 0..data_shards.len() {
            for byte_index in 0..shard_size {
                parity_shard[byte_index] ^= galois_multiply(
                    data_shards[data_index][byte_index],
                    (parity_index + data_index + 1) as u8,
                );
            }
        }
    }
    parity_shards
}

/// Galois Field multiplication using the precomputed tables
fn galois_multiply(x: u8, y: u8) -> u8 {
    if x == 0 || y == 0 {
        0
    } else {
        unsafe {
            GF_EXP_TABLE[GF_LOG_TABLE[x as usize] as usize + GF_LOG_TABLE[y as usize] as usize]
        }
    }
}

/// Recover missing shards using parity and received shards
fn recover_data(
    received_shards: &Vec<Vec<u8>>,
    parity_shards: &Vec<Vec<u8>>,
    parity_count: usize,
) -> Vec<Vec<u8>> {
    let shard_size = received_shards[0].len();
    let mut recovered_shards = received_shards.clone();

    for (i, shard) in received_shards.iter().enumerate() {
        if shard.iter().all(|&byte| byte == 0) {
            // Simulate recovery for this example (rebuild missing shard)
            let mut reconstructed = vec![0u8; shard_size];
            for parity_index in 0..parity_count {
                for byte_index in 0..shard_size {
                    reconstructed[byte_index] ^= galois_multiply(
                        parity_shards[parity_index][byte_index],
                        (parity_index + i + 1) as u8,
                    );
                }
            }
            recovered_shards[i] = reconstructed;
        }
    }
    recovered_shards
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parity_generation() {
        init_galois_field();
        let data_shards = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10, 11, 12]];
        let parity_count = 2;
        let parity_shards = generate_parity_shards(&data_shards, parity_count);
        assert_eq!(parity_shards.len(), parity_count);
        assert_eq!(parity_shards[0].len(), data_shards[0].len());
        assert_eq!(parity_shards[1].len(), data_shards[0].len());
    }

    #[test]
    fn test_recovery() {
        init_galois_field();
        let data_shards = vec![vec![1, 2, 3, 4], vec![5, 6, 7, 8], vec![9, 10, 11, 12]];
        let parity_count = 2;
        let parity_shards = generate_parity_shards(&data_shards, parity_count);
        // Simulate data loss
        let mut received_shards = data_shards.clone();
        received_shards[1] = vec![0, 0, 0, 0]; // Simulate loss of shard 2
        let recovered_shards = recover_data(&received_shards, &parity_shards, parity_count);
        assert_eq!(recovered_shards, data_shards);
    }
}
