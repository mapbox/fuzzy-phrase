use std::io::Cursor;
use byteorder::{BigEndian, WriteBytesExt, ReadBytesExt};

pub fn chop_int(num: u64) -> Vec<u8> {
    let mut wtr = vec![];
    wtr.write_u64::<BigEndian>(num).unwrap();
    wtr
}

pub fn three_byte_encode(num: u64) -> Vec<u8> {
    let chopped: Vec<u8> = chop_int(num);
    let three_bytes: Vec<u8> = chopped[5..8].to_vec();
    three_bytes
}

pub fn three_byte_decode(three_bytes: &[u8]) -> u64 {
    let mut padded_byte_vec: Vec<u8> = vec![0u8; 5];
    padded_byte_vec.extend_from_slice(three_bytes);
    let mut reader = Cursor::new(padded_byte_vec);
    reader.read_u64::<BigEndian>().unwrap()
}

pub fn phrase_to_key(phrase: &[u64]) -> Vec<u8> {
    let key: Vec<u8> = phrase.into_iter()
                    .flat_map(|word| three_byte_encode(*word))
                    .collect();
    key
}

pub fn key_to_phrase(key: &[u8]) -> Vec<u64> {
    let mut phrase: Vec<u64> = vec![];
    let mut i = 0;
    while i < (key.len() - 2) {
        let word = &key[i..i+3];
        phrase.push(three_byte_decode(&word));
        i += 3;
    }
    phrase
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chop_smallest_int_to_bytes() {
        let n: u64 = u64::min_value();
        let chopped: Vec<u8> = chop_int(n);
        assert_eq!(
            vec![0u8, 0u8, 0u8, 0u8,
                 0u8, 0u8, 0u8, 0u8],
            chopped
        );

    }

    #[test]
    fn chop_largest_int_to_bytes() {
        let n: u64 = u64::max_value();
        let chopped: Vec<u8> = chop_int(n);
        assert_eq!(
            vec![255u8, 255u8, 255u8, 255u8,
                 255u8, 255u8, 255u8, 255u8],
            chopped
        );
    }

    #[test]
    fn chop_big_int_to_bytes() {
        // first value larger than u32::max_value(), aka 2**32
        let n: u64 = 4_294_967_296;
        let chopped: Vec<u8> = chop_int(n);
        assert_eq!(
            vec![0u8, 0u8, 0u8, 1u8,
                 0u8, 0u8, 0u8, 0u8],
            chopped
        );
    }

    #[test]
    fn medium_integer_to_three_bytes() {
        // the number we're using is arbitrary.
        let n: u64 = 61_528;
        let three_bytes: Vec<u8> = three_byte_encode(n);
        assert_eq!(
            vec![ 0u8, 240u8, 88u8],
            three_bytes
        );
    }

    #[test]
    fn large_integer_to_three_bytes() {
        // the number we're using is arbitrary. happens to be the number of distinct words in
        // us-address, so gives us an idea of the cardinality we're dealing with.
        let n: u64 = 561_528;
        let three_bytes: Vec<u8> = three_byte_encode(n);
        assert_eq!(
            vec![ 8u8, 145u8, 120u8],
            three_bytes
        );
    }

    #[test]
    fn three_bytes_to_large_integer() {
        let three_bytes: Vec<u8> = vec![ 8u8, 145u8, 120u8];
        let n: u64 = three_byte_decode(&three_bytes);
        assert_eq!(
            561_528u64,
            n
        );
    }

    #[test]
    fn convert_phrase_to_key() {
        let phrase = [61_528_u64, 561_528u64, 1u64];
        let key = phrase_to_key(&phrase);
        assert_eq!(
            vec![
                0u8, 240u8, 88u8,    // 61_528
                8u8, 145u8, 120u8,   // 561_528
                0u8, 0u8,   1u8      // 1
            ],
            key
        );
    }

    #[test]
    fn convert_key_to_phrase() {
        let key =vec![
            0u8, 240u8, 88u8,    // 61_528
            8u8, 145u8, 120u8,   // 561_528
            0u8, 0u8,   1u8      // 1
        ];
        let phrase = key_to_phrase(&key);
        assert_eq!(
            vec![61_528_u64, 561_528u64, 1u64],
            phrase
        );
    }

}
