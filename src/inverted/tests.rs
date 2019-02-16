extern crate lazy_static;
extern crate strsim;
extern crate regex;
use std::fs::File;
use fst::Streamer;
use std::collections::BTreeMap;
use self::strsim::osa_distance;
use self::regex::Regex;
use super::*;
use std::sync::Mutex;

lazy_static! {
    static ref PREFIX_DATA: &'static str = include_str!("../../benches/data/phrase_test_shared_prefix.txt");
    static ref TYPO_DATA: &'static str = include_str!("../../benches/data/phrase_test_typos.txt");
    static ref PHRASES: Vec<&'static str> = {
        // shared-prefix test set
        let mut phrases = PREFIX_DATA.trim().split("\n").collect::<Vec<&str>>();
        // typos test set
        phrases.extend(TYPO_DATA.trim().split("\n"));
        // take a few of the prefix test data set examples and add more phrases that are strict
        // prefixes of entries we already have to test windowed search
        phrases.extend(PREFIX_DATA.trim().split("\n").take(5).map(|phrase| {
            phrase.rsplitn(2, " ").skip(1).next().unwrap()
        }));
        phrases
    };
    static ref WORDS: BTreeMap<&'static str, u32> = {
        let mut words: BTreeMap<&'static str, u32> = BTreeMap::new();
        for phrase in PHRASES.iter() {
            for word in phrase.split(' ') {
                words.insert(word, 0);
            }
        }
        let mut id: u32 = 0;
        for (_key, value) in words.iter_mut() {
            *value = id;
            id += 1;
        }
        words
    };
    static ref WORD_IDS: Vec<&'static str> = {
        WORDS.keys().map(|s| *s).collect()
    };
    static ref DISTANCES: BTreeMap<u32, Vec<(u32, u8)>> = {
        let mut out: BTreeMap<u32, Vec<(u32, u8)>> = BTreeMap::new();

        let mut non_number: Vec<(&'static str, u32)> = Vec::new();
        let number_chars = Regex::new("[0-9#]").unwrap();
        for (word, id) in WORDS.iter() {
            out.insert(*id, vec![(*id, 0)]);
            if !number_chars.is_match(word) {
                non_number.push((*word, *id));
            }
        }

        for (word1, id1) in &non_number {
            for (word2, id2) in &non_number {
                if osa_distance(word1, word2) == 1 {
                    out.get_mut(id1).unwrap().push((*id2, 1));
                }
            }
        }

        out
    };
    static ref ID_PHRASES: Vec<Vec<u32>> = {
        let mut id_phrases = PHRASES.iter().map(|phrase| {
            phrase.split(' ').map(|w| WORDS[w]).collect::<Vec<_>>()
        }).collect::<Vec<_>>();
        id_phrases.sort();
        id_phrases
    };
    static ref II: Mutex<InvertedIndex<Vec<u8>, capnp::serialize::OwnedSegments>> = {
        let mut builder = InvertedIndexBuilder::memory();
        for (i, id_phrase) in ID_PHRASES.iter().enumerate() {
            builder.insert(i as u32, &id_phrase).unwrap();
        }
        let bytes = builder.into_inner().unwrap();
        Mutex::new(
            InvertedIndex::<Vec<u8>, capnp::serialize::OwnedSegments>::from_bytes(bytes, Box::new(|id: u32| ID_PHRASES[id as usize].clone())).unwrap()
        )
    };
}

fn get_full(phrase: &str) -> Vec<QueryWord> {
    phrase.split(' ').map(
        |w| QueryWord::new_full(WORDS[w], 0)
    ).collect::<Vec<_>>()
}

fn expand_full(phrase: &[QueryWord]) -> String {
    let v: Vec<&str> = phrase.iter().map(|word| match word {
        QueryWord::Full { id, .. } => WORD_IDS[*id as usize],
        _ => panic!("no prefixes")
    }).collect();
    v.join(" ")
}

fn get_prefix(phrase: &str) -> Vec<QueryWord> {
    let words: Vec<&str> = phrase.split(' ').collect();
    let mut out = words[..(words.len() - 1)].iter().map(
        |w| QueryWord::new_full(*WORDS.get(w).unwrap(), 0)
    ).collect::<Vec<QueryWord>>();
    let last = &words[words.len() - 1];
    let prefix_match = WORDS.iter().filter(|(k, _v)| k.starts_with(last)).collect::<Vec<_>>();
    out.push(QueryWord::new_prefix((*prefix_match[0].1, *prefix_match.last().unwrap().1)));
    out
}

#[test]
fn sample_contains() {
    let ii = II.lock().unwrap();

    // just test everything
    for phrase in PHRASES.iter() {
        assert!(ii.contains(
            QueryPhrase::new(&get_full(phrase)).unwrap()
        ).unwrap());
    }
}

#[test]
fn sample_match_substring() {
    let ii = II.lock().unwrap();

    // just test everything
    // for phrase in PHRASES.iter() {
    //     assert!(ii.contains(
    //         QueryPhrase::new(&get_full(phrase)).unwrap()
    //     ).unwrap());
    // }
    let matches = ii.match_substring(QueryPhrase::new(&get_full("Co Rd")).unwrap()).unwrap();
    let expanded: Vec<_> = matches.iter().map(|x| expand_full(x.as_slice())).collect();
    println!("{:?}", expanded);
}

// #[test]
// fn sample_doesnt_contain() {
//     // construct some artificial broken examples by reversing the sequence of good ones
//     for phrase in PHRASES.iter() {
//         let mut inverse = get_full(phrase);
//         inverse.reverse();
//         assert!(!SET.contains(
//             QueryPhrase::new(&inverse).unwrap()
//         ).unwrap());
//     }
//
//     // a couple manual ones
//     let contains = |phrase| {
//         SET.contains(QueryPhrase::new(&get_full(phrase)).unwrap()).unwrap()
//     };
//
//     // typo
//     assert!(!contains("15## Hillis Market Rd"));
//     // prefix
//     assert!(!contains("40# Ivy"));
// }
//
// #[test]
// fn sample_contains_prefix() {
//     // being exhaustive is a little laborious, so just try a bunch of specific ones
//     let contains_prefix = |phrase| {
//         SET.contains_prefix(QueryPhrase::new(&get_prefix(phrase)).unwrap()).unwrap()
//     };
//
//     assert!(contains_prefix("8"));
//     assert!(contains_prefix("84"));
//     assert!(contains_prefix("84#"));
//     assert!(contains_prefix("84# "));
//     assert!(contains_prefix("84# G"));
//     assert!(contains_prefix("84# Suchava Dr"));
//
//     assert!(!contains_prefix("84# Suchava Dr Ln"));
//     assert!(!contains_prefix("Suchava Dr"));
//     // note that we don't test any that include words we don't know about -- in the broader
//     // scheme, that's not our job
// }
//
// fn get_full_variants(phrase: &str) -> Vec<Vec<QueryWord>> {
//     phrase.split(' ').map(
//         |w| DISTANCES[&WORDS[w]].iter().map(
//             |(id, distance)| QueryWord::new_full(*id, *distance)
//         ).collect::<Vec<_>>()
//     ).collect::<Vec<_>>()
// }
//
// fn get_prefix_variants(phrase: &str) -> Vec<Vec<QueryWord>> {
//     let words: Vec<&str> = phrase.split(' ').collect();
//     let mut out = words[..(words.len() - 1)].iter().map(
//         |w| DISTANCES.get(WORDS.get(w).unwrap()).unwrap().iter().map(
//             |(id, distance)| QueryWord::new_full(*id, *distance)
//         ).collect::<Vec<_>>()
//     ).collect::<Vec<Vec<QueryWord>>>();
//
//     let last = &words[words.len() - 1];
//     let prefix_match = WORDS.iter().filter(|(k, _v)| k.starts_with(last)).collect::<Vec<_>>();
//     let mut last_group = vec![QueryWord::new_prefix((*prefix_match[0].1, *prefix_match.last().unwrap().1))];
//     if let Some(id) = WORDS.get(last) {
//         for (id, distance) in DISTANCES.get(id).unwrap() {
//             if *distance == 1u8 {
//                 last_group.push(QueryWord::new_full(*id, *distance));
//             }
//         }
//     }
//     out.push(last_group);
//
//     out
// }
//
// #[test]
// fn sample_match_combinations() {
//     let correct = get_full("53# Country View Dr");
//     let no_typo = SET.match_combinations(&get_full_variants("53# Country View Dr"), 1).unwrap();
//     assert!(no_typo == vec![correct.clone()]);
//
//     let typo = SET.match_combinations(&get_full_variants("53# County View Dr"), 1).unwrap();
//     assert!(typo != vec![correct.clone()]);
// }
//
// #[test]
// fn sample_match_combinations_as_prefixes() {
//     let correct1 = get_prefix("53# Country");
//     let no_typo1 = SET.match_combinations_as_prefixes(&get_prefix_variants("53# Country"), 1).unwrap();
//     assert!(no_typo1 == vec![correct1.clone()]);
//
//     let typo1 = SET.match_combinations_as_prefixes(&get_prefix_variants("53# County"), 1).unwrap();
//     assert!(typo1 != vec![correct1.clone()]);
//
//     let correct2 = get_prefix("53# Country V");
//     let no_typo2 = SET.match_combinations_as_prefixes(&get_prefix_variants("53# Country V"), 1).unwrap();
//     assert!(no_typo2 == vec![correct2.clone()]);
//
//     let typo2 = SET.match_combinations_as_prefixes(&get_prefix_variants("53# County V"), 1).unwrap();
//     assert!(typo2 != vec![correct2.clone()]);
// }
//
// #[test]
// fn sample_contains_windows_simple() {
//     // just test everything
//     let max_phrase_dist = 2;
//     let ends_in_prefix = false;
//     for phrase in PHRASES.iter() {
//         let query_phrase = get_full(phrase);
//         let word_possibilities = get_full_variants(phrase);
//         let results = SET.match_combinations_as_windows(
//             &word_possibilities,
//             max_phrase_dist,
//             ends_in_prefix
//         ).unwrap();
//         assert!(results.len() > 0);
//         assert!(results.iter().any(|r| (&r.0, r.1) == (&query_phrase, false)));
//     }
// }
//
// #[test]
// fn sample_match_combinations_as_windows_all_full() {
//     // just test everything
//     let max_phrase_dist = 2;
//     for phrase in PHRASES.iter() {
//         let mut query_phrase = get_full(phrase);
//         let mut word_possibilities = get_full_variants(phrase);
//         // trim the last element to test prefix functionality
//         query_phrase.pop();
//         word_possibilities.pop();
//
//         let results = SET.match_combinations_as_windows(
//             &word_possibilities,
//             max_phrase_dist,
//             true
//         ).unwrap();
//
//         assert!(results.len() > 0);
//         assert!(results.iter().any(|r| (&r.0, r.1) == (&query_phrase, true)));
//     }
// }
//
// #[test]
// fn sample_match_combinations_as_windows_all_prefix() {
//     // just test everything
//     let max_phrase_dist = 2;
//     for phrase in PHRASES.iter() {
//         let query_phrase = get_prefix(phrase);
//         let word_possibilities = get_prefix_variants(phrase);
//
//         let results = SET.match_combinations_as_windows(
//             &word_possibilities,
//             max_phrase_dist,
//             true
//         ).unwrap();
//         assert!(results.len() > 0);
//         assert!(results.iter().any(|r| (&r.0, r.1) == (&query_phrase, true)));
//     }
// }
//
// #[test]
// fn sample_prefix_contains_windows_overlap() {
//     let word_possibilities = get_prefix_variants("84# Gleason Hollow Rd");
//     let results = SET.match_combinations_as_windows(
//         &word_possibilities,
//         1,
//         true
//     ).unwrap();
//
//     // this should match two different results, one of which is a prefix of the other, since we
//     // augmented the data with some prefix examples
//     assert_eq!(
//         results,
//         vec![
//             // this one doesn't end in the prefix
//             (get_full("84# Gleason Hollow"), false),
//             // but this one does
//             (get_prefix("84# Gleason Hollow Rd"), true),
//         ]
//     );
// }
//
// #[test]
// fn sample_prefix_contains_windows_substring() {
//     // this works
//     let word_possibilities = get_prefix_variants("59 Old Ne");
//     let results = SET.match_combinations_as_windows(
//         &word_possibilities,
//         1,
//         true
//     ).unwrap();
//     assert_eq!(results, vec![(get_prefix("59 Old Ne"), true)]);
//
//     // but this will fail because we can't window-recurse with eip=false and a PrefixWord
//     let results = SET.match_combinations_as_windows(
//         &word_possibilities,
//         1,
//         false
//     );
//     assert!(results.is_err());
//
//     // so let's try with just full words...
//     // except this also doesn't work when searched non-prefix
//     let word_possibilities = get_full_variants("59 Old New");
//     let results = SET.match_combinations_as_windows(
//         &word_possibilities,
//         1,
//         false
//     ).unwrap();
//     assert_eq!(results, vec![]);
//
//     // and it doesn't work with crap added to the end of it
//     let word_possibilities = get_prefix_variants("59 Old New Gleason");
//     let results = SET.match_combinations_as_windows(
//         &word_possibilities,
//         1,
//         true
//     ).unwrap();
//     assert_eq!(results, vec![]);
//
//     // or to the beginning -- we'd need to have a different start position
//     let word_possibilities = get_prefix_variants("Gleason 59 Old New");
//     let results = SET.match_combinations_as_windows(
//         &word_possibilities,
//         1,
//         true
//     ).unwrap();
//     assert_eq!(results, vec![]);
//
//     // on the other hand, we *should* be able to find a whole string with other stuff after it
//     let word_possibilities = get_prefix_variants("59 Old New Milford Rd Gleason");
//     let results = SET.match_combinations_as_windows(
//         &word_possibilities,
//         1,
//         true
//     ).unwrap();
//     assert_eq!(results, vec![(get_full("59 Old New Milford Rd"), false)]);
//
//     // and should also work with no prefixes
//     let word_possibilities = get_full_variants("59 Old New Milford Rd Gleason");
//     let results = SET.match_combinations_as_windows(
//         &word_possibilities,
//         1,
//         false
//     ).unwrap();
//     assert_eq!(results, vec![(get_full("59 Old New Milford Rd"), false)]);
//
//     // on the other hand, it still shouldn't work with stuff at the beginning
//     let word_possibilities = get_prefix_variants("Gleason 59 Old New Milford Rd");
//     let results = SET.match_combinations_as_windows(
//         &word_possibilities,
//         1,
//         true
//     ).unwrap();
//     assert_eq!(results, vec![]);
// }