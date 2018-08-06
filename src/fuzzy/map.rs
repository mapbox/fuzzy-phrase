use std::fs;
use std::error::Error;
use std::cmp::{min, Ordering};
use itertools::Itertools;
use fst::raw;
use fst::Error as FstError;
use std::path::{Path, PathBuf};
use std::io::{BufReader, BufWriter, Read};
use serde::{Deserialize, Serialize};
use rmps::{Deserializer, Serializer};
use std::io::{Error as IoError, ErrorKind as IoErrorKind};

use fuzzy::util::multi_modified_damlev_hint;

static MULTI_FLAG: u64 = 1 << 63;
static MULTI_MASK: u64 = !(1 << 63);

pub struct FuzzyMap {
    id_list: Vec<Vec<u32>>,
    fst: raw::Fst
}

#[derive(Serialize, Deserialize)]
pub struct SerializableIdList(Vec<Vec<u32>>);

#[derive(PartialEq, Eq, Debug)]
pub struct FuzzyMapLookupResult {
    pub word: String,
    pub id: u32,
    pub edit_distance: u8,
}

impl Ord for FuzzyMapLookupResult {
    fn cmp(&self, other: &FuzzyMapLookupResult) -> Ordering {
        (self.edit_distance, self.id, &self.word).cmp(&(other.edit_distance, other.id, &other.word))
    }
}

impl PartialOrd for FuzzyMapLookupResult {
    fn partial_cmp(&self, other: &FuzzyMapLookupResult) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl FuzzyMap {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, FstError> {
        let file_start = path.as_ref();

        let mut buffer: Vec<u8> = Vec::new();
        let mut file = BufReader::new(fs::File::open(file_start.with_extension("fst"))?);
        file.read_to_end(&mut buffer)?;
        buffer.shrink_to_fit();
        let fst = raw::Fst::from_bytes(buffer)?;

        let mf_reader = BufReader::new(fs::File::open(file_start.with_extension("msg"))?);
        let id_list: SerializableIdList = Deserialize::deserialize(&mut Deserializer::new(mf_reader)).unwrap();
        Ok(FuzzyMap { id_list: id_list.0, fst: fst })
    }

    fn find_matching_variants(&self, query: &[u8], indices: &[usize], position: usize, edit_distance: usize, node: &raw::Node, so_far: u64, out: &mut Vec<u64>) {
        if (indices.len() - 1 - position) <= edit_distance {
            // we're to the end of our string or within the edit distance
            // so if we're on a final string, emit output
            if node.is_final() {
                out.push(so_far + node.final_output().value());
            }
        }

        for i in position..min(position + edit_distance + 1, indices.len() - 1) {
            let mut found = true;
            let mut search_node = node.to_owned();
            let mut search_output = 0;
            for byte in &query[indices[i]..indices[i+1]] {
                if let Some(x) = search_node.find_input(*byte) {
                    let trans = search_node.transition(x);
                    search_output += trans.out.value();
                    search_node = self.fst.node(trans.addr);
                } else {
                    found = false;
                    break;
                }
            }
            if found {
                self.find_matching_variants(query, indices, i + 1, edit_distance - (i - position), &search_node, so_far + search_output, out);
            }
        }
    }

    fn find_matching_variants_ascii(&self, query: &[u8], position: usize, edit_distance: usize, node: &raw::Node, so_far: u64, out: &mut Vec<u64>) {
        if (query.len() - position) <= edit_distance {
            // we're to the end of our string or within the edit distance
            // so if we're on a final string, emit output
            if node.is_final() {
                out.push(so_far + node.final_output().value());
            }
        }

        for i in position..min(position + edit_distance + 1, query.len()) {
            if let Some(x) = node.find_input(query[i]) {
                let trans = node.transition(x);
                self.find_matching_variants_ascii(query, i + 1, edit_distance - (i - position), &self.fst.node(trans.addr), so_far + trans.out.value(), out);
            }
        }
    }

    pub fn lookup<'a, F>(&self, query: &str, edit_distance: u8, lookup_fn: F) -> Result<Vec<FuzzyMapLookupResult>, Box<Error>> where F: Fn(u32) -> &'a str {
        let mut matches = Vec::<u32>::new();

        let mut variant_ids: Vec<u64> = Vec::new();
        if query.is_ascii() {
            self.find_matching_variants_ascii(query.as_bytes(), 0, edit_distance as usize, &self.fst.root(), 0, &mut variant_ids);
        } else {
            let mut query_indices = query.char_indices().map(|(i, _c)| i).collect::<Vec<_>>();
            query_indices.push(query.len());
            self.find_matching_variants(query.as_bytes(), &query_indices, 0, edit_distance as usize, &self.fst.root(), 0, &mut variant_ids);
        }

        // check the query itself and the variants
        for uidx in variant_ids {
            if uidx & MULTI_FLAG != 0 {
                for x in &(self.id_list)[(uidx & MULTI_MASK) as usize] {
                    matches.push(*x as u32);
                }
            } else {
                matches.push(uidx as u32);
            }
        }
        //return all ids that match
        matches.sort();
        matches.dedup();

        let match_words = matches.iter().map(|id| lookup_fn(*id)).collect::<Vec<_>>();
        let distances = multi_modified_damlev_hint(query, &match_words, edit_distance as u32);

        let mut out = matches
            .into_iter()
            .enumerate()
            .filter_map(|(i, id)| {
                if distances[i] <= edit_distance as u32 {
                    Some(FuzzyMapLookupResult { word: match_words[i].to_owned(), id: id as u32, edit_distance: distances[i] as u8 })
                } else {
                    None
                }
            })
            .collect::<Vec<FuzzyMapLookupResult>>();
        out.sort();
        Ok(out)
    }
}

pub struct FuzzyMapBuilder {
    id_builder: Vec<Vec<u32>>,
    builder: raw::Builder<BufWriter<fs::File>>,
    file_path: PathBuf,
    word_variants: Vec<(String, u32)>,
    edit_distance: u8,
}

impl FuzzyMapBuilder {
    pub fn new<P: AsRef<Path>>(path: P, edit_distance: u8) -> Result<Self, Box<Error>> {
        let file_start = path.as_ref().to_owned();
        let fst_wtr = BufWriter::new(fs::File::create(file_start.with_extension("fst"))?);

        Ok(FuzzyMapBuilder {
            builder: raw::Builder::new_type(fst_wtr, 0)?,
            id_builder: Vec::<Vec<u32>>::new(),
            file_path: file_start,
            word_variants: Vec::<(String, u32)>::new(),
            edit_distance: edit_distance,
        })
    }

    pub fn build_from_iter<'a, T, P: AsRef<Path>>(path: P, words: T, edit_distance: u8) -> Result<(), Box<Error>> where T: Iterator<Item=&'a str> {
        let mut fuzzy_map_builder = FuzzyMapBuilder::new(path, edit_distance)?;

        for (i, word) in words.enumerate() {
            fuzzy_map_builder.insert(word, i as u32);
        }
        fuzzy_map_builder.finish()?;
        Ok(())
    }

    pub fn insert(&mut self, key: &str, id: u32) -> () {
        self.word_variants.push((key.to_owned(), id));
        let variants = super::get_variants(&key, self.edit_distance);
        for j in variants.into_iter() {
            self.word_variants.push((j, id));
        }
    }

    pub fn finish(mut self) -> Result<(), FstError> {
        self.word_variants.sort();

        for (key, group) in &(&self.word_variants).iter().dedup().group_by(|t| &t.0) {
            let opts = group.collect::<Vec<_>>();
            let id = if opts.len() == 1 {
                opts[0].1 as u64
            } else {
                self.id_builder.push((&opts).iter().map(|t| t.1).collect::<Vec<_>>());
                (self.id_builder.len() - 1) as u64 | MULTI_FLAG
            };
            self.builder.insert(key, id)?;
        }
        let mf_wtr = BufWriter::new(fs::File::create(self.file_path.with_extension("msg"))?);
        match SerializableIdList(self.id_builder).serialize(&mut Serializer::new(mf_wtr)) {
            Err(_e) => return Err(FstError::Io(IoError::new(IoErrorKind::InvalidInput, "File exists and is not a directory"))),
            Ok(()) => ()
        };
        self.builder.finish()
    }
}

#[cfg(test)]
mod tests {
    extern crate tempfile;
    extern crate lazy_static;

    use std::collections::BTreeSet;

    use super::*;
    use fuzzy::util::multi_modified_damlev;

    lazy_static! {
        static ref DATA: [&'static str; 4] = [
            include_str!("../../benches/data/phrase_test_shared_prefix.txt"),
            include_str!("../../benches/data/phrase_test_typos.txt"),
            include_str!("../../benches/data/phrase_test_cities_ar.txt"),
            include_str!("../../benches/data/phrase_test_cities_ru.txt"),
        ];
        static ref WORDS: Vec<&'static str> = {
            let mut bts: BTreeSet<&'static str> = BTreeSet::new();
            for data in DATA.iter() {
                let phrases = data.trim().split("\n").collect::<Vec<&str>>();
                for phrase in phrases {
                    let words = phrase.trim().split(" ");
                    for word in words {
                        bts.insert(word);
                    }
                }
            }
            bts.into_iter().collect()
        };
        static ref MAP_D1: FuzzyMap = {
            let dir = tempfile::tempdir().unwrap();
            let file_start = dir.path().join("fuzzy");
            FuzzyMapBuilder::build_from_iter(&file_start, WORDS.iter().cloned(), 1).unwrap();

            FuzzyMap::from_path(&file_start).unwrap()
        };
        static ref MAP_D2: FuzzyMap = {
            let dir = tempfile::tempdir().unwrap();
            let file_start = dir.path().join("fuzzy");
            FuzzyMapBuilder::build_from_iter(&file_start, WORDS.iter().cloned(), 2).unwrap();

            FuzzyMap::from_path(&file_start).unwrap()
        };
    }

    fn expect(word: &'static str, query: &'static str) -> FuzzyMapLookupResult {
        FuzzyMapLookupResult { word: word.to_owned(), id: WORDS.binary_search(&word).unwrap() as u32, edit_distance: multi_modified_damlev(&word, &[&query])[0] as u8 }
    }

    fn get_word(id: u32) -> &'static str {
        WORDS[id as usize]
    }

    #[test]
    fn build_d1() {
        lazy_static::initialize(&MAP_D1);
    }

    #[test]
    fn lookup_test_exact_d_1() {
        let query = "Shelton";
        let matches = MAP_D1.lookup(&query, 1, get_word);
        assert_eq!(matches.unwrap(), [expect("Shelton", query)]);

        //exact lookup, the original word in the data is - "agߪkaधaݤcݤkaqag"
        let query = "Москва";
        let matches = MAP_D1.lookup(&query, 1, get_word);
        assert_eq!(matches.unwrap(), [expect("Москва", query)]);
    }

    #[test]
    fn lookup_test_approx_d1() {
        //not exact lookup, the original word is - "Shelton", d=1
        let query = "Shleton";
        let matches = MAP_D1.lookup(&query, 1, get_word);
        assert_eq!(matches.unwrap(), [expect("Shelton", query)]);

        //exact lookup, the original word in the data is - "Москва"
        let query = "Москва";
        let matches = MAP_D1.lookup(&query, 1, get_word);
        assert_eq!(matches.unwrap(), [expect("Москва", query)]);

        //not exact lookup, d=1, more more than one suggestion because of two similiar words in the data
        //albana and albazan
        let query = "Christina";
        let matches = MAP_D1.lookup(&query, 1, get_word);
        assert_eq!(matches.unwrap(), [expect("Christian", query), expect("Christiana", query)]);

        //include a test that explores multiple results that share an fst entry
        let query = "Grayton";
        let matches = MAP_D1.lookup(&query, 1, get_word);
        assert_eq!(matches.unwrap(), [expect("Brayton", query), expect("Drayton", query)]);

        let query = "Keedy";
        let matches = MAP_D2.lookup(&query, 1, get_word);
        assert_eq!(matches.unwrap(), vec![])
    }

    #[test]
    fn lookup_test_garbage_d1() {
        let one_char_results: Vec<&'static str> = WORDS.iter().filter(|w| w.len() == 1).map(|w| *w).collect();
        //garbage input
        let query = "🤔";
        let matches = MAP_D1.lookup(&query, 1, get_word);
        assert_eq!(matches.unwrap(), one_char_results.iter().map(|w| expect(w, query)).collect::<Vec<_>>());

        let query = "";
        let matches = MAP_D1.lookup(&query, 1, get_word);
        assert_eq!(matches.unwrap(), one_char_results.iter().map(|w| expect(w, query)).collect::<Vec<_>>());
    }

    #[test]
    fn build_d2() {
        lazy_static::initialize(&MAP_D2);
    }

    #[test]
    fn lookup_test_cases_d_2() {
        let query = "Keedy";
        let matches = MAP_D2.lookup(&query, 2, get_word);
        assert_eq!(matches.unwrap(), [expect("Keesey", query), expect("Kennedy", query)])
    }
}
