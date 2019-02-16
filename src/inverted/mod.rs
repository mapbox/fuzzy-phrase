use std::io;

#[cfg(feature = "mmap")]
use std::path::Path;
#[cfg(feature = "mmap")]
use memmap;
#[cfg(feature = "mmap")]
use std::fs::File;

use std::collections::BTreeMap;
use std::error::Error;
use capnp::serialize;
use std::sync::Arc;

use super::{QueryWord, QueryPhrase};

pub mod inverted_index_capnp;

#[cfg(test)] mod tests;

pub struct InvertedIndex<T, U: capnp::message::ReaderSegments> {
    data: Option<T>,
    reader: capnp::message::Reader<U>,
    phrase_lookup_fn: Box<dyn Fn(u32) -> Vec<u32> + Send>
}

impl<T, U: capnp::message::ReaderSegments> InvertedIndex<T, U> {
    fn phrases_for_word(&self, word_id: u32) -> Result<Vec<u32>, Box<Error>> {
        let root = self.reader.get_root::<inverted_index_capnp::inverted_index::Reader>()?;
        let entry = root.get_entries()?.get(word_id);
        let count = entry.get_count();
        Ok(if count == 0 {
            vec![]
        } else {
            let compressed = entry.get_compressed_ids()?;
            fast_intersection::streamvbyte_delta_decode(compressed, count as usize, 0)
        })
    }

    fn get_intersection(&self, ids: &[u32]) -> Result<Vec<u32>, Box<Error>> {
        let mut matches: Option<Vec<u32>> = None;
        for id in ids {
            let phrases = self.phrases_for_word(*id)?;
            matches = match matches {
                Some(existing) => Some(fast_intersection::simd_intersection_avx2(&existing, &phrases)),
                None => Some(phrases)
            };
        }
        Ok(match matches {
            Some(intersection) => intersection,
            None => vec![]
        })
    }

    /// Test membership of a single phrase. Returns true iff the phrase matches a complete phrase
    /// in the set. Wraps the underlying Set::contains method.
    pub fn contains(&self, phrase: QueryPhrase) -> Result<bool, Box<Error>> {
        let ids: Vec<u32> = phrase.words.iter().map(|word| match word {
            QueryWord::Full { id, .. } => id.clone(),
            _ => panic!("no prefixes")
        }).collect();
        let intersection = self.get_intersection(&ids)?;
        Ok(intersection.len() > 0)
    }

    pub fn match_substring(&self, phrase: QueryPhrase) -> Result<Vec<Vec<QueryWord>>, Box<Error>> {
        let ids: Vec<u32> = phrase.words.iter().map(|word| match word {
            QueryWord::Full { id, .. } => id.clone(),
            _ => panic!("no prefixes")
        }).collect();
        let intersection = self.get_intersection(&ids)?;
        Ok(intersection.iter().map(|phrase_id| {
            let phrase = (self.phrase_lookup_fn)(*phrase_id);
            phrase.iter().map(|word_id| QueryWord::new_full(*word_id, 0)).collect()
        }).collect())
    }

    /// Test whether a query phrase can be found at the beginning of any phrase in the Set. Also
    /// known as a "starts with" search.
    pub fn contains_prefix(&self, phrase: QueryPhrase) -> Result<bool, Box<Error>>  {
        Ok(true)
    }

    /// Recursively explore the phrase graph looking for combinations of candidate words to see
    /// which ones match actual phrases in the phrase graph.
    pub fn match_combinations(
        &self,
        word_possibilities: &[Vec<QueryWord>],
        max_phrase_dist: u8
    ) -> Result<Vec<Vec<QueryWord>>, Box<Error>> {
        Ok(vec![])
    }

    /// Recursively explore the phrase graph looking for combinations of candidate words to see
    /// which ones match prefixes of actual phrases in the phrase graph.
    pub fn match_combinations_as_prefixes(
        &self,
        word_possibilities: &[Vec<QueryWord>],
        max_phrase_dist: u8
    ) -> Result<Vec<Vec<QueryWord>>, Box<Error>> {
        Ok(vec![])
    }

    /// Recursively explore the phrase graph looking for combinations of candidate words to see
    /// which ones match prefixes of actual phrases in the phrase graph.
    pub fn match_combinations_as_windows(
        &self,
        word_possibilities: &[Vec<QueryWord>],
        max_phrase_dist: u8,
        ends_in_prefix: bool
    ) -> Result<Vec<(Vec<QueryWord>, bool)>, Box<Error>> {
        Ok(vec![])
    }
}

// capnp::serialize::SliceSegments<'a>
impl InvertedIndex<Vec<u8>, capnp::serialize::OwnedSegments> {
    /// Create from a raw byte sequence, which must be written by `InvertedIndexBuilder`.
    pub fn from_bytes(bytes: Vec<u8>, phrase_lookup_fn: Box<dyn Fn(u32) -> Vec<u32> + Send>) -> Result<Self, Box<Error>> {
        let mut slice: &[u8] = &bytes;
        let reader = serialize::read_message(
            &mut slice,
            capnp::message::ReaderOptions::new()
        )?;
        Ok(InvertedIndex {
            data: None,
            reader: reader,
            phrase_lookup_fn: phrase_lookup_fn
        })
    }
}

#[cfg(feature = "mmap")]
impl<'a> InvertedIndex<memmap::Mmap, capnp::serialize::SliceSegments<'a>> {
    pub unsafe fn from_path<P: AsRef<Path>>(path: P, phrase_lookup_fn: Box<dyn Fn(u32) -> Vec<u32> + Send>) -> Result<Self, Box<Error>> {
        let in_file = File::open(path)?;
        let mmap = memmap::Mmap::map(&in_file)?;
        let static_slice = {
            let slice: &[u8] = mmap.as_ref();
            // this is probably really stupid
            std::mem::transmute::<&[u8], &'a [u8]>(slice)
        };
        let reader = capnp::serialize::read_message_from_words(
            capnp::Word::bytes_to_words(static_slice),
            *capnp::message::ReaderOptions::new().traversal_limit_in_words(1000000000)
        )?;
        Ok(InvertedIndex {
            data: Some(mmap),
            reader: reader,
            phrase_lookup_fn: phrase_lookup_fn
        })
    }
}

pub struct InvertedIndexBuilder<W> {
    writer: W,
    index: BTreeMap<u32, Vec<u32>>
}

impl InvertedIndexBuilder<Vec<u8>> {
    pub fn memory() -> Self {
        InvertedIndexBuilder { writer: Vec::new(), index: BTreeMap::new() }
    }

}

impl<W: io::Write> InvertedIndexBuilder<W> {

    pub fn new(wtr: W) -> Result<InvertedIndexBuilder<W>, Box<Error>> {
        Ok(InvertedIndexBuilder { writer: wtr, index: BTreeMap::new() })
    }

    /// Insert a phrase, specified as an array of word identifiers.
    pub fn insert(&mut self, phrase_id: u32, words: &[u32]) -> Result<(), Box<Error>> {
        for word in words {
            let word_vec = self.index.entry(*word).or_insert_with(|| Vec::new());
            word_vec.push(phrase_id);
        }
        Ok(())
    }

    pub fn into_inner(mut self) -> Result<W, Box<Error>> {
        let mut message = ::capnp::message::Builder::new_default();
        {
            let inverted_index = message.init_root::<inverted_index_capnp::inverted_index::Builder>();

            let num_words: u32 = match self.index.keys().next_back() {
                Some(max_word) => max_word + 1,
                _ => 0,
            };
            let mut entries = inverted_index.init_entries(num_words);

            let mut last_idx: isize = -1;
            for (word_id, phrases) in (&mut self).index.iter_mut() {
                let iword_id = *word_id as isize;
                if iword_id > last_idx + 1 {
                    for i in (last_idx + 1)..iword_id {
                        let mut entry = entries.reborrow().get(i as u32);
                        entry.set_count(0);
                        entry.set_compressed_ids(&[]);
                    }
                }

                phrases.sort();
                phrases.dedup();
                let compressed = fast_intersection::streamvbyte_delta_encode(phrases, 0);
                let mut entry = entries.reborrow().get(iword_id as u32);
                entry.set_count(phrases.len() as u32);
                entry.set_compressed_ids(&compressed);

                last_idx = iword_id;
            }
        }
        serialize::write_message(&mut self.writer, &message)?;
        Ok(self.writer)
    }

    pub fn finish(mut self) -> Result<(), Box<Error>> {
        self.into_inner()?;
        Ok(())
    }
}
