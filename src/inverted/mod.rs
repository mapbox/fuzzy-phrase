use std::io;

#[cfg(feature = "mmap")]
use std::path::Path;
#[cfg(feature = "mmap")]
use memmap;
#[cfg(feature = "mmap")]
use std::fs::File;

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Arc;
use owning_ref::{self, OwningHandle};

use super::{QueryWord, QueryPhrase};

mod inverted_index_generated;
pub use self::inverted_index_generated::{Entry, InvertedIndex as FbInvertedIndex, EntryArgs, InvertedIndexArgs, get_root_as_inverted_index};

#[cfg(test)] mod tests;

pub struct InvertedIndex<T> where T: owning_ref::StableAddress {
    reader: OwningHandle<T, Box<FbInvertedIndex<'static>>>,
    phrase_lookup_fn: Box<dyn Fn(u32) -> Vec<u32> + Send + Sync>
}

impl<T> InvertedIndex<T> where T: owning_ref::StableAddress {
    fn phrases_for_word(&self, word_id: u32) -> Result<Vec<u32>, Box<Error>> {
        let entry = self.reader.entries().unwrap().get(word_id as usize);
        let count = entry.count();
        Ok(if count == 0 {
            vec![]
        } else {
            let compressed = entry.compressed_ids().unwrap();
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

impl InvertedIndex<Vec<u8>> {
    /// Create from a raw byte sequence, which must be written by `InvertedIndexBuilder`.
    pub fn from_bytes(bytes: Vec<u8>, phrase_lookup_fn: Box<dyn Fn(u32) -> Vec<u32> + Send + Sync>) -> Result<Self, Box<Error>> {
        let reader: OwningHandle<Vec<u8>, Box<FbInvertedIndex<'static>>> = OwningHandle::new_with_fn(bytes, |v| {
            let data: &[u8] = unsafe { &*v };
            Box::new(get_root_as_inverted_index(data))
        });
        Ok(InvertedIndex {
            reader: reader,
            phrase_lookup_fn: phrase_lookup_fn
        })
    }
}

#[cfg(feature = "mmap")]
impl InvertedIndex<Box<memmap::Mmap>> {
    pub unsafe fn from_path<P: AsRef<Path>>(path: P, phrase_lookup_fn: Box<dyn Fn(u32) -> Vec<u32> + Send + Sync>) -> Result<Self, Box<Error>> {
        let in_file = File::open(path)?;
        let mmap = memmap::Mmap::map(&in_file)?;
        let reader: OwningHandle<Box<memmap::Mmap>, Box<FbInvertedIndex<'static>>> = OwningHandle::new_with_fn(Box::new(mmap), |m| {
            let mmap: &memmap::Mmap = unsafe { &*m };
            let slice: &[u8] = mmap.as_ref();
            Box::new(get_root_as_inverted_index(slice))
        });
        Ok(InvertedIndex {
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
        let mut fb_builder = flatbuffers::FlatBufferBuilder::new_with_capacity(1024);

        let num_words: u32 = match self.index.keys().next_back() {
            Some(max_word) => max_word + 1,
            _ => 0,
        };
        let mut entries: Vec<_> = Vec::new();
        let empty: [u8; 0] = [];

        let mut last_idx: isize = -1;
        for (word_id, phrases) in (&mut self).index.iter_mut() {
            let iword_id = *word_id as isize;
            if iword_id > last_idx + 1 {
                for i in (last_idx + 1)..iword_id {
                    let compressed_ids = fb_builder.create_vector(&empty);
                    let entry = Entry::create(&mut fb_builder, &EntryArgs{count: 0, compressed_ids: Some(compressed_ids)});
                    entries.push(entry);
                }
            }

            phrases.sort();
            phrases.dedup();
            let compressed = fast_intersection::streamvbyte_delta_encode(phrases, 0);
            let fb_compressed = fb_builder.create_vector(&compressed);
            let entry = Entry::create(&mut fb_builder, &EntryArgs{count: phrases.len() as u32, compressed_ids: Some(fb_compressed)});
            entries.push(entry);

            last_idx = iword_id;
        }

        let fb_entries = fb_builder.create_vector(&entries);
        let fb_index = FbInvertedIndex::create(&mut fb_builder, &InvertedIndexArgs{entries: Some(fb_entries)});

        fb_builder.finish(fb_index, None);
        self.writer.write(fb_builder.finished_data())?;
        Ok(self.writer)
    }

    pub fn finish(mut self) -> Result<(), Box<Error>> {
        self.into_inner()?;
        Ok(())
    }
}
