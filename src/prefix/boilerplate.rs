use std::fmt;
use std::io;
use std::io::prelude::*;
use std::path::Path;
use std::fs::File;
use fst::Streamer;
use fst::raw;
use fst::Error as FstError;
use fst::automaton::{Automaton, AlwaysMatch};

// pretty much everything in this file is copied from either upstream fst::Set or upstream
// fst:Map, so it's quarantined in its own file to separate it from stuff we're actually building
// ourselves (mostly operations relevant to prefixes)

pub struct PrefixSet(raw::Fst);

impl PrefixSet {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, FstError> {
        let mut buffer: Vec<u8> = Vec::new();
        let mut file = io::BufReader::new(File::open(path)?);
        file.read_to_end(&mut buffer)?;
        buffer.shrink_to_fit();

        PrefixSet::from_bytes(buffer)
    }

    // these are lifted from upstream Set
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, FstError> {
        raw::Fst::from_bytes(bytes).map(PrefixSet)
    }

    pub fn from_iter<T, I>(iter: I) -> Result<Self, FstError>
            where T: AsRef<[u8]>, I: IntoIterator<Item=T> {
        let mut builder = PrefixSetBuilder::memory();
        builder.extend_iter(iter)?;
        PrefixSet::from_bytes(builder.into_inner()?)
    }

    pub fn contains<K: AsRef<[u8]>>(&self, key: K) -> bool {
        self.0.contains_key(key)
    }

    pub fn stream(&self) -> Stream {
        Stream::new(self.0.stream())
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn as_fst(&self) -> &raw::Fst {
        &self.0
    }

    // this one is from Map
    pub fn get<K: AsRef<[u8]>>(&self, key: K) -> Option<u64> {
        self.0.get(key).map(|output| output.value())
    }
}

// Also from Map
impl fmt::Debug for PrefixSet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PrefixSet([")?;
        let mut stream = self.stream();
        let mut first = true;
        while let Some((k, v)) = stream.next() {
            if !first {
                write!(f, ", ")?;
            }
            first = false;
            write!(f, "({}, {})", String::from_utf8_lossy(k), v)?;
        }
        write!(f, "])")
    }
}

pub struct PrefixSetBuilder<W> {
    builder: raw::Builder<W>,
    count: u64
}

impl PrefixSetBuilder<Vec<u8>> {
    pub fn memory() -> Self {
        PrefixSetBuilder { builder: raw::Builder::memory(), count: 0 }
    }
}

impl<W: Write> PrefixSetBuilder<W> {
    pub fn new(wtr: W) -> Result<PrefixSetBuilder<W>, FstError> {
        Ok(PrefixSetBuilder { builder: raw::Builder::new_type(wtr, 0)?, count: 0 })
    }

    pub fn insert<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), FstError> {
        // this is the main behavior change vs. upstream: enforce autoincrementing IDs
        self.builder.insert(key, self.count)?;
        self.count += 1;
        Ok(())
    }

    pub fn extend_iter<T, I>(&mut self, iter: I) -> Result<(), FstError>
            where T: AsRef<[u8]>, I: IntoIterator<Item=T> {
        for key in iter {
            // likewise, enforce counts
            self.builder.insert(key, self.count)?;
            self.count += 1;
        }
        Ok(())
    }

    pub fn finish(self) -> Result<(), FstError> {
        self.builder.finish()
    }

    pub fn into_inner(self) -> Result<W, FstError> {
        self.builder.into_inner()
    }
}

pub struct Stream<'s, A=AlwaysMatch>(raw::Stream<'s, A>) where A: Automaton;

impl<'s, A: Automaton> Stream<'s, A> {
                #[doc(hidden)]
    pub fn new(fst_stream: raw::Stream<'s, A>) -> Self {
        Stream(fst_stream)
    }

    pub fn into_str_vec(self) -> Result<Vec<(String, u64)>, FstError> {
        self.0.into_str_vec()
    }
}

impl<'a, 's, A: Automaton> Streamer<'a> for Stream<'s, A> {
    type Item = (&'a [u8], u64);

    fn next(&'a mut self) -> Option<Self::Item> {
        self.0.next().map(|(key, out)| (key, out.value()))
    }
}