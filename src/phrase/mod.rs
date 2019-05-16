pub mod util;
pub mod query;

use std::io;
#[cfg(feature = "mmap")]
use std::path::Path;

use fst;
use fst::IntoStreamer;
use fst::raw::{CompiledAddr, Node, Fst, Builder, Output};

use self::util::{word_ids_to_key};
use self::util::PhraseSetError;
use self::query::QueryWord;

#[cfg(test)] mod tests;

type WordKey = [u8; 3];

pub struct PhraseSet(Fst);

/// PhraseSet is a lexicographically ordered set of phrases.
///
/// Phrases are sequences of words, where each word is represented as an integer. The integers
/// correspond to FuzzyMap values. Due to limitations in the fst library, however, the integers are
/// encoded as a series of 3 bytes.  For example, the three-word phrase "1## Main Street" will be
/// represented over 9 transitions, with one byte each.
///
/// | tokens  | integers  | three_bytes   |
/// |---------|-----------|---------------|
/// | 100     | 21        | [0,   0,  21] |
/// | main    | 457       | [0,   1, 201] |
/// | street  | 109821    | [1, 172, 253] |
///
impl PhraseSet {
    pub fn lookup(&self, phrase: &[QueryWord]) -> PhraseSetLookupResult {
        let fst = &self.0;
        let mut node = fst.root();
        let mut output = Output::zero();
        for word in phrase {
            match word {
                QueryWord::Full { key, .. } => {
                    for b in key.into_iter() {
                        node = if let Some(i) = node.find_input(*b) {
                            let t = node.transition(i);
                            output = output.cat(t.out);
                            fst.node(t.addr)
                        } else {
                            return PhraseSetLookupResult::NotFound;
                        }
                    }
                },
                QueryWord::Prefix { key_range, .. } => {
                    match self.matches_prefix_range(
                        node.addr(),
                        output,
                        *key_range
                    ) {
                        WordPrefixMatchResult::Found(match_state) => {
                            // we can return and stop looping -- the prefix is at the end
                            return PhraseSetLookupResult::Found { fst, match_state: PhraseSetMatchState::EndsInPrefix(match_state) };
                        },
                        WordPrefixMatchResult::NotFound => {
                            return PhraseSetLookupResult::NotFound;
                        }
                    }
                },
            }
        }
        PhraseSetLookupResult::Found { fst, match_state: PhraseSetMatchState::EndsInFullWord { node, output } }
    }

    /// Recursively explore the phrase graph looking for combinations of candidate words to see
    /// which ones match actual phrases in the phrase graph.
    pub fn match_combinations(
        &self,
        word_possibilities: &[Vec<QueryWord>],
        max_phrase_dist: u8
    ) -> Result<Vec<Vec<QueryWord>>, PhraseSetError> {
        // this is just a thin wrapper around a private recursive function, with most of the
        // arguments prefilled
        let fst = &self.0;
        let root = fst.root();
        let mut out: Vec<Vec<QueryWord>> = Vec::new();
        self.exact_recurse(word_possibilities, 0, &root, max_phrase_dist, Vec::new(), &mut out)?;
        Ok(out)
    }

    fn exact_recurse(
        &self,
        possibilities: &[Vec<QueryWord>],
        position: usize,
        node: &Node,
        budget_remaining: u8,
        so_far: Vec<QueryWord>,
        out: &mut Vec<Vec<QueryWord>>,
    ) -> Result<(), PhraseSetError> {
        let fst = &self.0;

        for word in possibilities[position].iter() {
            let (key, edit_distance) = match word {
                QueryWord::Full { key, edit_distance, .. } => (*key, *edit_distance),
                _ => return Err(PhraseSetError::new(
                    "The query submitted has a QueryWord::Prefix. This function only accepts QueryWord:Full"
                )),
            };
            if edit_distance > budget_remaining {
                break
            }

            // can we find the next word from our current position?
            let mut found = true;
            // make a mutable copy to traverse
            let mut search_node = node.to_owned();
            for b in key.into_iter() {
                if let Some(i) = search_node.find_input(*b) {
                    search_node = fst.node(search_node.transition_addr(i));
                } else {
                    found = false;
                    break;
                }
            }

            // only recurse or add a result if we the current word is in the graph in this position
            if found {
                let mut rec_so_far = so_far.clone();
                rec_so_far.push(word.clone());
                if position < possibilities.len() - 1 {
                    self.exact_recurse(
                        possibilities,
                        position + 1,
                        &search_node,
                        budget_remaining - edit_distance,
                        rec_so_far,
                        out,
                    )?;
                } else {
                    // if we're at the end of the line, we'll only keep this result if it's final
                    if search_node.is_final() {
                        out.push(rec_so_far);
                    }
                }
            }
        }
        Ok(())
    }

    /// Recursively explore the phrase graph looking for combinations of candidate words to see
    /// which ones match prefixes of actual phrases in the phrase graph.
    pub fn match_combinations_as_prefixes(
        &self,
        word_possibilities: &[Vec<QueryWord>],
        max_phrase_dist: u8
    ) -> Result<Vec<Vec<QueryWord>>, PhraseSetError> {
        // this is just a thin wrapper around a private recursive function, with most of the
        // arguments prefilled
        let fst = &self.0;
        let root = fst.root();
        let mut out: Vec<Vec<QueryWord>> = Vec::new();
        self.prefix_recurse(word_possibilities, 0, &root, max_phrase_dist, Vec::new(), &mut out)?;
        Ok(out)
    }

    fn prefix_recurse(
        &self,
        possibilities: &[Vec<QueryWord>],
        position: usize,
        node: &Node,
        budget_remaining: u8,
        so_far: Vec<QueryWord>,
        out: &mut Vec<Vec<QueryWord>>,
    ) -> Result<(), PhraseSetError> {
        let fst = &self.0;

        for word in possibilities[position].iter() {
            match word {
                QueryWord::Full { key, edit_distance, .. } => {
                    if *edit_distance > budget_remaining {
                        break
                    }

                    let mut found = true;
                    // make a mutable copy to traverse
                    let mut search_node = node.to_owned();
                    for b in key.into_iter() {
                        if let Some(i) = search_node.find_input(*b) {
                            search_node = fst.node(search_node.transition_addr(i));
                        } else {
                            found = false;
                            break;
                        }
                    }

                    // only recurse or add a result if we the current word is in the graph in
                    // this position
                    if found {
                        let mut rec_so_far = so_far.clone();
                        rec_so_far.push(word.clone());
                        if position < possibilities.len() - 1 {
                            self.prefix_recurse(
                                possibilities,
                                position + 1,
                                &search_node,
                                budget_remaining - edit_distance,
                                rec_so_far,
                                out,
                            )?;
                        } else {
                            out.push(rec_so_far);
                        }
                    }
                },
                QueryWord::Prefix { key_range, .. } => {
                    if let WordPrefixMatchResult::Found( .. ) = self.matches_prefix_range(
                        node.addr(),
                        Output::zero(),
                        *key_range
                    ) {
                        // presumably the prefix is at the end, so we don't need to consider the
                        // possibility of recursing, just of being done
                        let mut rec_so_far = so_far.clone();
                        rec_so_far.push(word.clone());
                        out.push(rec_so_far);
                    }
                },
            }
        }
        Ok(())
    }

    /// Recursively explore the phrase graph looking for combinations of candidate words to see
    /// which ones match prefixes of actual phrases in the phrase graph.
    pub fn match_combinations_as_windows(
        &self,
        word_possibilities: &[Vec<QueryWord>],
        max_phrase_dist: u8,
        ends_in_prefix: bool
    ) -> Result<Vec<(Vec<QueryWord>, bool)>, PhraseSetError> {
        // this is just a thin wrapper around a private recursive function, with most of the
        // arguments prefilled
        let fst = &self.0;
        let root = fst.root();
        let mut out: Vec<(Vec<QueryWord>, bool)> = Vec::new();
        self.window_recurse(word_possibilities, 0, &root, max_phrase_dist, ends_in_prefix, Vec::new(), &mut out)?;
        Ok(out)
    }

    fn window_recurse(
        &self,
        possibilities: &[Vec<QueryWord>],
        position: usize,
        node: &Node,
        budget_remaining: u8,
        ends_in_prefix: bool,
        so_far: Vec<QueryWord>,
        out: &mut Vec<(Vec<QueryWord>, bool)>,
    ) -> Result<(), PhraseSetError> {
        let fst = &self.0;

        for word in possibilities[position].iter() {
            match word {
                QueryWord::Full { key, edit_distance, .. } => {
                    if *edit_distance > budget_remaining {
                        break
                    }

                    let mut found = true;
                    // make a mutable copy to traverse
                    let mut search_node = node.to_owned();
                    for b in key.into_iter() {
                        if let Some(i) = search_node.find_input(*b) {
                            search_node = fst.node(search_node.transition_addr(i));
                        } else {
                            found = false;
                            break;
                        }
                    }

                    // only recurse or add a result if we the current word is in the graph in
                    // this position
                    if found {
                        // we want to add a result if we're at the end OR if we've hit a final
                        // node OR we're at the end of the phrase
                        let mut rec_so_far = so_far.clone();
                        rec_so_far.push(word.clone());
                        if position < possibilities.len() - 1 {
                            if search_node.is_final() {
                                out.push((rec_so_far.clone(), false));
                            }
                            self.window_recurse(
                                possibilities,
                                position + 1,
                                &search_node,
                                budget_remaining - edit_distance,
                                ends_in_prefix,
                                rec_so_far,
                                out,
                            )?;
                        } else {
                            // if we're at the end, require final node unless autocomplete is on
                            if search_node.is_final() || ends_in_prefix {
                                out.push((rec_so_far, ends_in_prefix));
                            }
                        }
                    }
                },
                QueryWord::Prefix { key_range, .. } => {
                    if !ends_in_prefix {
                        return Err(PhraseSetError::new(
                            "The query submitted has a QueryWord::Prefix. This function only accepts QueryWord:Full"
                        ))
                    }
                    if let WordPrefixMatchResult::Found( .. ) = self.matches_prefix_range(
                        node.addr(),
                        Output::zero(),
                        *key_range
                    ) {
                        // presumably the prefix is at the end, so we don't need to consider the
                        // possibility of recursing, just of being done; we can also assume AC is on
                        let mut rec_so_far = so_far.clone();
                        rec_so_far.push(word.clone());
                        out.push((rec_so_far, ends_in_prefix));
                    }
                },
            }
        }
        Ok(())
    }

    fn matches_prefix_range(&self, start_position: CompiledAddr, start_output: Output, key_range: (WordKey, WordKey)) -> WordPrefixMatchResult {
        let (sought_min_key, sought_max_key) = key_range;

		// self as fst
        let fst = &self.0;

        // get min value greater than or equal to the sought min
        let node0 = fst.node(start_position);
        for t0 in node0.transitions().skip_while(|t| t.inp < sought_min_key[0]) {
            let must_skip1 = t0.inp == sought_min_key[0];
            let node1 = fst.node(t0.addr);
            for t1 in node1.transitions() {
                if must_skip1 && t1.inp < sought_min_key[1] {
                    continue;
                }
                let must_skip2 = must_skip1 && t1.inp == sought_min_key[1];
                let node2 = fst.node(t1.addr);
                for t2 in node2.transitions() {
                    if must_skip2 && t2.inp < sought_min_key[2] {
                        continue;
                    }
                    // we've got three bytes! woohoo!
                    let mut next_after_min = [t0.inp, t1.inp, t2.inp];

                    if next_after_min <= sought_max_key {
                        // we found the first triple after the minimum,
                        // but we also need the last before the maximum

                        let max_node0 = fst.node(start_position);
                        for max_t0 in (0..max_node0.len()).rev().map(|i| max_node0.transition(i)).skip_while(|t| t.inp > sought_max_key[0]) {
                            let max_must_skip1 = max_t0.inp == sought_max_key[0];
                            let max_node1 = fst.node(max_t0.addr);
                            for max_t1 in (0..max_node1.len()).rev().map(|i| max_node1.transition(i)) {
                                if max_must_skip1 && max_t1.inp > sought_max_key[1] {
                                    continue;
                                }
                                let max_must_skip2 = max_must_skip1 && t1.inp == sought_max_key[1];
                                let max_node2 = fst.node(max_t1.addr);
                                for max_t2 in (0..max_node2.len()).rev().map(|i| max_node2.transition(i)) {
                                    if max_must_skip2 && max_t2.inp > sought_max_key[2] {
                                        continue;
                                    }
                                    // we've got three bytes! woohoo!
                                    return WordPrefixMatchResult::Found(WordPrefixMatchState {
                                        min_prefix_node: fst.node(t2.addr),
                                        min_prefix_output: start_output.cat(t0.out).cat(t1.out).cat(t2.out),
                                        max_prefix_node: fst.node(max_t2.addr),
                                        max_prefix_output: start_output.cat(max_t0.out).cat(max_t1.out).cat(max_t2.out)
                                    });
                                }
                            }
                        }

                        return WordPrefixMatchResult::Found(WordPrefixMatchState {
                            min_prefix_node: fst.node(t2.addr),
                            min_prefix_output: start_output.cat(t0.out).cat(t1.out).cat(t2.out),
                            max_prefix_node: fst.node(t2.addr),
                            max_prefix_output: start_output.cat(t0.out).cat(t1.out).cat(t2.out)
                        });
                    } else {
                        return WordPrefixMatchResult::NotFound;
                    }
                }
            }
        }
        WordPrefixMatchResult::NotFound
    }

    /// Create from a raw byte sequence, which must be written by `PhraseSetBuilder`.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, fst::Error> {
        Fst::from_bytes(bytes).map(PhraseSet)
    }

    #[cfg(feature = "mmap")]
    pub unsafe fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, fst::Error> {
        Fst::from_path(path).map(PhraseSet)
    }

}

impl<'s, 'a> IntoStreamer<'a> for &'s PhraseSet {
    type Item = (&'a [u8], fst::raw::Output);
    type Into = fst::raw::Stream<'s>;

    fn into_stream(self) -> Self::Into {
        self.0.stream()
    }
}

pub struct WordPrefixMatchState<'a> {
    min_prefix_node: Node<'a>,
    min_prefix_output: Output,
    max_prefix_node: Node<'a>,
    max_prefix_output: Output
}

enum WordPrefixMatchResult<'a> {
    NotFound,
    Found(WordPrefixMatchState<'a>)
}

pub enum PhraseSetMatchState<'a> {
    EndsInFullWord {
        node: Node<'a>,
        output: Output
    },
    EndsInPrefix(WordPrefixMatchState<'a>)
}

impl<'a> PhraseSetMatchState<'a> {
    // retrieves the min and max IDs of all phrases that begin with the given prefix (which may
    // itself begin either with a word prefix or a full word)
    pub fn prefix_range(&self, fst: &'a Fst) -> (Output, Output) {
        let (min_node, min_output, max_node, max_output) = match self {
            PhraseSetMatchState::EndsInFullWord { node, output } => {
                (node, output, node, output)
            },
            PhraseSetMatchState::EndsInPrefix(state) => {
                (&state.min_prefix_node, &state.min_prefix_output, &state.max_prefix_node, &state.max_prefix_output)
            }
        };

        let start = min_output.cat(min_node.final_output());

        let mut max_node: Node = max_node.to_owned();
        let mut max_output: Output = max_output.to_owned();

        while max_node.len() != 0 {
            let t = max_node.transition(max_node.len() - 1);
            max_output = max_output.cat(t.out);
            max_node = fst.node(t.addr);
        }
        (start, max_output.cat(max_node.final_output()))
    }
}

pub enum PhraseSetLookupResult<'a> {
    NotFound,
    Found { fst: &'a Fst, match_state: PhraseSetMatchState<'a> }
}

impl<'a> PhraseSetLookupResult<'a> {
    pub fn found(&self) -> bool {
        match self {
            PhraseSetLookupResult::NotFound => false,
            PhraseSetLookupResult::Found {..} => true
        }
    }

    pub fn found_final(&self) -> bool {
        match self {
            PhraseSetLookupResult::NotFound => false,
            PhraseSetLookupResult::Found { match_state, .. } => {
                match match_state {
                    PhraseSetMatchState::EndsInFullWord { node, .. } => node.is_final(),
                    PhraseSetMatchState::EndsInPrefix(..) => false
                }
            }
        }
    }

    pub fn id(&self) -> Option<Output> {
        match self {
            PhraseSetLookupResult::NotFound => None,
            PhraseSetLookupResult::Found { match_state, .. } => {
                match match_state {
                    PhraseSetMatchState::EndsInFullWord { node, output } => {
                        if node.is_final() {
                            Some(output.cat(node.final_output()))
                        } else {
                            None
                        }
                    },
                    PhraseSetMatchState::EndsInPrefix(..) => None
                }
            }
        }
    }

    pub fn range(&self) -> Option<(Output, Output)> {
        match self {
            PhraseSetLookupResult::NotFound => None,
            PhraseSetLookupResult::Found { fst, match_state } => Some(match_state.prefix_range(fst))
        }
    }

    pub fn has_continuations(&self) -> bool {
        match self {
            PhraseSetLookupResult::NotFound => false,
            PhraseSetLookupResult::Found { match_state, .. } => {
                match match_state {
                    PhraseSetMatchState::EndsInFullWord { node, .. } => node.len() > 0,
                    PhraseSetMatchState::EndsInPrefix(state) => {
                        state.min_prefix_node.len() > 0 || (state.min_prefix_node.addr() != state.max_prefix_node.addr())
                    }
                }
            }
        }
    }
}

pub struct PhraseSetBuilder<W> {
    builder: Builder<W>,
    count: u64
}

impl PhraseSetBuilder<Vec<u8>> {
    pub fn memory() -> Self {
        PhraseSetBuilder { builder: Builder::memory(), count: 0 }
    }
}

impl<W: io::Write> PhraseSetBuilder<W> {
    pub fn new(wtr: W) -> Result<PhraseSetBuilder<W>, fst::Error> {
        Ok(PhraseSetBuilder { builder: Builder::new_type(wtr, 0)?, count: 0 })
    }

    /// Insert a phrase, specified as an array of word identifiers.
    pub fn insert(&mut self, phrase: &[u32]) -> Result<(), fst::Error> {
        let key = word_ids_to_key(phrase);
        self.builder.insert(key, self.count)?;
        self.count += 1;
        Ok(())
    }

    pub fn into_inner(self) -> Result<W, fst::Error> {
        self.builder.into_inner()
    }

    pub fn finish(self) -> Result<(), fst::Error> {
        self.builder.finish()
    }
}
