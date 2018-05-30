use super::util;

/// An abstraction over full words and prefixes.
#[derive(Copy, Clone)]
pub enum QueryWord {
    /// A `Full` word is a word that has an identifier and is one of the members of a PrefixSet.
    Full {
        id: u32,
        edit_distance: u8,
    },

    /// A `Prefix` is a string that is the prefix to more than one full word, and includes an id_range field,
    /// which of identifiers.
    Prefix {
        id_range: (u32, u32),
    },
}

impl QueryWord
{
    pub fn to_string<'a, T:Fn(u32) -> &'a str>(&self, id_to_string: T) -> String {
        match &self {
            &QueryWord::Full {id, ..} => {
                let s = format!("{}", id_to_string(*id));
                return s
            },
            &QueryWord::Prefix {id_range, ..} => {
                let s_start: &str = id_to_string(id_range.0);
                let s_end: &str = id_to_string(id_range.1);
                let s = format!("{}..{}", s_start, s_end);
                return s
            }
        }
    }
}

/// A specialized container for a sequence of `QueryWord`s.
///
/// It allows iterating over a sequence of `QueryWord`s without taking ownership of them.  the `words`
/// field contains a _slice_ of _references_ to `QueryWord`s. This is helpful design because we'll often
/// be combining and re-combining the elements of multiple `Vec<QueryWord>`s. See the
/// `multiple_combinations` test below for an example. It's important that we work on references to
/// the elements of those arrays so that they can be re-used.
///
/// Because the `words` fields is made up of pointers, the lifetime annotations are necessary to
/// make sure that the sequence of words used to make the phrase does not go out of scope.
pub struct QueryPhrase<'a> {
    pub length: usize,
    pub words: &'a[QueryWord],
    pub has_prefix: bool,
}

impl<'a> QueryPhrase<'a> {

    pub fn new<T: AsRef<[QueryWord]>> (words: &'a T) -> Result<QueryPhrase<'a>, util::PhraseSetError> {
        let length: usize = words.as_ref().len();
        let has_prefix: bool = match words.as_ref()[length - 1] {
            QueryWord::Full {..} => false,
            QueryWord::Prefix {..} => true,
        };
        // disallow prefixes in any position except the final position
        for i in 0..length-1 {
            match words.as_ref()[i] {
                QueryWord::Prefix {..} => {
                    return Err(util::PhraseSetError::new(
                            "QueryPhrase may only have QueryWord::Prefix in final position."));
                },
                _ => ()
            }
        }

        Ok(QueryPhrase { words: words.as_ref(), length, has_prefix })
    }

    /// Return the length of the phrase (number of words)
    pub fn len(&self) -> usize {
        self.length
    }

    /// Sum the edit distances of the full words in the phrase
    pub fn total_edit_distance(&self) -> u8 {
        let mut total_edit_distance = 0;
        for word in self.words {
            match word {
                QueryWord::Full{ ref edit_distance, .. } => {
                    total_edit_distance += *edit_distance;
                },
                _ => (),
            }
        }
        total_edit_distance
    }

    /// Generate a key from the ids of the full words in this phrase
    pub fn full_word_key(&self) -> Vec<u8> {
        let mut full_word_key: Vec<u8> = vec![];
        for word in self.words {
            match word {
                QueryWord::Full{ ref id, .. } => {
                    let three_bytes = util::three_byte_encode(*id);
                    full_word_key.extend(three_bytes);
                },
                _ => (),
            }
        }
        full_word_key
    }

    /// Generate a key from the prefix range
    pub fn prefix_key_range(&self) -> Option<(Vec<u8>, Vec<u8>)> {
        let prefix_range = match self.words[self.length - 1] {
            QueryWord::Prefix{ ref id_range, .. } => {
                *id_range
            },
            _ => return None,
        };
        let prefix_start_key = util::three_byte_encode(prefix_range.0);
        let prefix_end_key = util::three_byte_encode(prefix_range.1);

        Some((prefix_start_key, prefix_end_key))
    }

    /// Generate a key from the prefix range
    pub fn prefix_id_range(&self) -> Option<(u32, u32)> {
        let prefix_range = match self.words[self.length - 1] {
            QueryWord::Prefix{ ref id_range, .. } => {
                *id_range
            },
            _ => return None,
        };
        Some(prefix_range)
    }

}

#[derive(Copy, Clone)]
pub struct QueryPhraseIterator<'a> {
    phrase: &'a QueryPhrase<'a>,
    offset: usize,
}

impl<'a> Iterator for QueryPhraseIterator<'a> {
    type Item = &'a QueryWord;

    fn next(&mut self) -> Option<&'a QueryWord> {
        if self.offset >= self.phrase.length {
            return None
        } else {
            let ref word = self.phrase.words[self.offset];
            self.offset += 1;
            Some(word)
        }
    }
}

impl<'a> IntoIterator for &'a QueryPhrase<'a> {
    type Item = &'a QueryWord;
    type IntoIter = QueryPhraseIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        QueryPhraseIterator{ phrase: self, offset: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn query_word_to_string() {
        let mut id_to_string_map = HashMap::new();

        id_to_string_map.insert(1u32, String::from("100"));
        id_to_string_map.insert(61_528u32, String::from("main"));
        id_to_string_map.insert(561_528u32, String::from("st"));

        let query_word = QueryWord::Full{ id: 61_528u32, edit_distance: 0 };

        let id_to_string_closure = |id: u32| id_to_string_map.get(&id).unwrap().as_str();

        let s = query_word.to_string(id_to_string_closure);
        assert_eq!(String::from("main"), s);
    }

    #[test]
    fn phrase_from_words() {
        let words = vec![
            QueryWord::Full{ id: 1u32, edit_distance: 0 },
            QueryWord::Full{ id: 61_528u32, edit_distance: 0 },
            QueryWord::Full{ id: 561_528u32, edit_distance: 0 },
        ];

        let phrase = QueryPhrase::new(&words).unwrap();
        assert_eq!(3, phrase.len());
        assert_eq!(false, phrase.has_prefix);
        assert_eq!(
            vec![
                0u8, 0u8,   1u8,     // 1
                0u8, 240u8, 88u8,    // 61_528
                8u8, 145u8, 120u8,   // 561_528
            ],
            phrase.full_word_key()
        );

        let shingle_one = &words[0..2];
        let shingle_one_query = QueryPhrase::new(&shingle_one).unwrap();
        assert_eq!(2, shingle_one_query.len());

        let shingle_two = &words[1..3];
        let shingle_two_query = QueryPhrase::new(&shingle_two).unwrap();
        assert_eq!(2, shingle_two_query.len());

    }

    #[test]
    fn phrase_multiple_combinations() {
        // three words, two variants for third word
        let words = vec![
            vec![ QueryWord::Full{ id: 1u32, edit_distance: 0 } ],
            vec![ QueryWord::Full{ id: 61_528u32, edit_distance: 0 } ],
            vec![
                QueryWord::Full{ id: 561_235u32, edit_distance: 0 },
                QueryWord::Full{ id: 561_247u32, edit_distance: 2 },
            ],
        ];

        let word_seq_a = [ words[0][0], words[1][0], words[2][0] ];
        let phrase_a = QueryPhrase::new(&word_seq_a).unwrap();

        assert_eq!(0, phrase_a.total_edit_distance());
        assert_eq!(false, phrase_a.has_prefix);
        assert_eq!(
            vec![
                0u8, 0u8,   1u8,    // 1
                0u8, 240u8, 88u8,   // 61_528
                8u8, 144u8, 83u8,   // 561_235
            ],
            phrase_a.full_word_key()
        );

        let mut word_ids = vec![];

        for word in phrase_a.into_iter() {
            match word {
                &QueryWord::Full{ ref id, .. } => {
                    word_ids.push(*id);
                },
                _ => {
                    panic!("Should be all full words");
                }
            }
        }

        // should be 2 full words, 2 ids
        assert_eq!(vec![1u32, 61_528u32, 561_235u32], word_ids);

        let word_seq_b = [ words[0][0], words[1][0], words[2][1] ];
        let phrase_b = QueryPhrase::new(&word_seq_b).unwrap();
        assert_eq!(2, phrase_b.total_edit_distance());
        assert_eq!(false, phrase_b.has_prefix);
        assert_eq!(
            vec![
                0u8, 0u8,   1u8,    // 1
                0u8, 240u8, 88u8,   // 61_528
                8u8, 144u8, 95u8,   // 561_247
            ],
            phrase_b.full_word_key()
        );

        let mut word_ids = vec![];

        for word in phrase_b.into_iter() {
            match word {
                &QueryWord::Full{ ref id, .. } => {
                    word_ids.push(*id);
                },
                _ => {
                    panic!("Should be all full words");
                }
            }
        }

        // should be 2 full words, 2 ids
        assert_eq!(vec![1u32, 61_528u32, 561_247u32], word_ids);
    }

    #[test]
    fn two_fuzzy_matches() {

        let words = vec![
            QueryWord::Full{ id: 1u32, edit_distance: 1 },
            QueryWord::Full{ id: 61_528u32, edit_distance: 2 },
        ];
        let phrase = QueryPhrase::new(&words).unwrap();

        assert_eq!(3, phrase.total_edit_distance());
        assert_eq!(false, phrase.has_prefix);
        assert_eq!(
            vec![
                0u8, 0u8,   1u8,     // 1
                0u8, 240u8, 88u8,    // 61_528
            ],
            phrase.full_word_key()
        );
        assert_eq!(None, phrase.prefix_key_range());

        let mut word_count = 0;
        let mut word_ids = vec![];

        let phrase_iter = phrase.into_iter();

        for word in phrase_iter {
            match word {
                &QueryWord::Full{ ref id, .. } => {
                    word_count += 1;
                    word_ids.push(*id);
                },
                _ => {
                    panic!("Should be all full words");
                }
            }
        }

        // should be 2 full words, 2 ids
        assert_eq!(2, word_count);
        assert_eq!(vec![1, 61_528], word_ids);

    }


    #[test]
    fn two_exact_matches_one_prefix() {

        let words = vec![
            QueryWord::Full{ id: 1u32, edit_distance: 0 },
            QueryWord::Full{ id: 61_528u32, edit_distance: 0 },
            QueryWord::Prefix{ id_range: (561_528u32, 561_531u32) },
        ];
        let phrase = QueryPhrase::new(&words).unwrap();

        assert_eq!(0, phrase.total_edit_distance());
        assert_eq!(true, phrase.has_prefix);
        assert_eq!(
            vec![
                0u8, 0u8,   1u8,     // 1
                0u8, 240u8, 88u8,    // 61_528
            ],
            phrase.full_word_key()
        );

        assert_eq!(
            Some((
                vec![ 8u8, 145u8, 120u8],     // 561_528
                vec![ 8u8, 145u8, 123u8],     // 561_531
            )),
            phrase.prefix_key_range()
        );

        assert_eq!(
            Some((
                561_528u32,
                561_531u32
            )),
            phrase.prefix_id_range()
        );

        let mut word_count = 0;
        let mut word_ids = vec![];

        let mut prefix_count = 0;
        let mut prefix_ids = vec![];

        let phrase_iter = phrase.into_iter();

        for word in phrase_iter {
            match word {
                &QueryWord::Full{ ref id, .. } => {
                    word_count += 1;
                    word_ids.push(*id);
                },
                &QueryWord::Prefix{ ref id_range, .. } => {
                    prefix_count += 1;
                    for i in (*id_range).0..(*id_range).1 {
                        prefix_ids.push(i);
                    }
                }
            }
        }

        // should be 2 full words, 2 ids
        assert_eq!(2, word_count);
        assert_eq!(vec![1, 61_528], word_ids);

        // should be 1 prefix, 3 ids
        assert_eq!(1, prefix_count);
        assert_eq!(vec![561_528, 561_529, 561_530], prefix_ids);
    }

    #[test]
    #[should_panic]
    fn non_terminal_prefix() {

        let words = vec![
            QueryWord::Full{ id: 1u32, edit_distance: 0 },
            QueryWord::Full{ id: 61_528u32, edit_distance: 0 },
            QueryWord::Prefix{ id_range: (561_528u32, 561_531u32) },
        ];
        let word_seq = [ words[0], words[2], words[1] ];
        QueryPhrase::new(&word_seq).unwrap();
    }

}
