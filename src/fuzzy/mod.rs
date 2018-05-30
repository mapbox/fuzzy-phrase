use std::collections::HashSet;
mod map;
pub use self::map::FuzzyMap;
pub use self::map::FuzzyMapBuilder;

#[cfg(test)] extern crate reqwest;

#[inline(always)]
fn get_variants<'a>(word: &str, edit_distance: u8) -> HashSet<String> {
    let mut variants: HashSet<String> = HashSet::new();
    get_variants_recursive(word, 1, edit_distance, &mut variants);
    variants
}

fn get_variants_recursive<'a>(word: &str, edit_distance: u8, max_distance: u8, delete_variants: &'a mut HashSet<String>) -> () {
    let mut iter = word.char_indices().peekable();

    while let Some((pos, _char)) = iter.next() {
        let mut deleted_item = String::with_capacity(word.len());
        deleted_item.push_str(&word[..pos]);

        if let Some((next_pos, _)) = iter.peek() {
            deleted_item.push_str(&word[*next_pos..]);
        }

        if edit_distance < max_distance {
            get_variants_recursive(&deleted_item, edit_distance + 1, max_distance, delete_variants);
        }
        delete_variants.insert(deleted_item);
    }
}
