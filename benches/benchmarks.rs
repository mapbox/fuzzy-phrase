#[macro_use]
extern crate criterion;
extern crate fuzzy_phrase;
extern crate fst;
extern crate reqwest;
extern crate itertools;
extern crate rand;

use criterion::Criterion;

mod prefix;
mod phrase;

criterion_group!{
    name = benches;
    config = Criterion::default();
    targets = prefix::benchmark, phrase::benchmark
}
criterion_main!(benches);
