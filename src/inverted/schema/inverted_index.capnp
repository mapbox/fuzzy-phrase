@0xab2215cd34a20ec6;

struct InvertedIndex {
    entries @0 :List(Entry);

    struct Entry {
        count @0 :UInt32;
        compressedIds @1 :Data;
    }
}