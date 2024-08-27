import argparse
import logging
import pathlib
import struct
import gzip
import bz2
import sqlite3
import hashlib
import collections
import string

from typing import NamedTuple

logger = logging.getLogger(__name__)

"""
Convert a dictionary in StarDict format into a Sqlite database.

## Tables

+====================================+
+           definitions              +
+----------------+------+------------+
| definition_id  | word | definition |
+----------------+------+------------+

+=======================+
+      word_index       +
+------+----------------+
| word | definition_id  |
+------+----------------+


## StarDict

* https://github.com/huzheng001/stardict-3/blob/master/dict/doc/StarDictFileFormat

Every dictionary consists of these files:

(1). somedict.ifo
(2). somedict.idx or somedict.idx[.gz|bz2]
(3). somedict.dict or somedict.dict[.gz|dz|bz2]
(4). somedict.syn (optional) -- not handled

### Idx Format

The .idx file is just a word list.

The word list is a sorted list of word entries.

Each entry in the word list contains three fields, one after the other:
     word_str;  // a utf-8 string terminated by a null byte.
     word_data_offset;  // word data's offset in .dict file
     word_data_size;  // word data's total size in .dict file

word_str gives the string representing this word.  It's the string
that is "looked up" by the StarDict.

Two or more entries may have the same "word_str" with different
word_data_offset and word_data_size. This may be useful for some
dictionaries. But this feature is only well supported by
StarDict-2.4.8 and newer.

The length of "word_str" should be less than 256. In other words,
(strlen(word) < 256).

If the version is "3.0.0" and "idxoffsetbits=64", word_data_offset will
be 64-bits unsigned number in network byte order. Otherwise it will be
32-bits.
word_data_size should be 32-bits unsigned number in network byte order.


### Dict Format

The .dict file is a pure data sequence, as the offset and size of each
word is recorded in the corresponding .idx file.

If the "sametypesequence" option is not used in the .ifo file, then
the .dict file has fields in the following order:
==============
word_1_data_1_type; // a single char identifying the data type
word_1_data_1_data; // the data
word_1_data_2_type;
word_1_data_2_data;
...... // the number of data entries for each word is determined by
       // word_data_size in .idx file
word_2_data_1_type;
word_2_data_1_data;
......
==============
It's important to note that each field in each word indicates its
own length, as described below.  The number of possible fields per
word is also not fixed, and is determined by simply reading data until
you've read word_data_size bytes for that word.

'm'
Word's pure text meaning.
The data should be a utf-8 string ending with a null byte.

"""

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- Table for storing definitions, with each word potentially having multiple definitions
CREATE TABLE IF NOT EXISTS definitions (
    definition_id INTEGER PRIMARY KEY,                -- Primary key
    word TEXT NOT NULL,                               -- The word being defined (not unique)
    definition TEXT NOT NULL                          -- The definition text
);

-- FTS-enabled virtual contentless table for full-text searching of definitions
CREATE VIRTUAL TABLE IF NOT EXISTS definitions_search USING fts5(
    definition,                                      -- The definition text
    content='',                                      -- contentless
);

-- Table for mapping words (including synonyms or variants) to definitions
CREATE TABLE IF NOT EXISTS word_index (
    word TEXT NOT NULL,                               -- A word (synonym or variant)
    definition_id INTEGER NOT NULL,                   -- Foreign key referencing the definition
    frequency INTEGER NOT NULL,                       -- Word frequency
    PRIMARY KEY (word, definition_id),                -- Composite primary key to prevent duplicates
    FOREIGN KEY (definition_id) REFERENCES definitions(definition_id) ON DELETE CASCADE
);

-- Create an index on word for faster lookups
CREATE INDEX IF NOT EXISTS idx_word ON definitions(word);
"""

INSERT_META = """INSERT INTO meta (key, value) VALUES (?, ?)"""
INSERT_DEFINITION = (
    """INSERT INTO definitions (definition_id, word, definition) VALUES (?, ?, ?)"""
)
UPDATE_DEFINITION_WORD = """UPDATE definitions set word=? where definition_id=?"""
INSERT_WORD_INDEX = (
    """INSERT INTO word_index (word, definition_id, frequency) VALUES (?, ?, ?)"""
)
INSERT_DEFINITION_FTS = """INSERT INTO definitions_search (rowid, definition) SELECT definition_id, definition FROM definitions"""

BATCH_SIZE = 10000

PUNCTUATION = string.punctuation.replace("'", "")


class IndexEntry(NamedTuple):
    """a word and the data offset and size"""

    word: bytes
    data_offset: int
    data_size: int


class Index(NamedTuple):
    """a list of IndexEntries and a dict of words and the corresponding entries"""

    entries: list[IndexEntry]
    words: dict[bytes, list[int]]


def first(iterable, otherwise=None):
    "return the first truthy item in the iterable or otherwise"
    for item in iterable:
        if item:
            return item
    return otherwise


def key_val(line: str) -> tuple[str, str]:
    "For a line formatted key=value, return (key, value)"
    key, val = line.split("=")
    return key.strip(), val.strip()


def find_dict_files(root: str) -> dict[str, pathlib.Path]:
    """
    Return the dictionary files in the given diractory.

    Every dictionary consists of these files:
    (1). somedict.ifo
    (2). somedict.idx or somedict.idx[.gz|bz2]
    (3). somedict.dict or somedict.dict[.gz|dz|bz2]
    """

    def with_ext(p: pathlib.Path, e: str):
        p = p.with_suffix(e)
        return p if p.exists() else None

    root_path = pathlib.Path(root)
    ifo = next(root_path.glob("*.ifo"))

    res = {
        "ifo": ifo,
        "idx": first(with_ext(ifo, e) for e in [".idx", ".idx.gz", ".idx.bz2"]),
        "dict": first(
            with_ext(ifo, e) for e in [".dict", ".dict.dz", ".dict.gz", ".dict.bz2"]
        ),
    }

    return res


def open_any(path: pathlib.Path, flags="r"):
    "returns an open function appropriate for the filename"
    if path.suffix == ".gz" or path.suffix == ".dz":
        return gzip.open(path, flags)
    elif path.suffix.endswith(".bz2"):
        return bz2.open(path, flags)
    else:
        return open(path, flags)


def read_ifo(path: pathlib.Path) -> dict[str, str | int]:
    """
    Return a StarDict ifo file as a dictionary
    """
    data: dict[str, str | int] = {}
    with open(path, "r") as ifo_file:
        data["title"] = ifo_file.readline().strip()
        key, ver = key_val(ifo_file.readline())
        if key != "version":
            raise ValueError(f"Expected unexpected key {key}, expected 'version'")
        if ver not in {"2.4.2", "3.0.0"}:
            raise ValueError(f"Unknown version {ver}, expected 2.4.2 or 3.0.0")
        data[key] = ver
        for line in ifo_file:
            key, val = key_val(line)
            if key == "idxoffsetbits" and data["version"] == "3.3.0":
                continue
            data[key] = int(val) if key in {"wordcount", "idxfilesize"} else val
    return data


def read_idx(path: pathlib.Path, index_offset_sz=32) -> Index:
    """
    Read dictionary index from .idx file and return an Index.
    """
    if index_offset_sz not in {32, 64}:
        raise ValueError(f"unexpected index offset size: {index_offset_sz}")
    sz = 4 if index_offset_sz == 32 else 8
    entries: list[IndexEntry] = []
    words: dict[bytes, list[int]] = {}
    content = open_any(path, "rb").read()

    offset = 0
    while True:
        if offset >= len(content):
            break

        end = content.find(b"\0", offset)
        if end == -1:
            raise ValueError(f"corrupted file? no end found at {offset}")

        word = content[offset:end]
        offset = end + 1
        (data_offset,) = struct.unpack("!I", content[offset : offset + sz])
        offset += sz
        (data_size,) = struct.unpack("!I", content[offset : offset + 4])
        offset += 4

        entries.append(IndexEntry(word, data_offset, data_size))
        if word not in words:
            words[word] = []
        words[word].append(len(entries) - 1)

    return Index(entries, words)


class StarDict(object):

    def __init__(self, root: str):
        self.files = find_dict_files(root)
        logger.debug(f"loading {self.files}")
        self.meta = read_ifo(self.files["ifo"])
        logger.debug(f"meta {self.meta}")
        self.index = read_idx(self.files["idx"])
        self.data = open_any(self.files["dict"]).read()

    def get(self, entry):
        word, offset, sz = entry
        return word.decode("utf8"), self.data[offset : offset + sz].decode("utf8")

    def lookup(self, word):
        mo = self.index.words[word.encode("utf8")]
        entry = self.index.entries[mo[0]]
        return self.get(entry)

    def __iter__(self):
        for entry in self.index.entries:
            word, definition = self.get(entry)
            yield word, definition


def hash(value: str) -> str:
    sha = hashlib.sha256()
    sha.update(value.encode("utf8"))
    return sha.hexdigest()


def update_freq(table: dict[str, int], desc: str):
    words = desc.lower().split()
    words = [word.strip(PUNCTUATION) for word in words]
    for word in words:
        table[word] += 1


def main(parser, args):
    stardict = StarDict(args.path)
    db_path = f"{stardict.files["ifo"].stem}.db"
    logger.debug(f"creating dictionary {db_path}")
    conn = sqlite3.connect(db_path)
    for sql in SCHEMA.split(";"):
        conn.execute(sql)

    # insert meta
    cur = conn.cursor()
    cur.execute("BEGIN TRANSACTION")
    cur.execute("DELETE FROM meta")
    cur.executemany(INSERT_META, stardict.meta.items())
    cur.execute("COMMIT")

    # insert definitions
    freq = collections.defaultdict(int)
    dedup = {}
    word_index = {}
    resolved = {}
    batch = []
    definition_id = 0
    cur.execute("BEGIN TRANSACTION")
    for word, definition in stardict:
        h = hash(definition)
        if h in dedup:
            duplicate_id = dedup[h]
            word_index[word] = duplicate_id
            if definition.startswith(word):
                if duplicate_id in resolved:
                    logger.debug(
                        f"resolution collision between {word} and {resolved[duplicate_id]} for {definition[:40]}"
                    )
                resolved[duplicate_id] = word
        else:
            definition_id += 1
            word_index[word] = definition_id
            dedup[h] = definition_id
            update_freq(freq, definition)
            batch.append((definition_id, word, definition))
            if len(batch) == BATCH_SIZE:
                cur.executemany(INSERT_DEFINITION, batch)
                batch = []
    if batch:
        cur.executemany(INSERT_DEFINITION, batch)

    cur.executemany(
        UPDATE_DEFINITION_WORD,
        ((word, definition_id) for definition_id, word in resolved.items()),
    )
    cur.executemany(
        INSERT_WORD_INDEX, ((w, word_index[w], freq[w.lower()]) for w in word_index)
    )
    cur.execute(INSERT_DEFINITION_FTS)

    cur.execute("COMMIT")
    conn.close()


def parse_args() -> tuple[argparse.ArgumentParser, argparse.Namespace]:
    parser = argparse.ArgumentParser(usage=__doc__)
    parser.add_argument("-D", "--debug", action="store_true", help="Log debug messages")
    parser.add_argument("path", help="Directory path containing the StarDict files")
    args = parser.parse_args()

    level = logging.DEBUG if args.debug else logging.INFO
    format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=level, format=format)
    return parser, args


if __name__ == "__main__":
    parser, args = parse_args()
    main(parser, args)
