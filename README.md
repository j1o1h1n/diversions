# Diversions

Recreations and diversions

## Dict

Inspired by reading some entries in Websters 1913 dictionary I wrote a little textualize gui for it.

This required converting the StarDict Websters dictionary into an sqlite database.

* See [You’re probably using the wrong dictionary](http://jsomers.net/blog/dictionary)

Build a toy dictionary database

```
$ cd dict
$ sqlite3 web1913.db < schema.sql
$ sqlite3 web1913.db ".mode csv" ".import definitions_dump.csv definitions"
$ sqlite3 web1913.db ".mode csv" ".import word_index_dump.csv word_index"
```

Run the dictionary

```
$ textual run dict/dict.py
```