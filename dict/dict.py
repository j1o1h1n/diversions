import time
import sqlite3

from dataclasses import dataclass
from functools import partial

from textual import on
from textual.app import App, ComposeResult
from textual.command import Hit, Hits, Provider
from textual.widgets import Header, TextArea, Input, ContentSwitcher
from textual.widgets import Static
from textual.message import Message

import textual.events

WORD_PUNCTUATION = "'-"
PHRASE_PUNCTUATION = "'- "
DOUBLE_CLICK_SECONDS = 0.3


@dataclass
class SelectWord(Message, bubble=False):
    """Message to tell the app to select a word."""

    word: str


class LookupDictionary(Provider):
    """A dictionary provider to select words."""

    async def search(self, query: str) -> Hits:
        """Called for each key."""
        global DICTIONARY
        dictionary = DICTIONARY
        for word, score in dictionary.match(query):
            yield Hit(
                score,
                word,
                partial(self.app.post_message, SelectWord(word)),
            )


def score(word, freq, prefix, total):
    if word.lower() == prefix.lower():
        return 1.0
    else:
        return freq / total


class Dictionary:
    DICTIONARY = "app/dict/web1913.db"
    LOOKUP = "SELECT d.word, d.definition FROM definitions d, word_index w WHERE d.definition_id = w.definition_id AND (w.word = ? or w.word = ?)"
    MATCH = "SELECT word, frequency FROM word_index WHERE word like ? or word like ? ORDER BY frequency DESC LIMIT 10"

    def __init__(self):
        self.conn = sqlite3.connect(Dictionary.DICTIONARY)

    def lookup(self, word):
        cur = self.conn.cursor()
        try:
            rs = cur.execute(Dictionary.LOOKUP, (word.capitalize(), word.lower()))
            return rs.fetchall()
        finally:
            cur.close()

    def match(self, prefix):
        cur = self.conn.cursor()
        try:
            rs = cur.execute(
                Dictionary.MATCH, (f"{prefix.capitalize()}%", f"{prefix.lower()}%")
            )
            res = list(rs.fetchall())
            total = sum(f for _, f in res)
            words = [(w, score(w, f, prefix, total)) for w, f in res]
            return words
        finally:
            cur.close()


DICTIONARY = Dictionary()


def get_selection(selection, text):
    (row0, col0), (row1, col1) = selection.start, selection.end
    lines = text.split("\n")
    if row0 == row1:
        return lines[row0][col0:col1]
    res = []
    res.append(lines[row0][col0:])
    for row in range(row0 + 1, row1):
        res.append(lines[row])
    res.append(lines[row1][:col1])
    return "\n".join(res)


def strip_phrase(word):
    """Removes the non-alphabetic characters."""
    return "".join(
        char for char in word if char.isalpha() or char in PHRASE_PUNCTUATION
    )


def strip_word(word):
    """Removes the non-alphabetic characters."""
    word = "".join(char for char in word if char.isalpha() or char in WORD_PUNCTUATION)
    return word.capitalize()


def find_word_or_phrase(line, idx):
    """returns the word or phrase at idx"""
    prev_curly = line[:idx].rfind("{")
    next_curly = line.find("}", prev_curly)
    if prev_curly > -1 and prev_curly <= idx <= next_curly:
        return strip_phrase(line[prev_curly:next_curly])

    prev_space = line[:idx].rfind(" ")
    if prev_space == -1:
        prev_space = 0
    next_space = line.find(" ", prev_space + 1)
    if next_space == -1:
        next_space = len(line)
    return strip_word(line[prev_space:next_space])


class Definition(Static):
    """Display a word definition."""

    last_click_ts = 0.0

    class Selected(Message):
        def __init__(self, word: str) -> None:
            self.word = word
            super().__init__()

        def __repr__(self):
            return f"Selected({self.word})"

    class ToggleEditable(Message):
        pass

    def is_double_click(self, event):
        ts = time.time()
        res = (
            (ts - self.last_click_ts <= DOUBLE_CLICK_SECONDS)
            and event.delta_x == 0
            and event.delta_y == 0
        )
        self.last_click_ts = ts
        return res

    async def on_click(self, event: textual.events.Event) -> None:
        if self.is_double_click(event):
            self.post_message(self.ToggleEditable())
        else:
            text = self.renderable.plain  # type: ignore
            x, y = event.x - 1, event.y  # type: ignore
            line = text.split("\n")[y]
            res = find_word_or_phrase(line, x)
            self.post_message(self.Selected(res))


class DictionaryApp(App):

    COMMANDS = {LookupDictionary}
    TITLE = "Press ctrl + p and type a word"

    # CSS_PATH = "dict.tcss"

    BINDINGS = [("escape", "toggle_definition")]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        global DICTIONARY
        self.dictionary = DICTIONARY
        self.dark = False

    def compose(self) -> ComposeResult:
        yield Header()
        yield Input(placeholder="Prolix", classes="box")

        with ContentSwitcher(initial="alpha"):
            yield Definition(id="alpha")
            yield TextArea(id="beta", read_only=True)

    async def on_input_submitted(self, message: Input.Submitted):
        word = message.value.capitalize()
        definitions = self.dictionary.lookup(word)
        if definitions:
            definition = definitions[0][1]
            self.query_one(ContentSwitcher).current = "alpha"
            self.query_one(Definition).update(definition)
            self.query_one(TextArea).text = definition
        else:
            # TODO flash border red
            pass

    @on(SelectWord)
    def select_word(self, event: SelectWord) -> None:
        lookup = self.query_one(Input)
        lookup.value = event.word
        lookup.action_end()

    def on_definition_selected(self, message: Definition.Selected) -> None:
        lookup = self.query_one(Input)
        lookup.value = message.word
        lookup.action_end()

    def on_definition_toggle_editable(self, message: Definition.ToggleEditable) -> None:
        self.query_one(ContentSwitcher).current = "beta"

    def on_text_area_selection_changed(
        self, message: TextArea.SelectionChanged
    ) -> None:
        selected = get_selection(message.selection, message.text_area.text)
        self.copy_to_clipboard(selected)

    def action_toggle_definition(self) -> None:
        self.query_one(ContentSwitcher).current = "alpha"

    # TODO on enter - set focus to input and toggle definition to alpha

# NOTE: dictionary is not provided, but the schema is as follows

# CREATE TABLE IF NOT EXISTS definitions (
#     definition_id INTEGER PRIMARY KEY,                -- Primary key
#     word TEXT NOT NULL,                               -- The word being defined (not unique)
#     definition TEXT NOT NULL                          -- The definition text
# );

# -- Table for mapping words (including synonyms or variants) to definitions
# CREATE TABLE IF NOT EXISTS word_index (
#     word TEXT NOT NULL,                               -- A word (synonym or variant)
#     definition_id INTEGER NOT NULL,                   -- Foreign key referencing the definition
#     frequency INTEGER NOT NULL,                       -- Word frequency
#     PRIMARY KEY (word, definition_id),                -- Composite primary key to prevent duplicates
#     FOREIGN KEY (definition_id) REFERENCES definitions(definition_id) ON DELETE CASCADE
# );

# -- Create an index on word for faster lookups
# CREATE INDEX IF NOT EXISTS idx_word ON definitions(word);


if __name__ == "__main__":
    app = DictionaryApp()
    app.run()
