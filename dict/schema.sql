
-- Table for storing definitions, with each word potentially having multiple definitions
CREATE TABLE IF NOT EXISTS definitions (
    definition_id INTEGER PRIMARY KEY,                -- Primary key
    word TEXT NOT NULL,                               -- The word being defined (not unique)
    definition TEXT NOT NULL                          -- The definition text
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
