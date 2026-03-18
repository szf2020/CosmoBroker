-- CosmoBroker SQLite Persistence Schema

CREATE TABLE IF NOT EXISTS mq_messages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    subject     TEXT NOT NULL,
    payload     BLOB NOT NULL,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS mq_consumers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    subject     TEXT NOT NULL,
    last_msg_id INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS mq_users (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    username    TEXT NOT NULL UNIQUE,
    password    TEXT NOT NULL,
    token       TEXT
);

CREATE INDEX IF NOT EXISTS idx_mq_messages_subject ON mq_messages(subject);
