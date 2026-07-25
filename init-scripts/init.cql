CREATE KEYSPACE IF NOT EXISTS twitter
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE twitter;

CREATE TABLE IF NOT EXISTS tweets (
    id        UUID PRIMARY KEY,
    text      TEXT,
    username  TEXT,
    sentiment TEXT,
    hashtags  LIST<TEXT>,
    words     LIST<TEXT>,
    score     FLOAT,
    likes     INT,
    retweets  INT,
    ts        TIMESTAMP
);

CREATE TABLE IF NOT EXISTS hashtag_stats (
    hashtag TEXT PRIMARY KEY,
    count   COUNTER
);

CREATE TABLE IF NOT EXISTS sentiment_stats (
    sentiment TEXT PRIMARY KEY,
    count     COUNTER
);