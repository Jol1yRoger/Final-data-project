
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    orientation TEXT,
    white_player TEXT NOT NULL,
    white_rating INTEGER,
    white_title TEXT,
    white_seconds INTEGER,
    black_player TEXT NOT NULL,
    black_rating INTEGER,
    black_title TEXT,
    black_seconds INTEGER,
    fen TEXT,
    last_move TEXT,
    white_clock INTEGER,
    black_clock INTEGER,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(game_id, ingested_at)
);


CREATE INDEX IF NOT EXISTS idx_game_id ON events(game_id);
CREATE INDEX IF NOT EXISTS idx_processed_at ON events(processed_at);

CREATE TABLE IF NOT EXISTS daily_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    summary_date DATE NOT NULL UNIQUE,
    total_games INTEGER,
    avg_white_rating REAL,
    avg_black_rating REAL,
    min_rating INTEGER,
    max_rating INTEGER,
    avg_game_duration_seconds REAL,
    total_titled_players INTEGER,
    rating_distribution_1000_1500 INTEGER,
    rating_distribution_1500_2000 INTEGER,
    rating_distribution_2000_2500 INTEGER,
    rating_distribution_2500_plus INTEGER,
    most_common_opening TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
