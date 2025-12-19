
import sqlite3
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, date

logger = logging.getLogger(__name__)


def get_db_connection(db_path: str) -> Optional[sqlite3.Connection]:

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Enable column access by name
        logger.info(f"Database connection established: {db_path}")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to database: {e}")
        return None


def init_database(db_path: str) -> bool:

    try:
        conn = get_db_connection(db_path)
        if not conn:
            return False
            
        cursor = conn.cursor()
        
        cursor.execute("""
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
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_game_id ON events(game_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_processed_at ON events(processed_at)
        """)
        
        cursor.execute("""
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
            )
        """)
        
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully")
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Failed to initialize database: {e}")
        return False


def insert_events(db_path: str, events: List[Dict[str, Any]]) -> int:

    if not events:
        return 0
        
    try:
        conn = get_db_connection(db_path)
        if not conn:
            return 0
            
        cursor = conn.cursor()
        inserted = 0
        
        for event in events:
            try:
                cursor.execute("""
                    INSERT OR IGNORE INTO events (
                        game_id, event_type, orientation,
                        white_player, white_rating, white_title, white_seconds,
                        black_player, black_rating, black_title, black_seconds,
                        fen, last_move, white_clock, black_clock,
                        ingested_at, processed_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    event['game_id'],
                    event['event_type'],
                    event.get('orientation'),
                    event['white_player'],
                    event.get('white_rating'),
                    event.get('white_title'),
                    event.get('white_seconds'),
                    event['black_player'],
                    event.get('black_rating'),
                    event.get('black_title'),
                    event.get('black_seconds'),
                    event.get('fen'),
                    event.get('last_move'),
                    event.get('white_clock'),
                    event.get('black_clock'),
                    event.get('ingested_at', datetime.now().isoformat()),
                    datetime.now().isoformat()
                ))
                if cursor.rowcount > 0:
                    inserted += 1
            except sqlite3.IntegrityError:
                continue
                
        conn.commit()
        conn.close()
        logger.info(f"Inserted {inserted} events into database")
        return inserted
        
    except sqlite3.Error as e:
        logger.error(f"Failed to insert events: {e}")
        return 0


def get_daily_events(db_path: str, target_date: date) -> List[Dict[str, Any]]:

    try:
        conn = get_db_connection(db_path)
        if not conn:
            return []
            
        cursor = conn.cursor()
        cursor.execute("""
            SELECT * FROM events
            WHERE DATE(processed_at) = ?
        """, (target_date.isoformat(),))
        
        rows = cursor.fetchall()
        events = [dict(row) for row in rows]
        
        conn.close()
        logger.info(f"Retrieved {len(events)} events for {target_date}")
        return events
        
    except sqlite3.Error as e:
        logger.error(f"Failed to get daily events: {e}")
        return []


def insert_summary(db_path: str, summary: Dict[str, Any]) -> bool:

    try:
        conn = get_db_connection(db_path)
        if not conn:
            return False
            
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR REPLACE INTO daily_summary (
                summary_date, total_games,
                avg_white_rating, avg_black_rating,
                min_rating, max_rating,
                avg_game_duration_seconds,
                total_titled_players,
                rating_distribution_1000_1500,
                rating_distribution_1500_2000,
                rating_distribution_2000_2500,
                rating_distribution_2500_plus,
                most_common_opening,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            summary['summary_date'],
            summary['total_games'],
            summary['avg_white_rating'],
            summary['avg_black_rating'],
            summary['min_rating'],
            summary['max_rating'],
            summary['avg_game_duration_seconds'],
            summary['total_titled_players'],
            summary['rating_distribution_1000_1500'],
            summary['rating_distribution_1500_2000'],
            summary['rating_distribution_2000_2500'],
            summary['rating_distribution_2500_plus'],
            summary['most_common_opening'],
            datetime.now().isoformat()
        ))
        
        conn.commit()
        conn.close()
        logger.info(f"Inserted summary for {summary['summary_date']}")
        return True
        
    except sqlite3.Error as e:
        logger.error(f"Failed to insert summary: {e}")
        return False
