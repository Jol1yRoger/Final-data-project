

import logging
import re
import sys
import os
from typing import Dict, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(dag_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from config.config import (
    DEFAULT_RATING,
    MIN_VALID_RATING,
    MAX_VALID_RATING,
    DEFAULT_CLOCK_TIME
)


def validate_fen(fen: str) -> bool:

    if not fen or not isinstance(fen, str):
        return False
    
    parts = fen.split()
    if len(parts) < 1:
        return False
    
    rows = parts[0].split('/')
    if len(rows) != 8:
        return False
    
    return True


def clean_rating(rating: Any, default: int = DEFAULT_RATING) -> int:
    try:
        rating_int = int(rating)
        if MIN_VALID_RATING <= rating_int <= MAX_VALID_RATING:
            return rating_int
        else:
            logger.warning(f"Invalid rating {rating_int}, using default {default}")
            return default
    except (ValueError, TypeError):
        return default


def clean_clock_time(seconds: Any, default: int = DEFAULT_CLOCK_TIME) -> int:
    try:
        seconds_int = int(seconds)
        if seconds_int >= 0:
            return seconds_int
        else:
            logger.warning(f"Negative clock time {seconds_int}, using default {default}")
            return default
    except (ValueError, TypeError):
        return default


def clean_player_name(name: str) -> str:
    if not name or not isinstance(name, str):
        return "unknown"
    return name.lower().strip()


def clean_game_event(raw_event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        event_type = raw_event.get('t', 'unknown')
        data = raw_event.get('d', {})
        
        game_id = data.get('id')
        if not game_id:
            logger.warning("Event missing game_id, skipping")
            return None
        
        players = data.get('players', [])
        if len(players) != 2:
            logger.warning(f"Game {game_id} has {len(players)} players, expected 2, skipping")
            return None
        
        white_player = None
        black_player = None
        white_rating = None
        black_rating = None
        white_title = None
        black_title = None
        white_seconds = None
        black_seconds = None
        
        for player in players:
            color = player.get('color', '').lower()
            user = player.get('user', {})
            name = user.get('name', user.get('id', 'unknown'))
            rating = player.get('rating')
            title = user.get('title')
            seconds = player.get('seconds')
            
            if color == 'white':
                white_player = clean_player_name(name)
                white_rating = clean_rating(rating)
                white_title = title if title else None
                white_seconds = clean_clock_time(seconds)
            elif color == 'black':
                black_player = clean_player_name(name)
                black_rating = clean_rating(rating)
                black_title = title if title else None
                black_seconds = clean_clock_time(seconds)
        
        if not white_player or not black_player:
            logger.warning(f"Game {game_id} missing player information, skipping")
            return None
        
        if white_rating is None or black_rating is None:
            logger.warning(f"Game {game_id} missing ratings, using defaults")
            white_rating = white_rating or DEFAULT_RATING
            black_rating = black_rating or DEFAULT_RATING
        
        fen = data.get('fen', '')
        if fen and not validate_fen(fen):
            logger.warning(f"Game {game_id} has invalid FEN, keeping anyway")
        
        last_move = data.get('lm')
        orientation = data.get('orientation', 'white')
        white_clock = clean_clock_time(data.get('wc'))
        black_clock = clean_clock_time(data.get('bc'))
        
        cleaned_event = {
            'game_id': str(game_id),
            'event_type': event_type,
            'orientation': orientation,
            'white_player': white_player,
            'white_rating': white_rating,
            'white_title': white_title,
            'white_seconds': white_seconds,
            'black_player': black_player,
            'black_rating': black_rating,
            'black_title': black_title,
            'black_seconds': black_seconds,
            'fen': fen if fen else None,
            'last_move': last_move if last_move else None,
            'white_clock': white_clock,
            'black_clock': black_clock,
            'ingested_at': datetime.now().isoformat()
        }
        
        return cleaned_event
        
    except Exception as e:
        logger.error(f"Error cleaning event: {e}")
        return None
