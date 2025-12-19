
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
import logging
from collections import Counter

import os
import sys

dag_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(dag_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from utils.db_utils import get_daily_events, insert_summary
from config.config import DB_PATH, RATING_BRACKETS

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


def compute_daily_analytics(**context):
    execution_date = context['execution_date']
    target_date = execution_date.date()
    
    logger.info(f"Computing analytics for date: {target_date}")
    
    events = get_daily_events(DB_PATH, target_date)
    
    if not events:
        logger.warning(f"No events found for {target_date}, skipping analytics")
        return
    
    logger.info(f"Processing {len(events)} events for analytics")
    
    total_games = len(events)
    white_ratings = []
    black_ratings = []
    all_ratings = []
    titled_players = 0
    game_durations = []
    opening_moves = []
    
    rating_dist = {
        '1000_1500': 0,
        '1500_2000': 0,
        '2000_2500': 0,
        '2500_plus': 0
    }
    
    for event in events:
        white_rating = event.get('white_rating')
        black_rating = event.get('black_rating')
        
        if white_rating:
            white_ratings.append(white_rating)
            all_ratings.append(white_rating)
        
        if black_rating:
            black_ratings.append(black_rating)
            all_ratings.append(black_rating)
        
        if event.get('white_title'):
            titled_players += 1
        if event.get('black_title'):
            titled_players += 1
        
        white_clock = event.get('white_clock', 0)
        black_clock = event.get('black_clock', 0)
        if white_clock and black_clock:
            avg_remaining = (white_clock + black_clock) / 2
            game_durations.append(avg_remaining)
        
        last_move = event.get('last_move')
        if last_move:
            opening_moves.append(last_move[:4])
    
    for rating in all_ratings:
        if 1000 <= rating < 1500:
            rating_dist['1000_1500'] += 1
        elif 1500 <= rating < 2000:
            rating_dist['1500_2000'] += 1
        elif 2000 <= rating < 2500:
            rating_dist['2000_2500'] += 1
        elif rating >= 2500:
            rating_dist['2500_plus'] += 1
    
    avg_white_rating = sum(white_ratings) / len(white_ratings) if white_ratings else 0
    avg_black_rating = sum(black_ratings) / len(black_ratings) if black_ratings else 0
    min_rating = min(all_ratings) if all_ratings else 0
    max_rating = max(all_ratings) if all_ratings else 0
    avg_duration = sum(game_durations) / len(game_durations) if game_durations else 0
    
    if opening_moves:
        opening_counter = Counter(opening_moves)
        most_common_opening = opening_counter.most_common(1)[0][0]
    else:
        most_common_opening = None
    
    summary = {
        'summary_date': target_date.isoformat(),
        'total_games': total_games,
        'avg_white_rating': round(avg_white_rating, 2),
        'avg_black_rating': round(avg_black_rating, 2),
        'min_rating': min_rating,
        'max_rating': max_rating,
        'avg_game_duration_seconds': round(avg_duration, 2),
        'total_titled_players': titled_players,
        'rating_distribution_1000_1500': rating_dist['1000_1500'],
        'rating_distribution_1500_2000': rating_dist['1500_2000'],
        'rating_distribution_2000_2500': rating_dist['2000_2500'],
        'rating_distribution_2500_plus': rating_dist['2500_plus'],
        'most_common_opening': most_common_opening
    }
    
    logger.info(f"Analytics computed: {summary}")
    
    success = insert_summary(DB_PATH, summary)
    
    if success:
        logger.info(f"Successfully stored daily summary for {target_date}")
    else:
        logger.error(f"Failed to store daily summary for {target_date}")
        raise Exception("Failed to insert daily summary")


with DAG(
    'dag3_daily_analytics',
    default_args=default_args,
    description='Daily analytics job to compute summary statistics',
    schedule_interval='@daily',
    catchup=False,
    tags=['lichess', 'analytics', 'summary'],
) as dag:
    
    analytics_task = PythonOperator(
        task_id='compute_daily_analytics',
        python_callable=compute_daily_analytics,
        execution_timeout=timedelta(minutes=20),
    )
    
    analytics_task
