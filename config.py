import configparser
import sqlite3
import logging

# Load configurations
config = configparser.ConfigParser()
config.read('config.ini')

MAX_RETRIES = int(config['DEFAULT']['max_retries'])
DB_FILENAME = config['DEFAULT']['db_filename']
MAX_THREADS = int(config['DEFAULT']['max_threads'])
RANDOM_WAIT_RANGE = int(config['DEFAULT']['random_wait_range'])
FAILURE_PROBABILITY = float(config['DEFAULT']['failure_probability'])
NUMBER_OF_TASKS_TO_RUN = int(config['DEFAULT']['number_of_tasks_to_run'])
LOG_FILE_NAME = config['DEFAULT']['log_file_name']

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.StreamHandler(), logging.FileHandler("scheduler.log")])


# Database setup
def setup_database():
    conn = sqlite3.connect(DB_FILENAME)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS tasks (
        name TEXT PRIMARY KEY,
        start_time TEXT,
        end_time TEXT,
        status TEXT,
        retry_count INTEGER
    )''')
    conn.commit()
    logging.info("Database setup completed")

setup_database()
