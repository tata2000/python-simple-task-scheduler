import configparser
import os

# Check for missing config.ini
if not os.path.exists('config.ini'):
    raise FileNotFoundError("The 'config.ini' file is missing!")

# Load configuration settings from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Extract specific settings with default values to prevent KeyErrors
FAILURE_PROBABILITY = float(config['TaskSettings'].get('failure_probability', 0.8))
MAX_SLEEP_DURATION = int(config['TaskSettings'].get('max_sleep_duration', 10))
STUCK_PROBABILITY = float(config['TaskSettings'].get('stuck_probability', 0.2))
RETRY_COUNT = int(config['TaskSettings'].get('retry_count', 3))
MAX_THREADS = int(config['SchedulerSettings'].get('max_threads', 5))
DATABASE_FILE = str(config['SchedulerSettings'].get('database_file_name', 'tasks.db'))
