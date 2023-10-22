import configparser

# Load configuration settings from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Extract specific settings
FAILURE_PROBABILITY = float(config['TaskSettings']['failure_probability'])
MAX_SLEEP_DURATION = int(config['TaskSettings']['max_sleep_duration'])
RETRY_COUNT = int(config['TaskSettings']['retry_count'])


