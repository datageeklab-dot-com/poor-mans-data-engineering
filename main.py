import os,traceback
import yaml
import logging
import argparse
from logging.handlers import RotatingFileHandler
from src.api import fetch_data
from src.database import create_database, save_data_to_database
from src.etl import extract_data
from datetime import datetime, timedelta

# Create a logs directory if it doesn't exist
logs_dir = 'logs'
os.makedirs(logs_dir, exist_ok=True)

# Generate a timestamp for the log file name
current_time = datetime.now()
log_timestamp = current_time.strftime('%Y%m%d%H%M%S')

# Set up logging with timestamped log files
log_filename = os.path.join(logs_dir, f'pipeline_{log_timestamp}.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
handler = RotatingFileHandler(log_filename, maxBytes=1024*1024, backupCount=1)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Load configuration from config.yaml
def load_config(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

# Load the last run timestamp from timestamp.yaml (including minutes)
def load_last_run_timestamp(timestamp_file):
    with open(timestamp_file, 'r') as file:
        timestamp_config = yaml.safe_load(file)
    return timestamp_config['last_run_timestamp']

# Save the current timestamp to timestamp.yaml (including minutes)
def save_current_timestamp(timestamp_file, timestamp):
    with open(timestamp_file, 'w') as file:
        yaml.dump({'last_run_timestamp': timestamp}, file)

# Parse command line arguments
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['backfill', 'increment'], help='Choose operation mode (backfill or increment)')
    return parser.parse_args()

if __name__ == "__main__":
    try:
        # Parse command line arguments
        args = parse_args()

        # Load configuration from config.yaml
        config = load_config('config/config.yaml')

        # Access API and database configuration
        api_url = config['api']['url']
        api_key = config['api']['api_key']
        db_name = config['database']['name']
        table_name = 'news_data'  # Change the table name as needed

        # Retrieve the last run timestamp (including minutes)
        timestamp_file = config['api']['timestamp_file']
        last_run_timestamp = load_last_run_timestamp(timestamp_file)

        if args.mode == 'backfill':
            # Use the backfill_timestamp from config.yaml
            time_from = config['api']['backfill_timestamp']
            logger.info(f"Setting mode of operation to Backfill")
            logger.info(f"Replacing data from the API since {time_from}...")
        elif args.mode == 'increment':
            # Use the last_run_timestamp from the previous run
            time_from = last_run_timestamp
            logger.info(f"Setting mode of operation to Increment")
            logger.info(f"Appending data from the API since {time_from}...")

        # Step 1: Fetch data from the API with 'time_from' parameter
        
        api_response = fetch_data(api_url, api_key, time_from=time_from)

        # Step 2: Extract data from the API response
        logger.info("Extracting data from the API response...")
        extracted_data = extract_data(api_response)
        extract_data_size=extracted_data.shape[0]

        logger.info(f"Total Records extracted : {extract_data_size}")

        if extract_data_size>0:
            # Step 3: Create a database and save data
            logger.info("Saving data...")
            engine = create_database(db_name)
            save_data_to_database(engine, extracted_data, table_name,mode=args.mode)

            # Calculate the new timestamp by adding 1 minute
            new_timestamp = (datetime.strptime(last_run_timestamp, '%Y%m%dT%H%M') + timedelta(minutes=1)).strftime('%Y%m%dT%H%M')

            # Save the current timestamp for the next run (including minutes)
            logger.info(f"Saving last timestamp: {new_timestamp} for next incremental run ")
            save_current_timestamp(timestamp_file, new_timestamp)

            logger.info("Data extraction and storage completed.")
        else:
            logger.info("No New data available")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

        print(traceback.format_exc())
