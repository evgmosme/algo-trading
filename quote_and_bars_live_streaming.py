import os
import json
import pandas as pd
import psycopg2
import pytz
import time
import logging
import asyncio
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values
from psycopg2.errors import DuplicateObject
from alpaca_trade_api.rest import REST
from alpaca_trade_api.common import URL
from alpaca_trade_api.stream import Stream
from airflow import DAG
from airflow.operators.python import PythonOperator
from typing import Optional
from airflow.utils.log.logging_mixin import LoggingMixin
import nest_asyncio

# Apply nest_asyncio to allow nested use of asyncio event loops
nest_asyncio.apply()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='01.ALPACA_Quote_Bars_Stream',
    default_args=default_args,
    description='A DAG to fetch stock quotes and insert into PostgreSQL',
    schedule='59 03 * * 1-5',
    start_date=datetime(2024, 5, 25, 4, 0, 0),
    catchup=False,
    tags=['quotes', 'bars', 'alpaca']
)

class QuoteStreamer(LoggingMixin):
    """
    A class to stream and insert stock quotes and bars into a PostgreSQL database.

    Attributes:
    pg_conn (psycopg2.connection): PostgreSQL connection object.
    table_quotes (str): Name of the PostgreSQL table to insert quotes into.
    table_bars (str): Name of the PostgreSQL table to insert bars into.
    api_key (str): API key for the Alpaca service.
    secret_key (str): Secret key for the Alpaca service.
    inserted_quotes_count (int): Total number of inserted quotes.
    inserted_bars_count (int): Total number of inserted bars.
    error_count (int): Total number of errors encountered during insertion.
    inserted_quotes_last_minute (int): Number of quotes inserted in the last minute.
    inserted_bars_last_minute (int): Number of bars inserted in the last minute.
    quote_id_counter (int): Counter to assign unique IDs to quotes.
    batch_quotes (list): List to batch quotes before insertion.
    batch_bars (list): List to batch bars before insertion.
    info_logger (logging.Logger): Logger for informational messages.
    """

    def __init__(self, pg_conn, table_quotes, table_bars, api_key, secret_key):
        """
        Initializes the QuoteStreamer with database connection, table name, and API credentials.

        Parameters:
        pg_conn (psycopg2.connection): PostgreSQL connection object.
        table_quotes (str): Name of the PostgreSQL table to insert quotes into.
        table_bars (str): Name of the PostgreSQL table to insert bars into.
        api_key (str): API key for the Alpaca service.
        secret_key (str): Secret key for the Alpaca service.
        """
        super().__init__()
        self.pg_conn = pg_conn
        self.table_quotes = table_quotes
        self.table_bars = table_bars
        self.api_key = api_key
        self.secret_key = secret_key
        self.inserted_quotes_count = 0
        self.inserted_bars_count = 0
        self.error_count = 0
        self.inserted_quotes_last_minute = 0
        self.inserted_bars_last_minute = 0
        self.quote_id_counter = 0
        self.batch_quotes = []
        self.batch_bars = []
        self._configure_logging()

    def _configure_logging(self):
        """
        Configures logging for the QuoteStreamer.
        Sets up error logging to a file and informational logging.
        """
        logging.basicConfig(filename='quote_errors.log', level=logging.ERROR, 
                            format='%(asctime)s %(levelname)s %(message)s')
        self.info_logger = self.log

    async def quote_callback(self, quote):
        """
        Callback function to process incoming quotes.
        
        Parameters:
        quote (Quote): A quote object containing quote details.
        """
        self.quote_id_counter += 1  # Increment the quote_id counter
        record = (
            self.quote_id_counter,
            quote.symbol,
            quote.timestamp,
            quote.ask_price,
            quote.ask_size,
            quote.ask_exchange,
            quote.bid_price,
            quote.bid_size,
            quote.bid_exchange,
            quote.conditions,
            quote.tape
        )
        self.batch_quotes.append(record)

    async def bar_callback(self, bar):
        """
        Callback function to process incoming bars.
        
        Parameters:
        bar (Bar): A bar object containing bar details.
        """
        record = (
            bar.symbol,
            datetime.utcfromtimestamp(bar.timestamp / 1e9),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.trade_count,
            bar.vwap
        )
        self.batch_bars.append(record)

    async def batch_insert(self):
        """
        Periodically inserts batched quotes and bars into the PostgreSQL database.
        Runs indefinitely, inserting quotes and bars every second.
        """
        while True:
            await asyncio.sleep(1)  # Insert every 1 second
            if self.batch_quotes or self.batch_bars:
                try:
                    with self.pg_conn.cursor() as cursor:
                        if self.batch_quotes:
                            execute_values(cursor, f"""
                                INSERT INTO {self.table_quotes} (quote_id, symbol, timestamp, ask_price, ask_size, 
                                ask_exchange, bid_price, bid_size, bid_exchange, conditions, tape)
                                VALUES %s
                            """, self.batch_quotes)
                            self.inserted_quotes_count += len(self.batch_quotes)
                            self.inserted_quotes_last_minute += len(self.batch_quotes)
                            self.batch_quotes.clear()
                        if self.batch_bars:
                            execute_values(cursor, f"""
                                INSERT INTO {self.table_bars} (symbol, timestamp, open, high, low, 
                                close, volume, trade_count, vwap)
                                VALUES %s
                            """, self.batch_bars)
                            self.inserted_bars_count += len(self.batch_bars)
                            self.inserted_bars_last_minute += len(self.batch_bars)
                            self.batch_bars.clear()
                        self.pg_conn.commit()
                except Exception as e:
                    self.pg_conn.rollback()
                    self.error_count += 1
                    self.log.error(f"Error inserting records: {e}")

    async def log_counts(self):
        """
        Logs the number of quotes and bars inserted in the last minute, the total number of inserted quotes 
        and bars, and the total number of errors. Runs indefinitely, logging counts at the start of each minute.
        """
        while True:
            now = datetime.now()
            next_minute = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
            sleep_duration = (next_minute - now).total_seconds()
            await asyncio.sleep(sleep_duration)
            
            self.info_logger.info(f"Quotes inserted in the last minute: {self.inserted_quotes_last_minute}")
            self.info_logger.info(f"Bars inserted in the last minute: {self.inserted_bars_last_minute}")
            self.info_logger.info(f"Total quotes inserted: {self.inserted_quotes_count}")
            self.info_logger.info(f"Total bars inserted: {self.inserted_bars_count}")
            self.info_logger.info(f"Errors: {self.error_count}")
            
            self.inserted_quotes_last_minute = 0
            self.inserted_bars_last_minute = 0

    async def start_streaming(self, symbols):
        """
        Starts streaming quotes and bars for the given symbols and handles the insertion and logging.

        Parameters:
        symbols (list): List of stock symbols to stream quotes and bars for.
        """
        stream = Stream(
            self.api_key,
            self.secret_key,
            base_url=URL('https://paper-api.alpaca.markets'),
            data_feed='sip'
        )

        # Subscribe to quote and bar updates for each symbol in the list
        for symbol in symbols:
            stream.subscribe_quotes(self.quote_callback, symbol)
            stream.subscribe_bars(self.bar_callback, symbol)

        await asyncio.gather(stream._run_forever(), self.batch_insert(), self.log_counts())

def create_postgres_connection():
    """
    Create and return a new PostgreSQL connection.
    """
    conn_string = os.getenv('TIMESCALE_LIVE_CONN_STRING')
    return psycopg2.connect(conn_string)

def create_hypertable(pg_conn, table, table_type):
    """
    Create hypertable and apply retention policy.
    """
    with pg_conn.cursor() as cursor:
        # Create main table if it doesn't exist
        if table_type == 'quotes':
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    quote_id serial,
                    symbol text,
                    timestamp timestamp(6) with time zone DEFAULT (current_timestamp(6) AT TIME ZONE 'UTC'),
                    ask_price real,
                    ask_size int,
                    ask_exchange text,
                    bid_price real,
                    bid_size int,
                    bid_exchange text,
                    conditions text,
                    tape text,
                    PRIMARY KEY(symbol, timestamp, quote_id)
                );
            """)
        elif table_type == 'bars':
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    symbol text,
                    timestamp timestamp with time zone DEFAULT (current_timestamp AT TIME ZONE 'UTC'),
                    open real,
                    high real,
                    low real,
                    close real,
                    volume int,
                    trade_count int,
                    vwap real,
                    PRIMARY KEY(symbol, timestamp)
                );
            """)
        # Convert the table to a hypertable
        cursor.execute(f"""
            SELECT create_hypertable('{table}', 'timestamp', if_not_exists => TRUE);
        """)
        # Add retention policy to drop data older than 7 days
        try:
            cursor.execute(f"""
                SELECT add_retention_policy('{table}', INTERVAL '7 days');
            """)
        except DuplicateObject:
            logging.info(f"Retention policy already exists for hypertable '{table}', skipping.")
        pg_conn.commit()

def stream_data(end_time_str):
    """
    Streams stock quotes and bars until the specified end time.

    Parameters:
    end_time_str (str): The end time in "HH:MM" format (Eastern Time).

    This function:
    - Retrieves API keys and connection strings from environment variables.
    - Sets up a connection to a PostgreSQL database and creates hypertables.
    - Reads a list of stock symbols from a CSV file.
    - Initializes a QuoteStreamer instance for streaming stock quotes and bars.
    - Configures logging.
    - Calculates the streaming duration based on the current time and the specified end time.
    - Starts an asyncio event loop to stream quotes and bars for the calculated duration.
    """
    
    api_key = os.getenv('ALPACA_API_KEY_JOHNYSSAN')
    secret_key = os.getenv('ALPACA_SECRET_KEY_JOHNYSSAN')
    conn_string = os.getenv('TIMESCALE_LIVE_CONN_STRING')
    TABLE_QUOTES = 'alpaca_quotes'
    TABLE_BARS = 'alpaca_bars_1min'

    pg_conn = psycopg2.connect(conn_string)

    create_hypertable(pg_conn, TABLE_QUOTES, 'quotes')
    create_hypertable(pg_conn, TABLE_BARS, 'bars')

    symbols_russel = pd.read_csv('/home/tradingbot/proxima/airflow_docer/data/russel3000_symbols.csv', header=None)
    symbols_russel = list(symbols_russel[0])

    quote_streamer = QuoteStreamer(pg_conn, TABLE_QUOTES, TABLE_BARS, api_key, secret_key)

    logger = logging.getLogger('QuoteStreamer')
    logging.basicConfig(level=logging.INFO)
    
    # Parse end_time_str to calculate the duration
    now = datetime.now(pytz.timezone('US/Eastern'))
    end_time = datetime.strptime(end_time_str, "%H:%M").replace(year=now.year, month=now.month, 
                                day=now.day, tzinfo=now.tzinfo)
    if now > end_time:
        end_time += timedelta(days=1)  # If the current time is past the end_time, set the end time to tomorrow

    duration = (end_time - now).total_seconds()

    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)

    end_time_finnish = end_time.astimezone(pytz.timezone('Europe/Helsinki'))

    logger.info(f"Streaming will run for {int(hours)} hours, {int(minutes)} minutes, and {int(seconds)} seconds.")
    logger.info(f"Streaming will end at {end_time.strftime('%Y-%m-%d, %H:%M:%S %Z')} ({end_time_finnish.strftime('%Y-%m-%d, %H:%M:%S %Z')})")

    # Start the event loop
    loop = asyncio.get_event_loop()
    
    async def run_streaming():
        try:
            await quote_streamer.start_streaming(symbols_russel)
        except asyncio.CancelledError:
            logger.info("Streaming stopped after the calculated duration")
            raise  # Re-raise the exception to propagate the cancellation
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise  # Re-raise the exception to ensure proper handling
        finally:
            pg_conn.close()
            logger.info("PostgreSQL connection closed")
            logger.info("Exiting script...")

    # If the loop is already running, use create_task, else run the loop
    if loop.is_running():
        task = loop.create_task(run_streaming())
        loop.run_until_complete(asyncio.sleep(duration))  # Run for the calculated duration
        task.cancel()  # Cancel the task after the duration
        try:
            loop.run_until_complete(task)  # Wait for the task cleanup
        except asyncio.CancelledError:
            pass  # Ignore the re-raised cancellation error
    else:
        task = loop.create_task(run_streaming())
        loop.run_until_complete(asyncio.sleep(duration))
        task.cancel()
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass  # Ignore the re-raised cancellation error

stream_data_task = PythonOperator(
    task_id='fetch_quotes',
    python_callable=stream_data,
    op_kwargs={'end_time_str': '20:01'},  # Pass the end_time as a string in "HH:MM" format
    dag=dag,
)

stream_data_task