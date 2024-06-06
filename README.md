This repository contains an Apache Airflow DAG designed to stream live stock quotes and 1-minute bars for 3000 Russell Index stocks using the Alpaca API. The data is inserted into TimescaleDB tables for further analysis and processing. The DAG is scheduled to run every weekday when the US stock premarket opens and continues until the aftermarket closes.

## Features
- **Data Coverage**: Streams live quotes and 1-minute bars for 3000 Russell index stocks.
- **High Volume Handling**: Capable of processing approximately 80 million quotes and 700.000 1-minute bars daily.
- **Precision Timing**: Aligns operations with US stock market timings, ensuring data is captured throughout the trading day.
- **Data Integrity and Storage**: Utilizes PostgreSQL with TimescaleDB extension for efficient large-scale time-series data handling.
- **Robust Error Handling and Logging**: Detailed logging for error tracking and operational integrity ensures reliable data insertion and system stability.

## Components

### QuoteStreamer Class
The `QuoteStreamer` class is responsible for streaming and inserting stock quotes and bars into a PostgreSQL database.

- **Attributes**:
  - `pg_conn`: PostgreSQL connection object.
  - `table_quotes`: Name of the PostgreSQL table for quotes.
  - `table_bars`: Name of the PostgreSQL table for bars.
  - `api_key`: API key for the Alpaca service.
  - `secret_key`: Secret key for the Alpaca service.
  - `inserted_quotes_count`: Total number of inserted quotes.
  - `inserted_bars_count`: Total number of inserted bars.
  - `error_count`: Total number of errors encountered during insertion.
  - `inserted_quotes_last_minute`: Number of quotes inserted in the last minute.
  - `inserted_bars_last_minute`: Number of bars inserted in the last minute.
  - `quote_id_counter`: Counter to assign unique IDs to quotes.
  - `batch_quotes`: List to batch quotes before insertion.
  - `batch_bars`: List to batch bars before insertion.
  - `info_logger`: Logger for informational messages.

### Main Functions

- **quote_callback**: Processes incoming quotes and batches them for insertion.
- **bar_callback**: Processes incoming bars and batches them for insertion.
- **batch_insert**: Periodically inserts batched quotes and bars into the PostgreSQL database.
- **log_counts**: Logs the number of quotes and bars inserted in the last minute, the total counts, and the total errors.
- **start_streaming**: Starts streaming quotes and bars for given symbols and handles insertion and logging.

### Supporting Functions

- **create_postgres_connection**: Creates and returns a new PostgreSQL connection.
- **create_hypertable**: Creates hypertables and applies retention policy in PostgreSQL/TimescaleDB.
- **stream_data**: Streams stock quotes and bars until the specified end time.

## Usage
To deploy this DAG:
1. Ensure all environment variables are set in your Airflow environment.
2. Verify that the PostgreSQL server is running and accessible.
3. Adjust the DAG and task settings as needed.
4. Activate the DAG via the Airflow dashboard.
