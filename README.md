# Alpaca Quote Bars Stream DAG

## Overview
This Airflow Directed Acyclic Graph (DAG) is designed to manage the streaming and storage of live stock data for the 3000 stocks listed in the Russell 3000 index. It operates every working day from the US stock market's premarket opening to the aftermarket close, fetching live quotes and 1-minute bars and inserting them into a TimescaleDB hypertable for robust time-series data management.

## Features
- **Data Coverage**: Streams live quotes and 1-minute bars for 3000 Russell index stocks.
- **High Volume Handling**: Capable of processing approximately 80 million quotes and 70,000 1-minute bars daily.
- **Precision Timing**: Aligns operations with US stock market timings, ensuring data is captured throughout the trading day.
- **Data Integrity and Storage**: Utilizes PostgreSQL with TimescaleDB extension for efficient large-scale time-series data handling.
- **Robust Error Handling and Logging**: Detailed logging for error tracking and operational integrity ensures reliable data insertion and system stability.

## DAG Configuration
- **Schedule**: Runs at 03:59 AM UTC every working day, coinciding with the US stock market's premarket opening.
- **Catchup**: False — prevents the DAG from executing on past dates if a scheduled run is missed.
- **Retries**: Configured to retry once with a 5-minute delay if a run fails.

## Components
- **QuoteStreamer**: Manages the streaming, batch processing, and database insertion of quotes and bars. Also handles hypertable creation for scalable data management.
- **Alpaca API Integration**: Connects to Alpaca's trading API to receive real-time data.
- **Concurrency and Asynchronous Processing**: Uses `asyncio` to manage concurrent data streams efficiently.
- **Error and Performance Logging**: Employs Python's logging module to monitor system performance and errors.

## Setup and Dependencies
### Environment Variables
- `ALPACA_API_KEY_JOHNYSSAN`: API key for Alpaca account.
- `ALPACA_SECRET_KEY_JOHNYSSAN`: Secret key for Alpaca account.
- `TIMESCALE_LIVE_CONN_STRING`: Connection string for the PostgreSQL database.

### External Libraries
- `psycopg2`: Adapter for the PostgreSQL database.
- `pandas`: For handling large datasets and CSV file operations.
- `pytz`: Provides timezone definitions for Python.
- `alpaca-trade-api`: Alpaca brokerage API for stock trading.
- `airflow`: For automating workflow.
- `nest_asyncio`: Enables nested use of asyncio's event loop.

## Usage
To deploy this DAG:
1. Ensure all environment variables are set in your Airflow environment.
2. Verify that the PostgreSQL server is running and accessible.
3. Adjust the DAG and task settings as needed.
4. Activate the DAG via the Airflow dashboard.

## Conclusion
This DAG provides a scalable, efficient solution for financial data analysts and quantitative traders requiring real-time access to substantial volumes of stock data. Leveraging modern data engineering tools and practices, it ensures that critical market data is timely, accurate, and readily accessible for decision-making and analytics.
