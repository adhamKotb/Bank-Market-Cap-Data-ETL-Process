# Bank-Market-Cap-Data-ETL-Process

This Python script performs an Extract, Transform, and Load (ETL) process on bank market capitalization data extracted from a Wikipedia page. It extracts bank names and market capitalizations using web scraping techniques, transforms the data by converting market cap values to multiple currencies based on exchange rates, and loads the transformed data into both a CSV file and a SQLite database. The script also logs progress messages with timestamps to a text file for monitoring. It provides a simple yet effective tool for gathering and analyzing bank market cap data from publicly available sources.

## Functions Overview

### extract(url)
- **Description**: Extracts bank names and market capitalizations from the provided URL using web scraping techniques.
- **Parameters**:
  - `url` (str): The URL of the webpage containing bank market cap data.
- **Returns**:
  - `DataFrame`: Pandas DataFrame containing extracted bank names and market caps.

### transform(df, csv_path)
- **Description**: Transforms the extracted data by converting market cap values to multiple currencies based on exchange rates provided in a CSV file.
- **Parameters**:
  - `df` (DataFrame): Pandas DataFrame containing bank names and market caps.
  - `csv_path` (str): Path to the CSV file containing exchange rate data.
- **Returns**:
  - `DataFrame`: Transformed Pandas DataFrame with market caps in multiple currencies.

### load_to_csv(df, output_path)
- **Description**: Loads the transformed data into a CSV file.
- **Parameters**:
  - `df` (DataFrame): Transformed Pandas DataFrame.
  - `output_path` (str): Path to save the CSV file.
- **Returns**:
  - None

### load_to_db(df, sql_connection, table_name)
- **Description**: Loads the transformed data into a SQLite database table.
- **Parameters**:
  - `df` (DataFrame): Transformed Pandas DataFrame.
  - `sql_connection` (sqlite3.Connection): SQLite database connection.
  - `table_name` (str): Name of the table to load data into.
- **Returns**:
  - None

### run_query(query_statement, sql_connection)
- **Description**: Executes a SQL query and prints the result.
- **Parameters**:
  - `query_statement` (str): SQL query to execute.
  - `sql_connection` (sqlite3.Connection): SQLite database connection.
- **Returns**:
  - None

### log_progress(message)
- **Description**: Logs progress messages with timestamps to a text file.
- **Parameters**:
  - `message` (str): Message to log.
- **Returns**:
  - None


## Installation
Follow these steps to install and run the script:

1. Install the required Python libraries using pip: pip install pandas beautifulsoup4 requests
2. Ensure you have the necessary input files:
- `exchange_rate.csv`: CSV file containing exchange rate data.
- `code_log.txt`: Text file for logging progress.

3. Run the `main.py` script
4. The script will perform the ETL process and output the results to `Largest_banks_data.csv` and `Banks.db`.

## Project Structure

- **main.py**: Contains the main script for the ETL process.
- **exchange_rate.csv**: CSV file containing exchange rate data.
- **code_log.txt**: Text file for logging progress.
- **README.md**: This documentation file.

## Contributing

Contributions are welcome! Feel free to open an issue or submit a pull request.





