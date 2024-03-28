import pandas as pd 
from bs4 import BeautifulSoup
import datetime
import sqlite3
import requests

def extract(url):
    
    # Fetching the HTML content from the provided URL
    html = requests.get(url).text
    
    # Parsing the HTML content
    soup = BeautifulSoup(html, 'html.parser')
    
    # Finding the table
    table = soup.find('table')
    
   # Lists to store extracted data
    bank_names = []
    market_caps = []
    # Looping through each row in the table
    for row in table.find_all('tr')[1:]:  # Skip the first row which contains headers
        # Extracting bank name and market cap from each row
        cols = row.find_all('td')
        bank_name = cols[1].find_all('a')[1].text
        market_cap = cols[2].text.strip()
        # Appending extracted data to lists
        bank_names.append(bank_name)
        market_caps.append(market_cap)
    # Creating a DataFrame from the extracted data
    df = pd.DataFrame({'Bank_Name': bank_names, 'Market Cap': market_caps})
    return df

def transform(df, csv_path):
    # Load exchange rate data from CSV file
    exchange_rates = pd.read_csv(csv_path)

    # Convert Market Cap column to numeric
    df['Market Cap'] = pd.to_numeric(df['Market Cap'], errors='coerce')

    # Convert Market Cap to respective currencies
    df['Market_Cap_USD'] = df['Market Cap']
    df['Market_Cap_EUR'] = df['Market Cap'] * exchange_rates.loc[exchange_rates['Currency'] == 'EUR', 'Rate'].values[0]
    df['Market_Cap_GBP'] = df['Market Cap'] * exchange_rates.loc[exchange_rates['Currency'] == 'GBP', 'Rate'].values[0]
    df['Market_Cap_INR'] = df['Market Cap'] * exchange_rates.loc[exchange_rates['Currency'] == 'INR', 'Rate'].values[0]

    df.drop(columns=['Market Cap'], inplace=True)
    return df

def load_to_csv(df,output_path):
    df.to_csv(output_path)
    

def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists = 'replace', index = False)



def run_query(query_statement, sql_connection):
    print("Query statement:")
    print(query_statement)
    print()
    
    # Execute the query
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    
    # Fetch and print the query output
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    
    # Close the cursor
    cursor.close()
    
log_file = 'code_log.txt'
def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open(log_file,"a") as f: 
        f.write(timestamp + ',' + message + '\n')

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
db_name = 'Banks.db'
table_name = 'Largest_banks'

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
output_path = 'Largest_banks_data.csv'

# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
df = extract(url)
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
print(df)

 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(df,'exchange_rate.csv')
print("Transformed Data") 
print(transformed_data)
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 

# Log the beginning of the Loading process 
log_progress("Load(CSV) phase Started") 
load_to_csv(transformed_data,output_path)
# Log the completion of the Loading process 
log_progress("Load(CSV) phase Ended") 

sql_connection = sqlite3.connect(db_name)
log_progress("Load(db) phase Started") 
load_to_db(transformed_data,sql_connection,table_name)
log_progress("Load(db) phase ended") 

# Log the completion of the ETL process 
log_progress("ETL Job Ended")  

query1 = "SELECT * FROM Largest_banks"
run_query(query1, sql_connection)
print()


query2 = "SELECT AVG(Market_Cap_GBP) FROM Largest_banks"
run_query(query2, sql_connection)
print()

# Query 3: Print only the names of the top 5 banks
query3 = "SELECT Bank_Name from Largest_banks LIMIT 5"
run_query(query3, sql_connection)


# Close the database connection
sql_connection.close()
 

 

 
  