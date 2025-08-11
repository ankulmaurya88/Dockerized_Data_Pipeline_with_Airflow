from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import requests
import json


from dotenv import load_dotenv
import os
import time

from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define the DAG
dag = DAG(
    'api_data_processing_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',  # Adjust the schedule as needed
)


'''
# Function to call API and fetch data
def fetch_data_from_api():
    url = "https://api.example.com/data"  # Replace with actual API URL
    response = requests.get(url)
    data = response.json()  # Assuming the response is in JSON format
    return data


'''

def fetch_data_from_alpha_vantage():
    # Fetch API key from .env file
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    # Check if API key exists
    if not api_key:
        print("Error: API key is missing. Please check your .env file.")
        return None
    
    print("I am working fine with Alpha Vantage!")

    # Define the URL and parameters for the API request
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": "MSFT",
        "interval": "5min",
        "apikey": api_key  # Use the API key from the .env file
    }
    
    # Send the GET request
    try:
        response = requests.get(url, params=params)
        
        # Check if the response status is OK (200)
        if response.status_code == 200:
            try:
                # Try to parse the response as JSON
                data = response.json()

                # Check if data is empty or contains an error message
                if 'Time Series (5min)' not in data:
                    print("Error: No stock data available. Response:", data)
                    return None
                
                print("Data fetched successfully!")
                return data

            except ValueError as e:
                print(f"Error: Failed to parse JSON response. Details: {e}")
                return None

        elif response.status_code == 429:
            # Handle rate limiting (429 status code)
            print("Rate limit exceeded. Waiting before retrying...")
            time.sleep(60)  # Wait for 60 seconds before retrying
            return fetch_data_from_alpha_vantage()  # Retry the function after the wait

        else:
            # Handle other HTTP errors (e.g., 404, 500)
            print(f"Error: Failed to fetch data. Status Code: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        # Handle network errors, timeouts, etc.
        print(f"Error: An error occurred while making the request. Details: {e}")
        return None















'''
# Function to process the data
def process_data(**kwargs):
    # Fetching the data from the previous task
    data = kwargs['task_instance'].xcom_pull(task_ids='fetch_data')
    
    # Perform some data processing (for example, filtering or cleaning)
    processed_data = []
    for item in data:
        # Example processing: extract needed fields and append to a new list
        processed_item = {
            'name': item.get('name'),
            'age': item.get('age'),
            'email': item.get('email')
        }
        processed_data.append(processed_item)

    # Returning processed data to the next task
    return processed_data









'''



def process_data(**kwargs):
    data = kwargs['task_instance'].xcom_pull(task_ids='fetch_data')
    if not data:
        print("Error: No data to process.")
        return []

    # Extract the time-series data
    try:
        time_series = data.get('Time Series (5min)', {})
        if not time_series:
            raise ValueError("No time series data found.")
    except KeyError as e:
        print(f"Error: Missing expected data field - {e}")
        return []
    except Exception as e:
        print(f"Unexpected error: {e}")
        return []

    # Process the data into a simplified format (e.g., extracting relevant fields)
    processed_data = []

    for timestamp, values in time_series.items():
        try:
            # Extracting the needed fields for each timestamp
            processed_item = {
                'timestamp': timestamp,
                'open': values.get('1. open'),
                'high': values.get('2. high'),
                'low': values.get('3. low'),
                'close': values.get('4. close'),
                'volume': values.get('5. volume')
            }
            # Check if any required field is missing
            if None in processed_item.values():
                raise ValueError(f"Missing data in timestamp {timestamp}: {processed_item}")
            
            # Append the processed item to the result list
            processed_data.append(processed_item)

        except KeyError as e:
            print(f"Error: Missing key {e} in timestamp {timestamp}")
            continue  # Skip to the next timestamp
        except ValueError as e:
            print(f"ValueError: {e}")
            continue  # Skip to the next timestamp
        except Exception as e:
            print(f"Unexpected error while processing timestamp {timestamp}: {e}")
            continue  # Skip to the next timestamp

    # Returning the processed data for the next task
    return processed_data







'''
# Function to update the data into PostgreSQL
def update_data_to_postgres(**kwargs):
    processed_data = kwargs['task_instance'].xcom_pull(task_ids='process_data')
    
    # Construct an SQL query to insert the data into your PostgreSQL table
    insert_query = """
    INSERT INTO your_table (name, age, email)
    VALUES (%s, %s, %s)
    """
    
    # Assuming you have a PostgreSQL connection set up in Airflow
    for item in processed_data:
        # Assuming `item` is a dictionary with keys 'name', 'age', 'email'
        values = (item['name'], item['age'], item['email'])
        
        # Use PostgresOperator or an appropriate method to insert data into the DB
        # Note: If you want to execute SQL directly, you can use PostgresHook to execute.
        # Here we are using PostgresOperator to insert rows
        postgres_operator = PostgresOperator(
            task_id='insert_data',
            sql=insert_query,
            parameters=values,
            postgres_conn_id='your_postgres_conn_id',  # Replace with your actual connection ID
            autocommit=True,
            dag=dag,
        )
        postgres_operator.execute(context=kwargs)
'''



def update_data_to_postgres(**kwargs):
    processed_data = kwargs['task_instance'].xcom_pull(task_ids='process_data')
    
    # Construct an SQL query to insert the data into your PostgreSQL table
    insert_query = """
    INSERT INTO trading_data (timestamp, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    # Assuming you have a PostgreSQL connection set up in Airflow
    for item in processed_data:
        # Assuming `item` is a dictionary with keys 'timestamp', 'open', 'high', 'low', 'close', 'volume'
        
        # Ensure the data types match the table schema
        values = (
            item['timestamp'],  # 'timestamp' should be in string or datetime format
            float(item['open']),  # 'open' should be float (decimal)
            float(item['high']),  # 'high' should be float (decimal)
            float(item['low']),   # 'low' should be float (decimal)
            float(item['close']), # 'close' should be float (decimal)
            int(item['volume'])   # 'volume' should be int
        )
        
        # Use PostgresOperator to insert rows into the DB
        postgres_operator = PostgresOperator(
            task_id='insert_data',
            sql=insert_query,
            parameters=values,
            postgres_conn_id='Trade',  # Replace with your actual connection ID
            autocommit=True,
            dag=dag,
        )
        
        # Execute the operator to perform the insertion
        postgres_operator.execute(context=kwargs)



# Define the tasks
task1 = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data_from_alpha_vantage,
    dag=dag,
)

task2 = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

task3 = PythonOperator(
    task_id='update_data',
    python_callable=update_data_to_postgres,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task1 >> task2 >> task3
