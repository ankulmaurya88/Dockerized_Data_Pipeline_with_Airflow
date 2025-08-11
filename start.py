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



dag = DAG(
    'api_data_processing_dag',
    start_date=datetime(2025, 8, 8),
    schedule_interval='@daily', 
)



def fetch_data_from_alpha_vantage():
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    if not api_key:
        print("Error: API key is missing. Please check your .env file.")
        return None
    
    print("I am working fine with Alpha Vantage!")

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": "MSFT",
        "interval": "5min",
        "apikey": api_key
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            try:
                data = response.json()

                if 'Time Series (5min)' not in data:
                    print("Error: No stock data available. Response:", data)
                    return None
                
                print("Data fetched successfully!")
                return data

            except ValueError as e:
                print(f"Error: Failed to parse JSON response. Details: {e}")
                return None

        elif response.status_code == 429:
            print("Rate limit exceeded. Waiting before retrying...")
            time.sleep(60)
            return fetch_data_from_alpha_vantage()

        else:
            print(f"Error: Failed to fetch data. Status Code: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"Error: An error occurred while making the request. Details: {e}")
        return None

def process_data(**kwargs):
    data = kwargs['task_instance'].xcom_pull(task_ids='fetch_data')
    if not data:
        print("Error: No data to process.")
        return []

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

    processed_data = []

    for timestamp, values in time_series.items():
        try:
            processed_item = {
                'timestamp': timestamp,
                'open': values.get('1. open'),
                'high': values.get('2. high'),
                'low': values.get('3. low'),
                'close': values.get('4. close'),
                'volume': values.get('5. volume')
            }
            if None in processed_item.values():
                raise ValueError(f"Missing data in timestamp {timestamp}: {processed_item}")
            
            processed_data.append(processed_item)

        except KeyError as e:
            print(f"Error: Missing key {e} in timestamp {timestamp}")
            continue
        except ValueError as e:
            print(f"ValueError: {e}")
            continue
        except Exception as e:
            print(f"Unexpected error while processing timestamp {timestamp}: {e}")
            continue

    return processed_data






def update_data_to_postgres(**kwargs):
    processed_data = kwargs['task_instance'].xcom_pull(task_ids='process_data')

    if not processed_data:
        print("Error: No processed data found to insert.")
        return

    insert_query = """
    INSERT INTO trading_data (timestamp, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s)
    """

    for item in processed_data:
        try:
            required_keys = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            for key in required_keys:
                if key not in item:
                    raise KeyError(f"Missing key '{key}' in data item: {item}")

            timestamp = item['timestamp']
            open_price = float(item['open'])
            high_price = float(item['high'])
            low_price = float(item['low'])
            close_price = float(item['close'])
            volume = int(item['volume'])

            values = (timestamp, open_price, high_price, low_price, close_price, volume)

            postgres_operator = PostgresOperator(
                task_id='insert_data',
                sql=insert_query,
                parameters=values,
                postgres_conn_id='Trade',
                autocommit=True,
                dag=dag,
            )

            postgres_operator.execute(context=kwargs)

        except KeyError as ke:
            print(f"Key error: {ke} - skipping this record.")
            continue

        except ValueError as ve:
            print(f"Value error: {ve} - invalid data types in record {item} - skipping this record.")
            continue

        except Exception as e:
            print(f"Unexpected error while inserting data: {e} - skipping this record.")
            continue





task_fetch_stock_data = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=fetch_data_from_alpha_vantage,
    dag=dag,
)

task_process_stock_data = PythonOperator(
    task_id='process_stock_data',
    python_callable=process_data,
    dag=dag,
)

task_insert_data_to_db = PythonOperator(
    task_id='insert_data_to_db',
    python_callable=update_data_to_postgres,
    dag=dag,
)

task_fetch_stock_data >> task_process_stock_data >> task_insert_data_to_db
