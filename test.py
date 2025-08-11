# import requests


# # Function to call API and fetch data
# def fetch_data_from_api():
#     print("I am working fine!")
#     url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=MSFT"  # Replace with actual API URL
#     response = requests.get(url)
#     data = response.json()  # Assuming the response is in JSON format
#     return data


# fetch_data_from_api()


# import requests
# from dotenv import load_dotenv
# import os

# load_dotenv()

# def fetch_data_from_alpha_vantage():
#     api_key = os.getenv('ALPHA_VANTAGE_API_KEY')  # Fetch API key from .env file
#     if not api_key:
#         print("Error: API key is missing. Please check your .env file.")
#         return None
#     print("I am working fine with Alpha Vantage!")
#     url = "https://www.alphavantage.co/query"
#     params = {
#         "function": "TIME_SERIES_INTRADAY",
#         "symbol": "MSFT",
#         "interval": "5min",
#         "apikey": api_key  # Use your actual Alpha Vantage API key
#     }
    
#     response = requests.get(url, params=params)
    
#     if response.status_code == 200:
#         try:
#             data = response.json()
#             print("Data fetched successfully!")
#             return data
#         except ValueError as e:
#             print("Error: Failed to parse JSON response", e)
#             return None
#     else:
#         print(f"Error: Failed to fetch data, Status Code: {response.status_code}")
#         return None

# data = fetch_data_from_alpha_vantage()
# if data:
#     print(data)







import requests
from dotenv import load_dotenv
import os
import time

# Load environment variables from .env file
load_dotenv()

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

# Call the function to fetch data
data = fetch_data_from_alpha_vantage()

# if data:
#     print(data)









def process_data(data):
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




fine_data=process_data(data)

for item in fine_data:
    for key, value in item.items():
        print(key,value)
# print(fine_data)