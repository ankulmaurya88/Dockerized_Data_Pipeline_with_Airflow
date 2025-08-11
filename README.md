
# Airflow Data Pipeline with Docker Compose

## Overview
This project sets up an Apache Airflow data pipeline using Docker Compose. The pipeline fetches stock data from Alpha Vantage API, processes it, and stores it in a PostgreSQL database.

## Prerequisites
- Docker & Docker Compose installed
- Alpha Vantage API key
- Basic knowledge of Airflow and Docker

## Setup

1. Clone the repo:
git clone https://github.com/ankulmaurya88/Dockerized_Data_Pipeline_with_Airflow.git
cd Dockerized_Data_Pipeline_with_Airflow

2. Create a .env file and add your Alpha Vantage API key:
ALPHA_VANTAGE_API_KEY=your_api_key_here

3. Generate Fernet and Secret keys for Airflow:
openssl rand -base64 32  # use output for AIRFLOW__CORE__FERNET_KEY
openssl rand -base64 32  # use output for AIRFLOW__WEBSERVER__SECRET_KEY

4. Update docker-compose.yml with your generated keys.

## Running the Pipeline

Start the Airflow environment:
docker-compose up -d

Initialize the database and create admin user (if needed):
docker-compose exec airflow-webserver airflow db init
docker-compose exec airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com

Access Airflow UI at http://localhost:8080

Trigger the DAG manually or wait for the scheduled run.

## Pipeline Structure
fetch_data: Fetch stock data from Alpha Vantage
process_data: Process the fetched data
update_data: Insert processed data into PostgreSQL

## Notes
Customize PostgreSQL connection in Airflow UI (Admin > Connections) with your DB details.
Modify DAG schedule or parameters in dags/your_dag.py as needed.

## Troubleshooting
Check container logs:
docker-compose logs -f airflow-webserver

Ensure .env variables are loaded correctly.

Happy data pipelining! 🚀
EOF
