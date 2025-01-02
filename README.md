# Trading Data Pipeline (in progress)

In the ever-evolving world of trading, a robust data strategy is essential to stay ahead. This project combines traditional market data, like stock prices and historical trends, with alternative data sources like social media sentiments and economic indicators to provide a comprehensive approach to trading data engineering.

## Data Pipeline
The pipeline employs a standardized Extract, Transform, Load (ETL) process, leveraging tools such as Pandas and Apache Spark.

Extract: We first extract/ingest data from market data providers, EODHD financial API, and alternative data sources. 
Transform: The data is then cleaned, validated, and transformed to ensure accuracy and reliability. 
Load: Finally, the processed data is stored in a centralized data lake leveraging Parquet, PostgreSQL, Docker, and TimescaleDB.

Apache Airflow is used for orchestration and automation of the pipeline.


## Tech Stack
Data Processing: Apache Spark, Pandas
Data Orchestration: Apache Airflow
Data Storage: TimescaleDB, Parquet, PostgreSQL
Automation & Deployment: Docker
APIs: EODHD financial API

## End Goal
- Develop analytics dashboards to help create advanced trading strategies.
- Incorporating real-time market data ingestion and processing.