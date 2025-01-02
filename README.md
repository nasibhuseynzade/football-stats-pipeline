# Football Data Pipeline

A data engineering project to extract, transform, and load (ETL) football match data using the [Football Data API](https://www.football-data.org/). This project focuses on building a pipeline to fetch and analyze Premier League match data, with plans to expand to more complex datasets.

## Experiments
- First Experiment - Normalizing data 
- Second Experiment - Transforming matches data to a teams standing table
- Third Experiment - Team standing table with a daily scheduler
- Fourth Expeeriment - Loading to AWS S3 bucket
- Fifth Experiment - Loading to aws s3 with airflow (not done yet)
- Sixth Experiment - Loading to SQL 
- Lambda Handler function - Connecting to the website with API Gateway

## Features
- Extract match data from the Football Data API.
- Transform raw JSON data into a clean, structured format.
- Load processed data into CSV files for analysis.

## Tools Used
- **Python**: Data extraction and processing.
- **Pandas**: Data transformation and analysis.
- **Football Data API**: Data source for football match statistics.

## Installation
1. Clone this repository:
   ```bash
   git clone https://github.com/nasibhuseynzade/football-stats-pipeline.git
   cd football-data-pipeline
