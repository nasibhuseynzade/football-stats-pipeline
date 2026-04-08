# ⚽ Premier League Statistics & Serverless ETL Pipeline

This repository contains the source code for a dynamic and visually appealing Premier League standings table and match fixtures application. 

Powered by an automated, event-driven ETL pipeline, this project fetches data periodically from the [Football-Data.org](https://www.football-data.org/) API. By decoupling the frontend from the data source using a serverless architecture, the application ensures lightning-fast load times, zero backend latency, and complete protection against API rate limits.

---
## Project is visualized in [my personal website](https://nasibhuseynzade.github.io/projects/pl/index.html).

## 📌 Project Architecture Overview

Instead of querying the external API directly on every user visit—which causes delays and risks hitting rate limits—this system uses an event-driven cloud backend. 

An AWS Lambda function wakes up on a schedule, transforms the raw API data into highly structured formats (CSV for standings, JSON for fixtures), and drops them into an Amazon S3 bucket. The frontend then simply fetches these pre-computed static files.


![Decoupled Multi-Output ETL Pipeline Architecture](pipeline.png)
*(Figure: Decoupled Multi-Output ETL Pipeline Architecture)*

---

## ✨ Key Features

* **Automated ETL Pipeline:** Scheduled data extraction and transformation running seamlessly in the background without manual intervention.
* **Incremental Processing:** Smart data merging that only processes new matches, significantly optimizing compute performance and reducing execution time.
* **Zero Latency:** The frontend is completely decoupled from the data pipeline. It fetches pre-computed static files directly from Amazon S3 for instant rendering.
* **Multi-Output Generation:** A single pipeline execution simultaneously generates clean, frontend-ready JSON for match fixtures and CSV files for league standings.

---

**AWS Lambda Handler Function for the whole ETL process:** [Premier_League/lambda_function.py](Premier_League/lambda_function.py)

---


## 🛠️ Technology Stack

| Category | Technologies Used |
| :--- | :--- |
| **Backend Processing** | Python, Pandas, Boto3 (AWS SDK) |
| **Cloud Infrastructure** | AWS Lambda, Amazon S3, AWS EventBridge |
| **Data Source** | Football-Data.org API |

---

## 📉 How It Works (The ETL Flow)

1. **Trigger (EventBridge):** Amazon EventBridge triggers the AWS Lambda function on a predefined schedule (e.g., daily or after match hours).
2. **Extract:** The Python script securely calls the Football-Data.org API to retrieve the latest raw match and standings data.
3. **Transform (Pandas):** The raw payload is processed incrementally. The script identifies new data, merges it with historical records, and formats it appropriately.
4. **Load (S3):** The transformed data is split and saved directly into a public-facing Amazon S3 bucket as `standings.csv` and `fixtures.json`.
5. **Consume:** The static HTML/JS frontend uses the Fetch API to render the UI instantly using the files sitting in the S3 bucket.

---

## 🚀 Getting Started

### Prerequisites
* An AWS Account (S3, Lambda, IAM, EventBridge)
* Python 3.x installed locally (for testing)
* A free API key from [Football-Data.org](https://www.football-data.org/)
