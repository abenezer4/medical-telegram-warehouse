# Medical Telegram Warehouse

This project implements a data pipeline to scrape medical-related content from Telegram channels, store it in a data lake, transform it using dbt, and serve it through an API.

## Architecture Overview

```
Telegram Channels → Data Lake (JSON/Images) → Data Warehouse (PostgreSQL) → API Layer
```

## Components

### 1. Data Scraping (`src/`)
- Telegram scraper using Telethon
- Downloads messages and images
- Stores in partitioned format

### 2. Data Warehouse (`medical_warehouse/`)
- Dimensional modeling with dbt
- Star schema design
- Automated tests and documentation

### 3. API Layer (`api/`)
- FastAPI-based REST API
- Serves transformed data
- Includes data validation

### 4. Analytics (`notebooks/`)
- Jupyter notebooks for data exploration
- Demonstration of pipeline functionality
- Analysis examples

## Setup Instructions

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Set up environment variables in `.env`
4. Run the scraper: `python src/scraper.py`
5. Transform data with dbt: `dbt run`
6. Start the API: `uvicorn api.main:app --reload`

## Environment Variables

Create a `.env` file with the following variables:

```bash
TG_API_ID=your_api_id_here
TG_API_HASH=your_api_hash_here
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=medical_warehouse
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
```

## Data Flow

1. **Scraping**: Collect messages and images from specified Telegram channels
2. **Storage**: Store raw data in structured data lake (partitioned by date)
3. **Transformation**: Apply dimensional modeling with dbt
4. **Serving**: Expose data through API endpoints

## Project Structure

```
medical-telegram-warehouse/
├── .vscode/
├── .github/workflows/
├── .env
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
├── README.md
├── data/
│   └── raw/
│       ├── telegram_messages/
│       ├── images/
│       └── manifests/
├── logs/
├── medical_warehouse/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   └── tests/
├── src/
├── api/
├── notebooks/
├── scripts/
└── tests/
```
