# ETL Pipeline Project: Large CSV Processing

This project downloads, transforms, and loads a large CSV dataset into a SQL database using Python. It handles the data in chunks to ensure memory efficiency and includes logging and error handling.

## Features
- Downloads a compressed CSV from a remote URL
- Cleans and transforms data using Pandas
- Stores the data in logically separated SQL tables
- Processes data in chunks for memory optimization
- Logs each major step and handles errors gracefully

## Tech Stack
- Python 3.8+
- Pandas
- SQLAlchemy
- SQLite (default DB, replaceable with any SQL DB)
- Requests

## Setup Instructions

### 1. Clone the repository or copy files
```bash
mkdir etl_project && cd etl_project
```

### 2. Install requirements
```bash
pip install pandas sqlalchemy requests
```

### 3. Files Structure
```
- etl_pipeline.py       # Main Python script
- database_schema.sql   # Optional schema to pre-create tables
- etl_pipeline.log      # Log file generated after execution
```

### 4. Run the ETL script
```bash
python etl_pipeline.py
```

This will:
- Download the CSV
- Read it chunk by chunk
- Clean and transform data
- Store results in SQLite DB `products.db`

## Notes
- Modify `CHUNK_SIZE` in `etl_pipeline.py` for tuning
- Default DB is SQLite for ease of setup; switch to MySQL/PostgreSQL by updating `create_engine()` URL

## Logging
Logs will be stored in `etl_pipeline.log`. Check it for any errors or status updates.

## License
MIT License
