from dagster import op, job, Config, Out
import subprocess
import os
import sys

@op
def scrape_telegram_data():
    """Run the telegram scraper script."""
    # Use the project's virtual environment python
    python_exe = os.path.join("venv8", "Scripts", "python.exe")
    result = subprocess.run([python_exe, "src/scraper.py", "--limit", "100"], check=True)
    return "Scraping completed"

@op
def load_raw_to_postgres(wait_for_scrape):
    """Run the loader script to move JSON data to Postgres."""
    python_exe = os.path.join("venv8", "Scripts", "python.exe")
    result = subprocess.run([python_exe, "src/loader.py"], check=True)
    return "Loading completed"

@op
def run_yolo_enrichment(wait_for_scrape):
    """Run the YOLO object detection script."""
    python_exe = os.path.join("venv8", "Scripts", "python.exe")
    result = subprocess.run([python_exe, "src/yolo_detect.py"], check=True)
    return "YOLO enrichment completed"

@op
def run_dbt_transformations(wait_for_load, wait_for_yolo):
    """Execute dbt models."""
    # Need to run loader again after YOLO to load detections, or ensure loader handles both
    # My loader.py already handles both if the file exists.
    # To be safe, run loader again after YOLO.
    python_exe = os.path.join("venv8", "Scripts", "python.exe")
    subprocess.run([python_exe, "src/loader.py"], check=True)
    
    dbt_exe = os.path.join("venv8", "Scripts", "dbt.exe")
    # Run dbt from the medical_warehouse directory
    result = subprocess.run([dbt_exe, "run"], cwd="medical_warehouse", check=True)
    return "dbt transformations completed"

@job
def medical_warehouse_pipeline():
    scrape_status = scrape_telegram_data()
    load_status = load_raw_to_postgres(scrape_status)
    yolo_status = run_yolo_enrichment(scrape_status)
    run_dbt_transformations(load_status, yolo_status)
