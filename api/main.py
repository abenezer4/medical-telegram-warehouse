from fastapi import FastAPI, HTTPException, Depends
from typing import List
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI(title="Medical Telegram Warehouse API", version="1.0.0")


@app.get("/")
def read_root():
    return {"message": "Welcome to the Medical Telegram Warehouse API"}


@app.get("/health")
def health_check():
    return {"status": "healthy"}


# Placeholder for future endpoints
@app.get("/messages")
def get_messages():
    """Retrieve messages from the data warehouse"""
    # This would connect to the database and return messages
    return {"message": "Messages endpoint - to be implemented with database connection"}


@app.get("/channels")
def get_channels():
    """Retrieve channel information from the data warehouse"""
    # This would connect to the database and return channel info
    return {"message": "Channels endpoint - to be implemented with database connection"}