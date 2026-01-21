from fastapi import FastAPI, HTTPException, Depends, Query
from typing import List, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text
from . import database, schemas
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

@app.get("/api/reports/top-products", response_model=List[schemas.TopProduct])
def get_top_products(limit: int = 10, db: Session = Depends(database.get_db)):
    """Returns the most frequently mentioned terms/products across all channels."""
    # Simplified approach: search for common medical keywords or use word frequency
    # For now, let's assume we have a way to count product mentions or just use a dummy query
    # based on the data we have. A real implementation would use NLP or a specific mapping table.
    query = text("""
        SELECT 
            split_part(message_text, ' ', 1) as product, 
            count(*) as mention_count
        FROM fct_messages 
        WHERE message_text IS NOT NULL AND message_text != ''
        GROUP BY product 
        ORDER BY mention_count DESC 
        LIMIT :limit
    """)
    result = db.execute(query, {"limit": limit}).fetchall()
    return [{"product": r[0], "mention_count": r[1]} for r in result]

@app.get("/api/channels/{channel_name}/activity", response_model=List[schemas.ChannelActivity])
def get_channel_activity(channel_name: str, db: Session = Depends(database.get_db)):
    """Returns posting activity and trends for a specific channel."""
    query = text("""
        SELECT 
            d.full_date as date, 
            count(m.message_id) as message_count
        FROM fct_messages m
        JOIN dim_channels c ON m.channel_key = c.channel_key
        JOIN dim_dates d ON m.date_key = d.date_key
        WHERE c.channel_name = :channel_name
        GROUP BY d.full_date
        ORDER BY d.full_date
    """)
    result = db.execute(query, {"channel_name": channel_name}).fetchall()
    return [{"date": r[0], "message_count": r[1]} for r in result]

@app.get("/api/search/messages")
def search_messages(query: str = Query(..., min_length=3), limit: int = 20, db: Session = Depends(database.get_db)):
    """Searches for messages containing a specific keyword."""
    sql_query = text("""
        SELECT 
            message_id, 
            message_text, 
            view_count, 
            forward_count
        FROM fct_messages
        WHERE message_text ILIKE :search_query
        LIMIT :limit
    """
    )
    result = db.execute(sql_query, {"search_query": f"%{query}%", "limit": limit}).fetchall()
    return [dict(row._mapping) for row in result]

@app.get("/api/reports/visual-content", response_model=List[schemas.VisualContentStats])
def get_visual_content_stats(db: Session = Depends(database.get_db)):
    """Returns statistics about image usage across channels."""
    query = text("""
        SELECT 
            c.channel_name,
            count(*) as total_images,
            avg(confidence_score) as avg_confidence,
            count(*) FILTER (WHERE image_category = 'promotional') as promotional_count,
            count(*) FILTER (WHERE image_category = 'product_display') as product_display_count
        FROM fct_image_detections f
        JOIN dim_channels c ON f.channel_key = c.channel_key
        GROUP BY c.channel_name
    """
    )
    result = db.execute(query).fetchall()
    return [
        {
            "channel_name": r[0], 
            "total_images": r[1], 
            "avg_confidence": r[2] or 0.0,
            "promotional_count": r[3],
            "product_display_count": r[4]
        } for r in result
    ]