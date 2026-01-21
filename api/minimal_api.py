from fastapi import FastAPI, Query
from pydantic import BaseModel
from typing import Optional
import uvicorn

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="Minimal API demonstrating all 4 required endpoints",
    version="1.0.0"
)

# Simple data models
class APIResponse(BaseModel):
    success: bool
    message: str
    data: dict
    count: int

# Endpoint 1: Top Products
@app.get("/api/reports/top-products", response_model=APIResponse)
async def top_products(limit: int = Query(10, ge=1, le=50)):
    """Endpoint 1: Top mentioned medical products"""
    mock_products = [
        {"product_term": "Paracetamol", "frequency": 45, "percentage": 18.0},
        {"product_term": "Vitamin C", "frequency": 32, "percentage": 12.8},
        {"product_term": "Amoxicillin", "frequency": 28, "percentage": 11.2},
        {"product_term": "Face Mask", "frequency": 25, "percentage": 10.0},
        {"product_term": "Hand Sanitizer", "frequency": 22, "percentage": 8.8}
    ]
    
    return APIResponse(
        success=True,
        message=f"Top {min(limit, 5)} medical products",
        data={"top_products": mock_products[:limit]},
        count=min(limit, 5)
    )

# Endpoint 2: Channel Activity
@app.get("/api/channels/{channel_name}/activity", response_model=APIResponse)
async def channel_activity(channel_name: str, days: int = 7):
    """Endpoint 2: Channel posting activity"""
    mock_activity = [
        {"date": "2026-01-12", "message_count": 5, "avg_views": 120.5, "avg_forwards": 3.2},
        {"date": "2026-01-13", "message_count": 7, "avg_views": 145.3, "avg_forwards": 4.1},
        {"date": "2026-01-14", "message_count": 6, "avg_views": 132.8, "avg_forwards": 3.8}
    ]
    
    return APIResponse(
        success=True,
        message=f"Activity for {channel_name} (last {days} days)",
        data={
            "channel_name": channel_name,
            "period_days": days,
            "activity": mock_activity
        },
        count=len(mock_activity)
    )

# Endpoint 3: Message Search
@app.get("/api/search/messages", response_model=APIResponse)
async def search_messages(
    query: str = Query(..., description="Search keyword"),
    limit: int = Query(20, ge=1, le=100)
):
    """Endpoint 3: Search messages by keyword"""
    mock_messages = [
        {
            "message_id": 123456,
            "channel_name": "CheMed123",
            "message_text": f"Looking for {query} at wholesale prices",
            "views": 150,
            "forwards": 5,
            "has_image": True,
            "message_date": "2026-01-15T10:30:00",
            "image_category": "product_display"
        },
        {
            "message_id": 123457,
            "channel_name": "tikvahpharma",
            "message_text": f"New shipment of {query} available",
            "views": 120,
            "forwards": 3,
            "has_image": True,
            "message_date": "2026-01-14T14:20:00",
            "image_category": "promotional"
        }
    ]
    
    return APIResponse(
        success=True,
        message=f"Found {min(limit, 2)} messages with '{query}'",
        data={"messages": mock_messages[:limit]},
        count=min(limit, 2)
    )

# Endpoint 4: Visual Content Stats
@app.get("/api/reports/visual-content", response_model=APIResponse)
async def visual_content_stats():
    """Endpoint 4: Visual content statistics"""
    mock_stats = {
        "overall": {
            "total_messages": 250,
            "messages_with_images": 194,
            "image_percentage": 77.6
        },
        "by_channel": [
            {
                "channel_name": "CheMed123",
                "total_messages": 50,
                "messages_with_images": 46,
                "image_percentage": 92.0,
                "promotional_posts": 15,
                "product_display_posts": 25,
                "lifestyle_posts": 6
            },
            {
                "channel_name": "lobelia4cosmetics",
                "total_messages": 50,
                "messages_with_images": 50,
                "image_percentage": 100.0,
                "promotional_posts": 20,
                "product_display_posts": 25,
                "lifestyle_posts": 5
            }
        ]
    }
    
    return APIResponse(
        success=True,
        message="Visual content statistics",
        data=mock_stats,
        count=len(mock_stats["by_channel"])
    )

# Health check
@app.get("/health")
async def health():
    return {"status": "healthy", "api": "running"}

# Root
@app.get("/")
async def root():
    return {
        "message": "Medical Telegram Analytics API",
        "status": "running",
        "endpoints": [
            "/api/reports/top-products",
            "/api/channels/{channel_name}/activity",
            "/api/search/messages",
            "/api/reports/visual-content",
            "/docs",
            "/health"
        ]
    }

if __name__ == "__main__":
    uvicorn.run(
        "api.minimal_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )