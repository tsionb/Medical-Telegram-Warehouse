#!/usr/bin/env python3
"""
Simple FastAPI Test - Medical Telegram Analytics
"""

from fastapi import FastAPI
import uvicorn

app = FastAPI(
    title="Medical Telegram Analytics API",
    description="API for analyzing Ethiopian medical business data",
    version="1.0.0"
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Medical Telegram Analytics API",
        "status": "running",
        "endpoints": [
            "/api/reports/top-products",
            "/api/channels/{channel_name}/activity",
            "/api/search/messages",
            "/api/reports/visual-content",
            "/docs (API documentation)"
        ]
    }

@app.get("/health")
async def health():
    """Health check"""
    return {"status": "healthy"}

@app.get("/api/reports/top-products")
async def top_products(limit: int = 10):
    """Top products endpoint (mock data for testing)"""
    return {
        "success": True,
        "message": f"Top {limit} medical products",
        "data": {
            "top_products": [
                {"product_term": "Paracetamol", "frequency": 45, "percentage": 18.0},
                {"product_term": "Vitamin C", "frequency": 32, "percentage": 12.8},
                {"product_term": "Amoxicillin", "frequency": 28, "percentage": 11.2},
                {"product_term": "Face Mask", "frequency": 25, "percentage": 10.0},
                {"product_term": "Hand Sanitizer", "frequency": 22, "percentage": 8.8}
            ]
        },
        "count": 5
    }

@app.get("/api/channels/{channel_name}/activity")
async def channel_activity(channel_name: str, days: int = 7):
    """Channel activity endpoint (mock data)"""
    return {
        "success": True,
        "message": f"Activity for {channel_name}",
        "data": {
            "channel_name": channel_name,
            "period_days": days,
            "activity": [
                {"date": "2026-01-12", "message_count": 5, "avg_views": 120.5, "avg_forwards": 3.2},
                {"date": "2026-01-13", "message_count": 7, "avg_views": 145.3, "avg_forwards": 4.1},
                {"date": "2026-01-14", "message_count": 6, "avg_views": 132.8, "avg_forwards": 3.8}
            ]
        },
        "count": 3
    }

@app.get("/api/search/messages")
async def search_messages(query: str, limit: int = 20):
    """Message search endpoint (mock data)"""
    return {
        "success": True,
        "message": f"Found messages with '{query}'",
        "data": {
            "messages": [
                {
                    "message_id": 123456,
                    "channel_name": "CheMed123",
                    "message_text": f"Looking for {query} at wholesale prices",
                    "views": 150,
                    "forwards": 5,
                    "has_image": True,
                    "message_date": "2026-01-15T10:30:00",
                    "image_category": "product_display"
                }
            ]
        },
        "count": 1
    }

@app.get("/api/reports/visual-content")
async def visual_content_stats():
    """Visual content stats endpoint (mock data)"""
    return {
        "success": True,
        "message": "Visual content statistics",
        "data": {
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
                }
            ]
        },
        "count": 1
    }

if __name__ == "__main__":
    uvicorn.run(
        "api.simple_main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
