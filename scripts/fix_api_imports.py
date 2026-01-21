"""Fix API import issues"""

import os
import shutil
from pathlib import Path

def fix_schemas():
    """Fix the schemas import issue"""
    print(" Fixing API schema imports...")
    
    # Remove empty __init__.py files
    init_files = [
        "api/schemas/__init__.py",
        "api/routers/__init__.py"
    ]
    
    for init_file in init_files:
        if os.path.exists(init_file):
            os.remove(init_file)
            print(f" Removed: {init_file}")
    
    # Create proper schemas.py
    schemas_content = '''"""
Pydantic schemas for request/response validation
"""

from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime

class MessageBase(BaseModel):
    """Base message schema"""
    message_id: int
    channel_name: str
    message_text: str
    views: int
    forwards: int
    has_image: bool

class ChannelBase(BaseModel):
    """Base channel schema"""
    channel_name: str
    channel_type: str
    total_posts: int
    avg_views: float
    avg_forwards: float
    image_percentage: float

class SearchRequest(BaseModel):
    """Search request schema"""
    query: str
    limit: Optional[int] = 20

class TopProductResponse(BaseModel):
    """Top products response"""
    product_term: str
    frequency: int
    percentage: float

class ChannelActivityResponse(BaseModel):
    """Channel activity response"""
    date: str
    message_count: int
    avg_views: float
    avg_forwards: float

class MessageSearchResponse(MessageBase):
    """Message search response"""
    message_date: datetime
    image_category: Optional[str] = None
    detected_objects: Optional[str] = None

class VisualContentStatsResponse(BaseModel):
    """Visual content statistics"""
    channel_name: str
    total_messages: int
    messages_with_images: int
    image_percentage: float
    promotional_posts: int
    product_display_posts: int
    lifestyle_posts: int

class APIResponse(BaseModel):
    """Generic API response"""
    success: bool
    message: str
    data: Optional[dict] = None
    count: Optional[int] = None
'''
    
    with open("api/schemas.py", "w") as f:
        f.write(schemas_content)
    print(" Created: api/schemas.py")
    
    # Update main.py imports
    main_py_path = "api/main.py"
    if os.path.exists(main_py_path):
        with open(main_py_path, "r") as f:
            content = f.read()
        
        # Fix import lines
        content = content.replace(
            "from api.schemas import (",
            "from api.schemas import ("
        )
        
        with open(main_py_path, "w") as f:
            f.write(content)
        print(" Updated: api/main.py")
    
    # Create __init__.py files (empty is fine now)
    init_files_to_create = [
        "api/__init__.py",
        "api/routers/__init__.py"
    ]
    
    for init_file in init_files_to_create:
        Path(init_file).touch()
        print(f" Created: {init_file}")
    
    return True

def create_simple_api_test():
    """Create a simple test API to verify"""
    print("\n Creating simple test API...")
    
    simple_main = '''#!/usr/bin/env python3
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
'''
    
    with open("api/simple_main.py", "w") as f:
        f.write(simple_main)
    print(" Created: api/simple_main.py")
    
    return True

def main():
    """Main fix function"""
    print("="*60)
    print("🔧 FASTAPI IMPORT FIX SCRIPT")
    print("="*60)
    
    # Option 1: Fix the existing API
    fix_schemas()
    
    
    # Create simple version as backup
    create_simple_api_test()
    
    print("\n" + "="*60)
    print(" Fix script complete!")

if __name__ == "__main__":
    main()