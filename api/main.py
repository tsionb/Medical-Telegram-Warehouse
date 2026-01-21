"""
Main FastAPI application for Medical Telegram Analytics API
Building Analytical API
"""

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List, Optional
import uvicorn

# Import database and schemas
from api.database import get_db
from api.schemas import (
    TopProductResponse, ChannelActivityResponse, 
    MessageSearchResponse, VisualContentStatsResponse,
    SearchRequest, APIResponse
)

# Import routers
from api.routers import products, channels, search, visual

# Create FastAPI app
app = FastAPI(
    title="Medical Telegram Analytics API",
    description="API for analyzing Ethiopian medical business data from Telegram",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, restrict this!
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(products.router, prefix="/api", tags=["Products"])
app.include_router(channels.router, prefix="/api", tags=["Channels"])
app.include_router(search.router, prefix="/api", tags=["Search"])
app.include_router(visual.router, prefix="/api", tags=["Visual Content"])

# Health check endpoint
@app.get("/", tags=["Health"])
async def root():
    """Health check endpoint"""
    return {
        "message": "Medical Telegram Analytics API",
        "version": "1.0.0",
        "status": "running",
        "endpoints": [
            "/api/reports/top-products",
            "/api/channels/{channel_name}/activity",
            "/api/search/messages",
            "/api/reports/visual-content",
            "/docs (API documentation)"
        ]
    }

@app.get("/health", tags=["Health"])
async def health_check(db: Session = Depends(get_db)):
    """Health check with database connection"""
    try:
        # Test database connection
        db.execute("SELECT 1")
        return {
            "status": "healthy",
            "database": "connected",
            "timestamp": "Server time here"  # You can add datetime
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

if __name__ == "__main__":
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )