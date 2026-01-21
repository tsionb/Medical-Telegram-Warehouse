"""
Channel Activity Router
Endpoint: /api/channels/{channel_name}/activity
"""

from fastapi import APIRouter, Depends, HTTPException, Path
from sqlalchemy.orm import Session
from typing import List
from datetime import datetime, timedelta

from api.database import get_db
from api.schemas import ChannelActivityResponse, APIResponse

router = APIRouter()

@router.get("/channels/{channel_name}/activity", response_model=APIResponse)
async def get_channel_activity(
    channel_name: str = Path(..., description="Name of the Telegram channel"),
    days: int = 7,
    db: Session = Depends(get_db)
):
    """
    Get posting activity and trends for a specific channel
    
    Returns daily message counts, average views, and forwards
    for the specified channel over the given time period.
    """
    try:
        # Validate channel exists
        channel_check = """
        SELECT channel_name FROM analytics_marts.dim_channels 
        WHERE channel_name = %s
        """
        result = db.execute(channel_check, (channel_name,))
        if not result.fetchone():
            raise HTTPException(
                status_code=404,
                detail=f"Channel '{channel_name}' not found"
            )
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        # Query channel activity
        query = """
        SELECT 
            TO_CHAR(dd.full_date, 'YYYY-MM-DD') as date,
            COUNT(fm.message_key) as message_count,
            COALESCE(AVG(fm.views), 0) as avg_views,
            COALESCE(AVG(fm.forwards), 0) as avg_forwards
        FROM analytics_marts.fct_messages fm
        JOIN analytics_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        JOIN analytics_marts.dim_dates dd ON fm.date_key = dd.date_key
        WHERE dc.channel_name = %s
            AND dd.full_date >= %s
            AND dd.full_date <= %s
        GROUP BY dd.full_date
        ORDER BY dd.full_date DESC
        """
        
        result = db.execute(query, (channel_name, start_date, end_date))
        activity_data = result.fetchall()
        
        # Format response
        activity_list = []
        for row in activity_data:
            activity_list.append({
                "date": row[0],
                "message_count": row[1],
                "avg_views": float(row[2]) if row[2] else 0,
                "avg_forwards": float(row[3]) if row[3] else 0
            })
        
        # Get channel statistics
        stats_query = """
        SELECT 
            total_posts,
            avg_views,
            avg_forwards,
            image_percentage
        FROM analytics_marts.dim_channels
        WHERE channel_name = %s
        """
        
        stats_result = db.execute(stats_query, (channel_name,))
        stats = stats_result.fetchone()
        
        return APIResponse(
            success=True,
            message=f"Activity for channel '{channel_name}'",
            data={
                "channel_name": channel_name,
                "period_days": days,
                "activity": activity_list,
                "statistics": {
                    "total_posts": stats[0] if stats else 0,
                    "avg_views": float(stats[1]) if stats and stats[1] else 0,
                    "avg_forwards": float(stats[2]) if stats and stats[2] else 0,
                    "image_percentage": float(stats[3]) if stats and stats[3] else 0
                } if stats else {}
            },
            count=len(activity_list)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching channel activity: {str(e)}"
        )