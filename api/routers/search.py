"""
Message Search Router
Endpoint: /api/search/messages
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional

from api.database import get_db
from api.schemas import MessageSearchResponse, APIResponse, SearchRequest

router = APIRouter()

@router.get("/search/messages", response_model=APIResponse)
async def search_messages(
    query: str = Query(..., description="Search keyword"),
    channel_name: Optional[str] = Query(None, description="Filter by channel"),
    limit: int = Query(20, description="Maximum results", ge=1, le=100),
    db: Session = Depends(get_db)
):
    """
    Search for messages containing specific keywords
    
    Returns messages that match the search query, optionally
    filtered by channel name.
    """
    try:
        # Build SQL query with full-text search
        sql = """
        SELECT 
            fm.message_id,
            dc.channel_name,
            fm.message_text,
            fm.views,
            fm.forwards,
            fm.has_image,
            dd.full_date as message_date,
            yd.image_category,
            yd.detected_objects
        FROM analytics_marts.fct_messages fm
        JOIN analytics_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        JOIN analytics_marts.dim_dates dd ON fm.date_key = dd.date_key
        LEFT JOIN image_analysis.yolo_detections yd ON fm.message_id = yd.message_id
        WHERE fm.message_text ILIKE %s
        """
        
        params = [f"%{query}%"]
        
        # Add channel filter if provided
        if channel_name:
            sql += " AND dc.channel_name = %s"
            params.append(channel_name)
        
        # Add ordering and limit
        sql += " ORDER BY fm.views DESC LIMIT %s"
        params.append(limit)
        
        # Execute query
        result = db.execute(sql, params)
        messages = result.fetchall()
        
        # Format response
        message_list = []
        for row in messages:
            message_list.append({
                "message_id": row[0],
                "channel_name": row[1],
                "message_text": row[2],
                "views": row[3],
                "forwards": row[4],
                "has_image": row[5],
                "message_date": row[6],
                "image_category": row[7],
                "detected_objects": row[8]
            })
        
        return APIResponse(
            success=True,
            message=f"Found {len(message_list)} messages matching '{query}'",
            data={"messages": message_list},
            count=len(message_list)
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error searching messages: {str(e)}"
        )

@router.post("/search/messages", response_model=APIResponse)
async def search_messages_post(
    request: SearchRequest,
    channel_name: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Search for messages (POST version with request body)
    """
    return await search_messages(
        query=request.query,
        channel_name=channel_name,
        limit=request.limit,
        db=db
    )