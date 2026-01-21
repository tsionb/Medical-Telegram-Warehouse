"""
Visual Content Statistics Router
Endpoint: /api/reports/visual-content
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List

from api.database import get_db
from api.schemas import VisualContentStatsResponse, APIResponse

router = APIRouter()

@router.get("/reports/visual-content", response_model=APIResponse)
async def get_visual_content_stats(
    db: Session = Depends(get_db)
):
    """
    Get statistics about image usage across channels
    
    Returns visual content statistics including:
    - Messages with images
    - Image categories (promotional, product_display, lifestyle)
    - Image usage by channel
    """
    try:
        # Query 1: Overall visual content statistics
        overall_query = """
        SELECT 
            COUNT(*) as total_messages,
            SUM(CASE WHEN has_image THEN 1 ELSE 0 END) as messages_with_images,
            ROUND((SUM(CASE WHEN has_image THEN 1 ELSE 0 END) * 100.0 / COUNT(*)), 2) as image_percentage
        FROM analytics_marts.fct_messages
        """
        
        overall_result = db.execute(overall_query)
        overall_stats = overall_result.fetchone()
        
        # Query 2: Visual content by channel
        channel_query = """
        SELECT 
            dc.channel_name,
            dc.channel_type,
            COUNT(fm.message_key) as total_messages,
            SUM(CASE WHEN fm.has_image THEN 1 ELSE 0 END) as messages_with_images,
            ROUND((SUM(CASE WHEN fm.has_image THEN 1 ELSE 0 END) * 100.0 / COUNT(fm.message_key)), 2) as image_percentage,
            SUM(CASE WHEN yd.image_category = 'promotional' THEN 1 ELSE 0 END) as promotional_posts,
            SUM(CASE WHEN yd.image_category = 'product_display' THEN 1 ELSE 0 END) as product_display_posts,
            SUM(CASE WHEN yd.image_category = 'lifestyle' THEN 1 ELSE 0 END) as lifestyle_posts
        FROM analytics_marts.fct_messages fm
        JOIN analytics_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        LEFT JOIN image_analysis.yolo_detections yd ON fm.message_id = yd.message_id
        GROUP BY dc.channel_name, dc.channel_type
        ORDER BY image_percentage DESC
        """
        
        channel_result = db.execute(channel_query)
        channel_stats = channel_result.fetchall()
        
        # Query 3: Image category distribution
        category_query = """
        SELECT 
            yd.image_category,
            COUNT(*) as count
        FROM image_analysis.yolo_detections yd
        WHERE yd.image_category IS NOT NULL
        GROUP BY yd.image_category
        ORDER BY count DESC
        """
        
        category_result = db.execute(category_query)
        categories = category_result.fetchall()
        
        # Format response
        channel_list = []
        for row in channel_stats:
            channel_list.append({
                "channel_name": row[0],
                "channel_type": row[1],
                "total_messages": row[2],
                "messages_with_images": row[3],
                "image_percentage": float(row[4]) if row[4] else 0,
                "promotional_posts": row[5] or 0,
                "product_display_posts": row[6] or 0,
                "lifestyle_posts": row[7] or 0
            })
        
        category_list = []
        for row in categories:
            category_list.append({
                "category": row[0],
                "count": row[1]
            })
        
        return APIResponse(
            success=True,
            message="Visual content statistics",
            data={
                "overall": {
                    "total_messages": overall_stats[0] if overall_stats else 0,
                    "messages_with_images": overall_stats[1] if overall_stats else 0,
                    "image_percentage": float(overall_stats[2]) if overall_stats and overall_stats[2] else 0
                },
                "by_channel": channel_list,
                "by_category": category_list,
                "insights": {
                    "top_channel_for_images": max(channel_list, key=lambda x: x["image_percentage"])["channel_name"] if channel_list else "N/A",
                    "most_common_category": max(category_list, key=lambda x: x["count"])["category"] if category_list else "N/A",
                    "total_images_analyzed": sum(row[1] for row in categories) if categories else 0
                }
            },
            count=len(channel_list)
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error fetching visual content stats: {str(e)}"
        )