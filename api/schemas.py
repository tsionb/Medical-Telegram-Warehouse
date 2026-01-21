"""
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
