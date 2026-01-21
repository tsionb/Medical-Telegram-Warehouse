"""
Top Products Router
Endpoint: /api/reports/top-products
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List
import re
from collections import Counter

from api.database import get_db
from api.schemas import TopProductResponse, APIResponse

router = APIRouter()

# Common medical product terms to look for 
MEDICAL_PRODUCT_TERMS = [
    # Medicines
    'paracetamol', 'aspirin', 'ibuprofen', 'amoxicillin', 'vitamin', 
    'antibiotic', 'painkiller', 'antacid', 'syrup', 'tablet', 'capsule',
    'ointment', 'cream', 'lotion', 'drops', 'injection',
    
    # Medical supplies
    'mask', 'gloves', 'thermometer', 'bandage', 'gauze', 'syringe',
    'cotton', 'alcohol', 'sanitizer', 'disinfectant',
    
    # Ethiopian medicine names 
    'ቫይታሚን', 'መድሃኒት', 'ፓራሲታሞል', 'አስፕሪን',  # Amharic terms
]

@router.get("/reports/top-products", response_model=APIResponse)
async def get_top_products(
    limit: int = Query(10, description="Number of top products to return", ge=1, le=50),
    db: Session = Depends(get_db)
):
    """
    Get top mentioned medical products across all channels
    
    Returns the most frequently mentioned medical product terms
    extracted from message text across all Telegram channels.
    """
    try:
        # Query all messages
        query = """
        SELECT message_text 
        FROM analytics_marts.fct_messages fm
        JOIN analytics_marts.dim_channels dc ON fm.channel_key = dc.channel_key
        WHERE message_text IS NOT NULL AND message_text != ''
        """
        
        result = db.execute(query)
        messages = [row[0] for row in result.fetchall()]
        
        if not messages:
            return APIResponse(
                success=True,
                message="No messages found",
                data={"top_products": []},
                count=0
            )
        
        # Extract product mentions
        all_terms = []
        
        for message in messages:
            # Convert to lowercase for case-insensitive matching
            text_lower = message.lower()
            
            # Look for medical product terms
            found_terms = []
            for term in MEDICAL_PRODUCT_TERMS:
                if term in text_lower:
                    found_terms.append(term)
            
            # Also extract capitalized words (might be product names)
            words = re.findall(r'\b[A-Z][a-z]+\b', message)
            found_terms.extend([w.lower() for w in words if len(w) > 3])
            
            all_terms.extend(found_terms)
        
        # Count frequencies
        term_counter = Counter(all_terms)
        
        # Get top N terms
        top_terms = term_counter.most_common(limit)
        
        # Calculate percentages
        total_mentions = sum(term_counter.values())
        
        # Format response
        top_products = []
        for term, count in top_terms:
            percentage = (count / total_mentions * 100) if total_mentions > 0 else 0
            top_products.append({
                "product_term": term,
                "frequency": count,
                "percentage": round(percentage, 2)
            })
        
        return APIResponse(
            success=True,
            message=f"Top {len(top_products)} medical products",
            data={"top_products": top_products},
            count=len(top_products)
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error processing top products: {str(e)}"
        )