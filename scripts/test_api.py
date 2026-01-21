"""
Test the FastAPI endpoints
"""

import requests
import json
import time

BASE_URL = "http://localhost:8000"

def test_endpoint(endpoint, method="GET", params=None, data=None):
    """Test an API endpoint"""
    url = f"{BASE_URL}{endpoint}"
    
    print(f"\n Testing: {method} {endpoint}")
    print("-" * 60)
    
    try:
        if method == "GET":
            response = requests.get(url, params=params)
        elif method == "POST":
            response = requests.post(url, params=params, json=data)
        
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f" Success: {result.get('message', 'No message')}")
            
            if result.get('data'):
                # Show some data
                data = result['data']
                if 'top_products' in data and len(data['top_products']) > 0:
                    print(" Sample data (first 3 items):")
                    for item in data['top_products'][:3]:
                        print(f"  • {item['product_term']}: {item['frequency']} mentions")
                
                elif 'messages' in data and len(data['messages']) > 0:
                    print(f" Found {len(data['messages'])} messages")
                    if len(data['messages']) > 0:
                        msg = data['messages'][0]
                        print(f"  Sample: {msg['message_text'][:100]}...")
                
                elif 'activity' in data:
                    print(f" Activity data: {len(data['activity'])} days")
                
                elif 'overall' in data:
                    print(f" Overall: {data['overall']['messages_with_images']}/{data['overall']['total_messages']} messages with images")
            
            return True
        else:
            print(f" Error: {response.text}")
            return False
            
    except requests.exceptions.ConnectionError:
        print(" Cannot connect to API. Is it running?")
        print(f"   Run: python -m uvicorn api.main:app --reload")
        return False
    except Exception as e:
        print(f" Error: {e}")
        return False

def main():
    print("="*60)
    print(" FASTAPI TEST SCRIPT ")
    print("Testing all 4 required endpoints")
    print("="*60)
    
    # Check if API is running
    print("\n1. Checking API health...")
    if not test_endpoint("/health"):
        return
    
    print("\n" + "="*60)
    print("2. Testing all 4 required endpoints:")
    
    # Test 1: Top Products
    test_endpoint("/api/reports/top-products", params={"limit": 5})
    
    # Test 2: Channel Activity
    test_endpoint("/api/channels/CheMed123/activity", params={"days": 7})
    
    # Test 3: Message Search
    test_endpoint("/api/search/messages", params={"query": "paracetamol", "limit": 3})
    
    # Test 4: Visual Content Stats
    test_endpoint("/api/reports/visual-content")
    
    print("\n" + "="*60)
    print(" API TESTING COMPLETE!")
    print("\n API Documentation:")
    print("   Open: http://localhost:8000/docs")

if __name__ == "__main__":
    main()