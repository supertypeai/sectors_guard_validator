#!/usr/bin/env python3
"""
Simple test script to verify CORS configuration
Run this locally to test the API endpoints
"""

import requests
import json

def test_cors_endpoints():
    """Test CORS configuration on various endpoints"""
    
    # Base URL for local testing
    base_url = "http://localhost:8000"
    
    # Test endpoints
    endpoints = [
        "/",
        "/health", 
        "/debug-config",
        "/cors-test",
        "/api/validation/tables"
    ]
    
    print("🧪 Testing CORS configuration...")
    print("=" * 50)
    
    for endpoint in endpoints:
        try:
            url = f"{base_url}{endpoint}"
            print(f"\n📡 Testing: {url}")
            
            # Test preflight request (OPTIONS)
            options_response = requests.options(
                url,
                headers={
                    "Origin": "https://sectors-guard.vercel.app",
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "Content-Type",
                }
            )
            
            print(f"   OPTIONS Status: {options_response.status_code}")
            print(f"   CORS Headers: {dict(options_response.headers)}")
            
            # Test actual request
            if endpoint == "/api/validation/tables":
                response = requests.get(url)
            else:
                response = requests.get(url)
                
            print(f"   GET Status: {response.status_code}")
            print(f"   Response Headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                print("   ✅ Success")
            else:
                print(f"   ❌ Failed: {response.text}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print("\n" + "=" * 50)
    print("🏁 CORS test completed")

def test_production_cors():
    """Test CORS on production"""
    
    base_url = "https://sectors-guard-validator.fly.dev"
    
    endpoints = [
        "/",
        "/health",
        "/debug-config", 
        "/cors-test"
    ]
    
    print("🌐 Testing production CORS...")
    print("=" * 50)
    
    for endpoint in endpoints:
        try:
            url = f"{base_url}{endpoint}"
            print(f"\n📡 Testing: {url}")
            
            response = requests.get(
                url,
                headers={"Origin": "https://sectors-guard.vercel.app"},
                timeout=10
            )
            
            print(f"   Status: {response.status_code}")
            print(f"   CORS Headers: {response.headers.get('Access-Control-Allow-Origin', 'Not Set')}")
            
            if response.status_code == 200:
                print("   ✅ Success")
                if endpoint == "/debug-config":
                    print(f"   Config: {response.json()}")
            else:
                print(f"   ❌ Failed: {response.text}")
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
    
    print("\n" + "=" * 50)
    print("🏁 Production test completed")

if __name__ == "__main__":
    print("Choose test mode:")
    print("1. Local testing (requires server running on localhost:8000)")
    print("2. Production testing")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    if choice == "1":
        test_cors_endpoints()
    elif choice == "2":
        test_production_cors()
    else:
        print("Invalid choice")