# test_events_api.py
import requests
import json

BASE_URL = "http://127.0.0.1:8082"

def test_health():
    """Test health endpoint"""
    response = requests.get(f"{BASE_URL}/api/events/health")
    print(f"Health Check: {response.status_code}")
    print(f"Response: {response.json()}\n")
    return response.status_code == 200

def test_movie_event():
    """Test movie event creation"""
    data = {
        "movie_id": 123,
        "title": "Inception",
        "genre": "Sci-Fi",
        "year": 2010,
        "action": "CREATE"
    }
    response = requests.post(f"{BASE_URL}/api/events/movie", json=data)
    print(f"Movie Event: {response.status_code}")
    print(f"Response: {response.json()}\n")
    return response.status_code == 201

def test_user_event():
    """Test user event creation"""
    data = {
        "user_id": 1,
        "username": "johndoe",
        "email": "john@example.com",
        "action": "REGISTER"
    }
    response = requests.post(f"{BASE_URL}/api/events/user", json=data)
    print(f"User Event: {response.status_code}")
    print(f"Response: {response.json()}\n")
    return response.status_code == 201

def test_payment_event():
    """Test payment event creation"""
    data = {
        "payment_id": 1,
        "user_id": 1,
        "amount": 19.99,
        "currency": "USD",
        "status": "COMPLETED"
    }
    response = requests.post(f"{BASE_URL}/api/events/payment", json=data)
    print(f"Payment Event: {response.status_code}")
    print(f"Response: {response.json()}\n")
    return response.status_code == 201

if __name__ == "__main__":
    print("Testing Events Microservice API...")
    print("=" * 50)
    
    tests = [
        test_health,
        test_movie_event,
        test_user_event,
        test_payment_event
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append(result)
        except Exception as e:
            print(f"Test failed: {e}")
            results.append(False)
    
    print("=" * 50)
    print(f"Tests passed: {sum(results)}/{len(results)}")
    print(f"All tests {'PASSED' if all(results) else 'FAILED'}")