import requests
import json

def test_sentiment_endpoint():
    # The URL is formed by the base host, the prefix defined in main.py, and the route in routers.py
    url = "http://localhost:8081/api/sentiment/"
    
    # Define the payload based on the SentimentRequest Pydantic model
    payload = {
        "text": "This product is absolutely amazing and I highly recommend it!",
        "comment_id": 101
    }
    
    headers = {
        "Content-Type": "application/json"
    }

    print(f"Sending request to: {url}")
    print(f"Payload: {json.dumps(payload, indent=2)}")

    try:
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code == 200:
            print("\nSuccess!")
            print("Response:", json.dumps(response.json(), indent=2))
        else:
            print(f"\nFailed with status code: {response.status_code}")
            print("Detail:", response.text)
            
    except requests.exceptions.ConnectionError:
        print("\nError: Could not connect to the server. Make sure your FastAPI app is running on http://localhost:8081")

if __name__ == "__main__":
    test_sentiment_endpoint()