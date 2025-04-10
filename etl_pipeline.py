from flask import Flask, request, jsonify
import requests
import pandas as pd
import pymongo
import json  
from datetime import datetime
from flask_cors import CORS


# Initialize Flask App
app = Flask(__name__)
CORS(app)  # Allow Cross-Origin Requests

# MongoDB Connection
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "etl_db"
COLLECTION_NAME = "weather_data"

# OpenWeatherMap API Details
API_KEY = "458bb89e4faa4516b75194516250204"

def get_weather_data(city):
    """Fetch weather data for a given city."""
    API_URL = f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q={city}&aqi=no"

    try:
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise exception if request fails

        data = response.json()

        # Save raw data (for debugging)
        with open("raw_data.json", "w") as file:
            json.dump(data, file, indent=4)  

        return data
    except requests.exceptions.RequestException as e:
        print(f"[{datetime.now()}] ERROR: Failed to fetch data - {e}")
        return None

def transform(data):
    """Transform raw JSON data into a structured format."""
    if not data:
        return None

    df = pd.json_normalize(data, sep="_")  # Flatten JSON structure

    # Data Cleaning
    df.dropna(inplace=True)
    df.drop_duplicates(inplace=True)

    cleaned_data = df.to_dict(orient="records")[0]  # Convert first record to dict

    # Add timestamp for latest update
    cleaned_data["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Save cleaned data (for debugging)
    with open("cleaned_data.json", "w") as file:
        json.dump(cleaned_data, file, indent=4)

    return cleaned_data

def load(data):
    """Load cleaned data into MongoDB (overwrite old record for the city)."""
    if not data:
        print(f"[{datetime.now()}] No data to insert.")
        return

    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Ensure only the latest record is stored for each city
    collection.update_one(
        {"location_name": data["location_name"]},  # Find by city name
        {"$set": data},  # Update with latest data
        upsert=True  # Insert if not exists
    )

    print(f"[{datetime.now()}] Data updated for {data['location_name']} in MongoDB.")

@app.route("/api/weather", methods=["POST"])
def weather_api():
    """API Endpoint to get weather data."""
    city = request.json.get("city")

    if not city:
        return jsonify({"error": "City name is required"}), 400

    print(f"\n[{datetime.now()}] Running ETL Pipeline for {city}...\n")

    raw_data = get_weather_data(city)
    cleaned_data = transform(raw_data)

    if cleaned_data:
        load(cleaned_data)
        return jsonify(cleaned_data), 200
    else:
        return jsonify({"error": "Failed to fetch weather data"}), 500

if __name__ == "__main__":
    app.run(debug=True)
