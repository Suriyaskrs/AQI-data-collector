import os
import requests
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)

class AQIDataCollector:
    def __init__(self):
        self.api_key = os.getenv('OPENWEATHER_API_KEY')
        self.base_url = "http://api.openweathermap.org/data/2.5/air_pollution"
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        self.csv_filename = self.data_dir / "Air_Quality_Data.csv"
        self.data = []
        
    def get_air_quality_data(self, lat, lon):
        params = {
            'lat': lat,
            'lon': lon,
            'appid': self.api_key
        }
        
        try:
            response = requests.get(f"{self.base_url}", params=params)
            response.raise_for_status()
            logging.info(f"Successfully fetched data for coordinates: {lat}, {lon}")
            return response.json()
        except requests.RequestException as e:
            logging.error(f"Error fetching data: {e}")
            return None
    
    def collect_data_for_multiple_locations(self, locations):
        try:
            existing_data = pd.DataFrame()
            if self.csv_filename.exists():
                existing_data = pd.read_csv(self.csv_filename)
                existing_data['timestamp'] = pd.to_datetime(existing_data['timestamp'])

            for location in locations:
                data = self.get_air_quality_data(location['lat'], location['lon'])
                if data:
                    self.process_and_store_data(data)
                    logging.info(f"Processing data for {location['name']}")

            new_data = pd.DataFrame(self.data)
            
            if not new_data.empty:
                if not existing_data.empty:
                    combined_data = pd.concat([existing_data, new_data], ignore_index=True)
                    combined_data = combined_data.drop_duplicates(subset=['timestamp', 'lat', 'lon'], keep='last')
                    combined_data = combined_data.sort_values('timestamp')
                else:
                    combined_data = new_data

                self.data_dir.mkdir(exist_ok=True)
                combined_data.to_csv(self.csv_filename, index=False)
                logging.info(f"Data has been saved to {self.csv_filename}")
            
            self.data = []
            
        except Exception as e:
            logging.error(f"Error in data collection: {e}")
            raise

    def process_and_store_data(self, data):
        if not data or 'list' not in data:
            return
        
        for item in data['list']:
            components = item['components']
            timestamp = datetime.fromtimestamp(item['dt'])
            
            record = {
                'timestamp': timestamp,
                'lat': data['coord']['lat'],
                'lon': data['coord']['lon'],
                'aqi': item['main']['aqi'],
                'co': components.get('co'),
                'no': components.get('no'),
                'no2': components.get('no2'),
                'o3': components.get('o3'),
                'so2': components.get('so2'),
                'pm2_5': components.get('pm2_5'),
                'pm10': components.get('pm10'),
                'nh3': components.get('nh3')
            }
            self.data.append(record)

def main():
    collector = AQIDataCollector()
    
    locations = [
        {'name': 'Mumbai', 'lat': 19.0760, 'lon': 72.8777},
        {'name': 'Delhi', 'lat': 28.6139, 'lon': 77.2090},
        {'name': 'Kolkata', 'lat': 22.5726, 'lon': 88.3639},
        {'name': 'Chennai', 'lat': 13.0827, 'lon': 80.2707}
    ]
    
    collector.collect_data_for_multiple_locations(locations)

if __name__ == "__main__":
    main()
