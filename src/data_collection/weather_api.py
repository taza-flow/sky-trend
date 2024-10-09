import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("WEATHER_API_KEY")
BASE_URL = "http://apis.data.go.kr/1360000/"

def get_weather_observation(location, start_date, end_date):
    url = f"{BASE_URL}AsosDalyInfoService/getWthrDataList"
    params = {
        "serviceKey": API_KEY,
        "numOfRows": "999",
        "dataType": "JSON",
        "dataCd": "ASOS",
        "dateCd": "DAY",
        "startDt": start_date,
        "endDt": end_date,
        "stnIds": location
    }
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an exception for bad status codes
    return response.json()

def get_all_weather_data(location, start_date, end_date):
    try:
        weather_obs = get_weather_observation(location, start_date, end_date)
        return {"observation": weather_obs}
    except requests.RequestException as e:
        logger.error(f"Error fetching weather data: {str(e)}")
        return None

if __name__ == "__main__":
    location = "108"
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y%m%d")
    end_date = datetime.now().strftime("%Y%m%d")
    
    data = get_all_weather_data(location, start_date, end_date)
    if data:
        print(data)
    else:
        print("Failed to fetch weather data")