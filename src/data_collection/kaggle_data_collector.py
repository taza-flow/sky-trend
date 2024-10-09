# sky_trend/data_collection.py

import requests
from kaggle.api.kaggle_api_extended import KaggleApi
from google.cloud import bigquery

def collect_weather_data(api_key, location, start_date, end_date):
    """
    기상청 API를 사용하여 날씨 데이터를 수집합니다.
    """
    base_url = "http://api.openweathermap.org/data/2.5/weather"
    params = {
        "q": location,
        "appid": api_key,
        "units": "metric"
    }
    
    weather_data = []
    current_date = start_date
    while current_date <= end_date:
        params["dt"] = int(current_date.timestamp())
        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            data = response.json()
            weather_data.append({
                "date": current_date.strftime("%Y-%m-%d"),
                "temperature": data["main"]["temp"],
                "precipitation": data["rain"]["1h"] if "rain" in data else 0
            })
        current_date += timedelta(days=1)
    
    return weather_data

def download_sales_data(dataset_name, path):
    """
    Kaggle API를 사용하여 판매 데이터셋을 다운로드합니다.
    """
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(dataset_name, path=path, unzip=True)
    
    # CSV 파일을 읽어 딕셔너리 리스트로 변환
    sales_data = []
    with open(f"{path}/sales_data.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sales_data.append({
                "date": row["date"],
                "sales": int(row["sales"])
            })
    
    return sales_data

def load_data_to_bigquery(client, table_id, data):
    """
    수집된 데이터를 BigQuery 테이블에 적재합니다.
    """
    errors = client.insert_rows_json(table_id, data)
    if errors == []:
        print("Data loaded successfully.")
    else:
        print(f"Errors occurred: {errors}")

def main(client, table_id, weather_api_key, location, start_date, end_date, kaggle_dataset):
    # 날씨 데이터 수집
    weather_data = collect_weather_data(weather_api_key, location, start_date, end_date)
    
    # 판매 데이터 다운로드
    sales_data = download_sales_data(kaggle_dataset, "data")
    
    # 데이터 병합
    merged_data = []
    for weather, sales in zip(weather_data, sales_data):
        merged_data.append({**weather, **sales})
    
    # BigQuery에 데이터 적재
    load_data_to_bigquery(client, table_id, merged_data)

if __name__ == "__main__":
    # 테스트를 위한 설정
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(
        'path/to/your/service_account.json',
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_id = "your-project.sky_trend_dataset.weather_sales"
    
    main(client, table_id, "your_weather_api_key", "Seoul", date(2023, 1, 1), date(2023, 12, 31), "kaggle/sales_dataset")