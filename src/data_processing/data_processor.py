# sky_trend/data_processing.py

from google.cloud import bigquery

def create_processed_table(client, dataset_id, source_table_id, processed_table_id):
    """
    처리된 데이터를 저장할 새 테이블을 생성합니다.
    """
    processed_table = bigquery.Table(processed_table_id)
    processed_table = client.create_table(processed_table, exists_ok=True)
    print(f"Created table {processed_table_id}")

def process_data(client, dataset_id, source_table_id, processed_table_id):
    """
    BigQuery 내에서 데이터를 처리하고 결과를 새 테이블에 저장합니다.
    """
    query = f"""
    CREATE OR REPLACE TABLE `{processed_table_id}` AS
    SELECT
        date,
        temperature,
        precipitation,
        sales,
        AVG(temperature) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS avg_temp_7days,
        SUM(precipitation) OVER (ORDER BY date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS total_precip_7days,
        LAG(sales, 7) OVER (ORDER BY date) AS sales_7days_ago,
        EXTRACT(DAYOFWEEK FROM date) AS day_of_week,
        EXTRACT(MONTH FROM date) AS month
    FROM
        `{source_table_id}`
    WHERE
        temperature BETWEEN -50 AND 50  -- 이상치 제거
        AND precipitation >= 0
        AND sales >= 0
    ORDER BY
        date
    """
    
    job_config = bigquery.QueryJobConfig(destination=processed_table_id)
    query_job = client.query(query, job_config=job_config)
    query_job.result()  # 쿼리 완료까지 대기
    
    print(f"Data processed and saved to {processed_table_id}")

def validate_processed_data(client, processed_table_id):
    """
    처리된 데이터의 유효성을 검사합니다.
    """
    query = f"""
    SELECT
        COUNT(*) AS total_rows,
        COUNT(DISTINCT date) AS unique_dates,
        AVG(temperature) AS avg_temperature,
        MAX(precipitation) AS max_precipitation,
        AVG(sales) AS avg_sales
    FROM
        `{processed_table_id}`
    """
    
    query_job = client.query(query)
    results = query_job.result()
    
    for row in results:
        print(f"Total rows: {row.total_rows}")
        print(f"Unique dates: {row.unique_dates}")
        print(f"Average temperature: {row.avg_temperature:.2f}")
        print(f"Maximum precipitation: {row.max_precipitation:.2f}")
        print(f"Average sales: {row.avg_sales:.2f}")

def main(client, dataset_id, source_table_id):
    processed_table_id = f"{dataset_id}.processed_weather_sales"
    
    create_processed_table(client, dataset_id, source_table_id, processed_table_id)
    process_data(client, dataset_id, source_table_id, processed_table_id)
    validate_processed_data(client, processed_table_id)

if __name__ == "__main__":
    # 테스트를 위한 설정
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(
        'path/to/your/service_account.json',
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    dataset_id = "your-project.sky_trend_dataset"
    source_table_id = f"{dataset_id}.weather_sales"
    
    main(client, dataset_id, source_table_id)