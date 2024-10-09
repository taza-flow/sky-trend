# sky_trend/analyzer.py

from google.cloud import bigquery
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def run_bigquery_analysis(client, table_id):
    """
    BigQuery를 사용하여 데이터 분석을 수행합니다.
    """
    queries = {
        "monthly_sales": f"""
            SELECT 
                EXTRACT(MONTH FROM date) AS month,
                AVG(sales) AS avg_sales
            FROM `{table_id}`
            GROUP BY month
            ORDER BY month
        """,
        "temp_sales_correlation": f"""
            SELECT 
                CORR(temperature, sales) AS temp_sales_corr
            FROM `{table_id}`
        """,
        "top_sales_days": f"""
            SELECT 
                date, temperature, precipitation, sales
            FROM `{table_id}`
            ORDER BY sales DESC
            LIMIT 10
        """
    }
    
    results = {}
    for name, query in queries.items():
        query_job = client.query(query)
        results[name] = query_job.to_dataframe()
    
    return results

def visualize_results(results):
    """
    분석 결과를 시각화합니다.
    """
    # 월별 평균 판매량 그래프
    plt.figure(figsize=(10, 6))
    sns.barplot(x='month', y='avg_sales', data=results['monthly_sales'])
    plt.title('Average Monthly Sales')
    plt.xlabel('Month')
    plt.ylabel('Average Sales')
    plt.savefig('monthly_sales.png')
    plt.close()

    # 온도와 판매량의 상관관계
    corr = results['temp_sales_correlation']['temp_sales_corr'].iloc[0]
    plt.figure(figsize=(8, 4))
    plt.text(0.5, 0.5, f'Correlation: {corr:.2f}', ha='center', va='center', fontsize=20)
    plt.axis('off')
    plt.savefig('temp_sales_correlation.png')
    plt.close()

    # 최고 판매일의 날씨 조건
    plt.figure(figsize=(12, 6))
    sns.scatterplot(x='temperature', y='sales', size='precipitation', 
                    data=results['top_sales_days'], sizes=(20, 200))
    plt.title('Top Sales Days: Temperature vs Sales')
    plt.xlabel('Temperature')
    plt.ylabel('Sales')
    plt.savefig('top_sales_days.png')
    plt.close()

def perform_eda(client, table_id):
    """
    EDA를 수행하고 결과를 반환합니다.
    """
    results = run_bigquery_analysis(client, table_id)
    visualize_results(results)
    return results

def main(client, table_id):
    results = perform_eda(client, table_id)
    print("EDA completed. Visualization images saved.")
    return results

if __name__ == "__main__":
    # 테스트를 위한 설정
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(
        'path/to/your/service_account.json',
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    table_id = "your-project.sky_trend_dataset.processed_weather_sales"
    
    main(client, table_id)