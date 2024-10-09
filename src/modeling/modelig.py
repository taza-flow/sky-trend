# sky_trend/modeling.py

from google.cloud import bigquery
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import pandas as pd
import numpy as np

def train_bigquery_ml_model(client, dataset_id, table_id):
    """
    BigQuery ML을 사용하여 선형 회귀 모델을 훈련합니다.
    """
    model_name = f"{dataset_id}.sales_prediction_model"
    
    # 모델 생성 및 훈련
    query = f"""
    CREATE OR REPLACE MODEL `{model_name}`
    OPTIONS(model_type='linear_reg', input_label_cols=['sales']) AS
    SELECT
        temperature, precipitation, avg_temp_7days, total_precip_7days,
        sales_7days_ago, day_of_week, month, sales
    FROM
        `{table_id}`
    """
    job = client.query(query)
    job.result()  # 모델 훈련 완료까지 대기
    
    # 모델 평가
    eval_query = f"""
    SELECT
        *
    FROM
        ML.EVALUATE(MODEL `{model_name}`)
    """
    eval_job = client.query(eval_query)
    eval_results = eval_job.to_dataframe()
    
    print("BigQuery ML Model Evaluation Results:")
    print(eval_results)
    
    return model_name

def train_sklearn_model(client, table_id):
    """
    scikit-learn을 사용하여 랜덤 포레스트 모델을 훈련합니다.
    """
    # 데이터 로드
    query = f"""
    SELECT
        temperature, precipitation, avg_temp_7days, total_precip_7days,
        sales_7days_ago, day_of_week, month, sales
    FROM
        `{table_id}`
    """
    df = client.query(query).to_dataframe()
    
    # 특성과 타겟 분리
    X = df.drop('sales', axis=1)
    y = df['sales']
    
    # 훈련/테스트 세트 분리
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 모델 훈련
    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)
    
    # 모델 평가
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print("Random Forest Model Evaluation Results:")
    print(f"Mean Squared Error: {mse}")
    print(f"R-squared Score: {r2}")
    
    return model

def main(client, dataset_id, table_id):
    bq_model = train_bigquery_ml_model(client, dataset_id, table_id)
    sklearn_model = train_sklearn_model(client, table_id)
    return bq_model, sklearn_model

if __name__ == "__main__":
    # 테스트를 위한 설정
    from google.oauth2 import service_account
    credentials = service_account.Credentials.from_service_account_file(
        'path/to/your/service_account.json',
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    dataset_id = "your-project.sky_trend_dataset"
    table_id = f"{dataset_id}.processed_weather_sales"
    
    main(client, dataset_id, table_id)