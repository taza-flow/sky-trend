# sky_trend/visualizer.py

import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery
import pandas as pd
from sklearn.metrics import mean_squared_error, r2_score

def create_summary_visualizations(eda_results):
    """
    EDA 결과를 바탕으로 요약 시각화를 생성합니다.
    """
    # 월별 평균 판매량 그래프
    plt.figure(figsize=(12, 6))
    sns.barplot(x='month', y='avg_sales', data=eda_results['monthly_sales'])
    plt.title('Average Monthly Sales')
    plt.xlabel('Month')
    plt.ylabel('Average Sales')
    plt.savefig('summary_monthly_sales.png')
    plt.close()

    # 온도와 판매량의 산점도
    plt.figure(figsize=(12, 6))
    sns.scatterplot(x='temperature', y='sales', data=eda_results['top_sales_days'])
    plt.title('Temperature vs Sales (Top Sales Days)')
    plt.xlabel('Temperature')
    plt.ylabel('Sales')
    plt.savefig('summary_temp_vs_sales.png')
    plt.close()

def visualize_model_comparison(bq_model_results, sklearn_model_results):
    """
    BigQuery ML 모델과 scikit-learn 모델의 성능을 비교 시각화합니다.
    """
    models = ['BigQuery ML', 'scikit-learn']
    mse = [bq_model_results['mean_squared_error'], sklearn_model_results['mean_squared_error']]
    r2 = [bq_model_results['r2_score'], sklearn_model_results['r2_score']]

    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1)
    plt.bar(models, mse)
    plt.title('Mean Squared Error Comparison')
    plt.ylabel('MSE')

    plt.subplot(1, 2, 2)
    plt.bar(models, r2)
    plt.title('R-squared Score Comparison')
    plt.ylabel('R-squared')

    plt.tight_layout()
    plt.savefig('model_comparison.png')
    plt.close()

def generate_report(eda_results, bq_model_results, sklearn_model_results):
    """
    분석 결과와 모델 성능을 요약한 보고서를 생성합니다.
    """
    report = f"""
    # Sky-Trend Project Report

    ## Exploratory Data Analysis Summary
    - Monthly average sales range from {eda_results['monthly_sales']['avg_sales'].min():.2f} to {eda_results['monthly_sales']['avg_sales'].max():.2f}
    - Temperature and sales correlation: {eda_results['temp_sales_correlation']['temp_sales_corr'].iloc[0]:.2f}
    - Highest sales day: {eda_results['top_sales_days']['date'].iloc[0]} with {eda_results['top_sales_days']['sales'].iloc[0]} sales

    ## Model Performance Comparison
    ### BigQuery ML Linear Regression Model
    - Mean Squared Error: {bq_model_results['mean_squared_error']:.2f}
    - R-squared Score: {bq_model_results['r2_score']:.2f}

    ### scikit-learn Random Forest Model
    - Mean Squared Error: {sklearn_model_results['mean_squared_error']:.2f}
    - R-squared Score: {sklearn_model_results['r2_score']:.2f}

    ## Conclusion
    [Add your conclusion and insights here]

    ## Next Steps
    [Add recommended next steps or areas for further investigation]
    """

    with open('sky_trend_report.md', 'w') as f:
        f.write(report)

def main(client, eda_results, bq_model_name, sklearn_model):
    # BigQuery ML 모델 평가 결과 가져오기
    query = f"""
    SELECT
        *
    FROM
        ML.EVALUATE(MODEL `{bq_model_name}`)
    """
    bq_eval_results = client.query(query).to_dataframe()

    # scikit-learn 모델 평가를 위한 테스트 데이터 가져오기
    test_data_query =