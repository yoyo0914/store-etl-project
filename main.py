import io
import requests
import json
import csv
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from flask import Flask, request

app = Flask(__name__)

today = datetime.now().strftime('%Y-%m-%d')

PROJECT_ID = "bigquerydemo-457512"  
BUCKET_NAME = f"{PROJECT_ID}-store-data"

def extract_data():
    """從 Fake Store API 抓取資料"""
    url = "https://fakestoreapi.com/products"
    response = requests.get(url)
    
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"無法從 API 抓取資料: {response.status_code}")

def transform_data(products):
    """根據部門需求轉換資料"""
    sales_data = [{
        "title": p["title"],
        "price": p["price"],
        "category": p["category"]
    } for p in products]
    
    product_data = [{
        "title": p["title"],
        "description": p["description"],
        "image": p["image"]
    } for p in products]
    
    finance_data = [{
        "id": p["id"],
        "price": p["price"],
        "rating": p["rating"]["rate"] if "rating" in p and "rate" in p["rating"] else None
    } for p in products]
    
    return {
        "sales": sales_data,
        "product": product_data,
        "finance": finance_data
    }

def load_to_gcs(data):
    """將資料上傳到 Google Cloud Storage"""
    client = storage.Client()
    
    try:
        bucket = client.get_bucket(BUCKET_NAME)
    except Exception:
        bucket = client.create_bucket(BUCKET_NAME, location="asia-east1")
        print(f"已建立儲存桶: {BUCKET_NAME}")
    
    for dept, dept_data in data.items():
        blob_name = f"{dept}_products_{today}.json"
        blob = bucket.blob(blob_name)
        
        blob.upload_from_string(
            json.dumps(dept_data, indent=2),
            content_type='application/json'
        )
        
        print(f"已上傳資料到 GCS: {blob_name}")

def load_to_bigquery(data):
    """將資料載入到 BigQuery"""
    client = bigquery.Client()
    dataset_id = f"{PROJECT_ID}.store_data"
    
    try:
        client.get_dataset(dataset_id)
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "asia-east1"
        client.create_dataset(dataset)
        print(f"已建立資料集: {dataset_id}")
    
    for dept, dept_data in data.items():
        table_id = f"{dataset_id}.{dept}_products"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        
        ndjson_data = "\n".join(json.dumps(item) for item in dept_data)
        
        job = client.load_table_from_file(
            file_obj=io.StringIO(ndjson_data),
            destination=table_id,
            job_config=job_config
        )
        
        job.result()
        print(f"已載入資料到 BigQuery: {table_id}")

def main():
    try:
        print("開始 ETL 流程...")
        
        print("從 Fake Store API 抓取資料...")
        products = extract_data()
        print(f"已抓取 {len(products)} 筆產品資料")
        
        print("轉換資料...")
        transformed_data = transform_data(products)
        
        print("載入資料到 Google Cloud Storage...")
        load_to_gcs(transformed_data)
        
        print("載入資料到 BigQuery...")
        load_to_bigquery(transformed_data)
        
        print("ETL 流程完成！")
        return True
    
    except Exception as e:
        print(f"ETL 流程發生錯誤: {e}")
        raise e

@app.route('/', methods=['GET', 'POST'])
def run_etl_http():
    """Cloud Run HTTP 入口點"""
    try:
        main()
        return "ETL 流程成功完成！", 200
    except Exception as e:
        return f"ETL 流程失敗: {str(e)}", 500

if __name__ == "__main__":
    import os
    if os.environ.get('PORT'):
        port = int(os.environ.get('PORT', 8080))
        app.run(host='0.0.0.0', port=port)
    else:
        main()
