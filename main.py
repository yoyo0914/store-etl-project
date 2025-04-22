import io
import requests
import json
import csv
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery

# 今天的日期
today = datetime.now().strftime('%Y-%m-%d')

# 設置您的 GCP 專案 ID 和儲存桶名稱
PROJECT_ID = "bigquerydemo-457512"  # 替換為您的實際專案 ID
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
    # 銷售部資料
    sales_data = [{
        "title": p["title"],
        "price": p["price"],
        "category": p["category"]
    } for p in products]
    
    # 產品部資料
    product_data = [{
        "title": p["title"],
        "description": p["description"],
        "image": p["image"]
    } for p in products]
    
    # 財務部資料
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
    bucket = client.bucket(BUCKET_NAME)
    
    for dept, dept_data in data.items():
        # 建立 JSON 檔案名稱
        blob_name = f"{dept}_products_{today}.json"
        blob = bucket.blob(blob_name)
        
        # 將資料上傳為 JSON
        blob.upload_from_string(
            json.dumps(dept_data, indent=2),
            content_type='application/json'
        )
        
        print(f"已上傳資料到 GCS: {blob_name}")

def load_to_bigquery(data):
    """將資料載入到 BigQuery"""
    client = bigquery.Client()
    dataset_id = f"{PROJECT_ID}.store_data"
    
    for dept, dept_data in data.items():
        # 建立表格 ID (如果表格已存在會被替換)
        table_id = f"{dataset_id}.{dept}_products"
        
        # 將資料載入到 BigQuery
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        
        # 轉換為 NDJSON 格式 (每行一個 JSON 物件)
        ndjson_data = "\n".join(json.dumps(item) for item in dept_data)
        
        # 執行載入作業
        job = client.load_table_from_file(
            file_obj=io.StringIO(ndjson_data),
            destination=table_id,
            job_config=job_config
        )
        
        # 等待作業完成
        job.result()
        print(f"已載入資料到 BigQuery: {table_id}")

def main():
    try:
        print("開始 ETL 流程...")
        
        # 1. 抓取資料
        print("從 Fake Store API 抓取資料...")
        products = extract_data()
        print(f"已抓取 {len(products)} 筆產品資料")
        
        # 2. 轉換資料
        print("轉換資料...")
        transformed_data = transform_data(products)
        
        # 3. 載入資料到 GCS
        print("載入資料到 Google Cloud Storage...")
        load_to_gcs(transformed_data)
        
        # 4. 載入資料到 BigQuery
        print("載入資料到 BigQuery...")
        load_to_bigquery(transformed_data)
        
        print("ETL 流程完成！")
        return True
    
    except Exception as e:
        print(f"ETL 流程發生錯誤: {e}")
        raise e

def run_etl(request=None):
    """Cloud Function 入口點"""
    try:
        main()
        return "ETL 流程成功完成！", 200
    except Exception as e:
        return f"ETL 流程失敗: {str(e)}", 500

if __name__ == "__main__":
    main()