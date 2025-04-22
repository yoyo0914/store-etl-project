# 商店資料 ETL 專案

此專案從 Fake Store API 獲取產品資料，根據不同部門需求進行轉換，並將資料存儲到 Google Cloud Platform。

## 功能

- 每天自動從 Fake Store API 抓取資料
- 根據部門需求轉換成不同的資料視角
- 儲存於 GCP（BigQuery 和 Cloud Storage）
- 結合 CI/CD，自動部署 ETL 程式碼

## 部門資料需求

- **銷售部**: title, price, category
- **產品部**: title, description, image
- **財務部**: id, price, rating

## 技術堆疊

- Python
- Google Cloud Platform (BigQuery, Cloud Storage)
- Cloud Functions/Cloud Run
- Cloud Scheduler
- CI/CD (GitHub Actions/Cloud Build)