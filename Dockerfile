FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 使用 gunicorn 啟動 Flask 應用
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
