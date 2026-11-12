FROM python:3.11-slim-bookworm

WORKDIR /app

COPY qualitydb/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 校验规则与采集端使用同一份文件（构建时从 collector 复制），避免规则漂移。
COPY collector/app/validation.py ./validation.py
COPY qualitydb/receiver.py ./receiver.py
COPY qualitydb/app.py ./app.py
COPY qualitydb/templates ./templates

ENV QUALITYDB_DB=/data/quality.db \
    QUALITYDB_DROP_AFTER_PERSIST=0 \
    PYTHONUNBUFFERED=1

EXPOSE 8100
CMD ["python", "app.py"]
