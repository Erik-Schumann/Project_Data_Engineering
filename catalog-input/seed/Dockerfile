FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY seed_catalog.py .
COPY seeds ./seeds
ENTRYPOINT ["python", "seed_catalog.py"]
