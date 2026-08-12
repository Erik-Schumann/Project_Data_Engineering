# Plain Python - this job is a periodic SQL query against reporting-db
# (see README.md).
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY job.py .
CMD ["python", "job.py"]
