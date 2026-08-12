FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py state.py generator.py ./
COPY templates ./templates
COPY schemas ./schemas
EXPOSE 5001
# --workers 1 is load-bearing, not a resource-saving default: the generator
# thread starts once at app import time, so more workers would each start
# their own generator and double-produce every event. --timeout 120 for the
# same idle-keepalive reason as the Catalog Admin frontend (see its Dockerfile).
CMD ["gunicorn", "--bind", "0.0.0.0:5001", "--workers", "1", "--timeout", "120", "app:app"]
