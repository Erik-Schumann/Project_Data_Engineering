# Build context is ../  (catalog-input/), not this directory — needed so
# this image can also pull in seed_catalog.py + seeds/ from ../seed, letting
# the frontend trigger the same seeding logic as the CLI seeder without a
# second copy of the parsing code. See docker-compose.yml's catalog-frontend
# service (context: ./catalog-input, dockerfile: frontend/Dockerfile).
FROM python:3.12-slim
WORKDIR /app
COPY frontend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY frontend/app.py frontend/db.py ./
COPY frontend/templates ./templates
COPY seed/seed_catalog.py ./seed_catalog.py
COPY seed/seeds ./seeds
EXPOSE 5000
# --timeout 120: gunicorn's sync worker class treats an idle keep-alive
# connection (e.g. a browser tab just sitting open between clicks) the same
# as a stuck request and kills the worker at the default 30s, which the
# browser sees as a connection reset / "internal server error." This is a
# low-traffic single-admin internal tool, so a generous timeout has no real
# downside — the DB queries themselves take milliseconds.
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "2", "--timeout", "120", "app:app"]
