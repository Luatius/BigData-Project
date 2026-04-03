FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY scripts/ ./scripts/

ENV PYTHONPATH=/app

CMD ["sh", "-c", "python scripts/run_scraper.py && python scripts/fetch_ubahn_stations.py"]
