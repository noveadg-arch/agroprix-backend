FROM python:3.11-slim
WORKDIR /app
# System libs required by reportlab (PDF generation) and psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    libfreetype6-dev \
    libfontconfig1 \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p data
EXPOSE 8000
CMD ["python", "run.py"]
