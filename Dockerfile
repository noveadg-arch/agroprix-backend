FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 10000

# Start script: import CSV if DB doesn't exist, then start uvicorn
CMD bash -c "if [ ! -f data/agroprix.db ]; then python import_csv.py; fi && uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-10000}"
