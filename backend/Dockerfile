FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/openfinance-requirements.txt
RUN pip install --no-cache-dir -r /tmp/openfinance-requirements.txt
RUN python -c "import pyarrow, pyiceberg, pyiceberg_core, pgvector, psycopg, s3fs"

COPY . /app

EXPOSE 8000

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
