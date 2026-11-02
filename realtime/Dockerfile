FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir 'akshare==1.20.21' 'kafka-python==2.2.15' 'psycopg[binary]>=3.2,<4'
COPY realtime /app/realtime
ENV PYTHONUNBUFFERED=1
