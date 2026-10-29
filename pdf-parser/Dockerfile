ARG PDF_PARSER_BASE_IMAGE=docker.1ms.run/nvidia/cuda:12.9.1-cudnn-runtime-ubuntu22.04
FROM ${PDF_PARSER_BASE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

ARG UBUNTU_MIRROR=https://archive.ubuntu.com/ubuntu
RUN sed -i "s|http://archive.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g; s|http://security.ubuntu.com/ubuntu|${UBUNTU_MIRROR}|g" /etc/apt/sources.list \
    && apt-get -o Acquire::Retries=3 update \
    && apt-get install -y --no-install-recommends python3 python3-pip libglib2.0-0 libgl1 libgomp1 curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pdf-parser/requirements.txt /tmp/pdf-parser-requirements.txt
RUN python3 -m pip install --no-cache-dir --timeout 120 --retries 5 paddlepaddle-gpu==3.2.2 --index-url https://www.paddlepaddle.org.cn/packages/stable/cu129/
RUN python3 -m pip install --no-cache-dir --timeout 120 --retries 5 -r /tmp/pdf-parser-requirements.txt
COPY backend /app/backend

EXPOSE 8090
CMD ["python3", "-m", "uvicorn", "backend.pdf_parser.app:app", "--host", "0.0.0.0", "--port", "8090"]
