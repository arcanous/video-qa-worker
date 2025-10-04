FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg tesseract-ocr && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY worker ./worker
ENV PYTHONUNBUFFERED=1

# --- traceability (OCI labels) ---
ARG VERSION=0.0.0
ARG VCS_REF=dev
LABEL org.opencontainers.image.title="videoqa-worker" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}"

CMD ["python","-m","worker.run"]