FROM python:3.10-slim

WORKDIR /app

# Install libgomp1 for LightGBM support on Linux
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mml_hybrid_server.py .
COPY model_lgb_90d.joblib .
COPY model_meta.json .
COPY dashboard_template.html .

ENV PORT=8000
EXPOSE 8000

CMD ["python", "mml_hybrid_server.py"]
