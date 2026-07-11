FROM python:3.12-slim

WORKDIR /app

# Install deps first for better layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY mexc_ema200_scanner.py mexc_ema200_dashboard.py ./

# The server reads $PORT (set by most hosts). Default 8000 for local `docker run`.
ENV PORT=8000
ENV SCAN_EVERY=10
EXPOSE 8000

CMD ["python", "mexc_ema200_dashboard.py"]
