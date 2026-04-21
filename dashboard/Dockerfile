FROM python:3.11-slim-bookworm

WORKDIR /app

# Установка системных зависимостей (необходимы для psycopg2 и некоторых C-расширений)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    g++ \
    libopenblas-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Установка Python-зависимостей
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Копирование исходного кода дашборда
COPY . /app/

# Директория для PDF-отчётов
RUN mkdir -p /app/data/reports

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]