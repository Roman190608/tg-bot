FROM python:3.11-slim

# ffmpeg
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bot.py .
COPY menu_light.png menu_dark.png welcome.gif ./

# Создаём директорию для загрузок
RUN mkdir -p /app/downloads

ENV DATA_DIR=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "bot.py"]
