FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV TELEGRAM_BOT_TOKEN="7762767790:AAGHDLDc2pn-j68x0Ni4X1mYfCHlq6sSPbU"

CMD ["python", "main.py"]