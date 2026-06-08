FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PORT=8050

WORKDIR /app

RUN python -m pip install --upgrade pip

COPY requirements.txt .
RUN python -m pip install --no-cache-dir \
    "dash>=2.17.0" \
    "dash-bootstrap-components>=1.6.0" \
    "pymongo>=4.6.0" \
    "gunicorn>=22.0.0" \
    "websockets>=12.0.0" \
    "pandas>=2.1.0" \
    "plotly>=5.20.0"

COPY app.py .
COPY src ./src
COPY data/processed/cleaned ./data/processed/cleaned

EXPOSE 8050

CMD ["gunicorn", "--bind", "0.0.0.0:8050", "--workers", "2", "--timeout", "60", "app:server"]

