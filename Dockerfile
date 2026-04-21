FROM python:3.12-slim AS builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libaio1t64 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    libaio1t64 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

COPY . ./

RUN useradd -m -u 1000 django
RUN chown -R django:django /app

USER django

EXPOSE 8022

CMD ["gunicorn", \
     "--bind", "0.0.0.0:8022", \
     "--workers", "2", \
     "--threads", "2", \
     "--timeout", "120", \
     "config.wsgi:application"]