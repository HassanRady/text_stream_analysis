FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir pipenv

COPY Pipfile Pipfile.lock ./

RUN pipenv requirements > requirements.txt

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH" \
    PYTHONPATH="/app"

RUN groupadd -g 10001 appgroup && \
    useradd -m -u 10001 -g appgroup appuser

WORKDIR /app

COPY --from=builder /opt/venv /opt/venv

COPY ./src ./src
COPY ./migrations ./migrations

RUN chown -R appuser:appgroup /app
USER appuser

EXPOSE 8000
ENV PORT=8000

CMD ["sh", "-c", "uvicorn src.app:app --host 0.0.0.0 --port ${PORT} --proxy-headers --workers ${WEB_CONCURRENCY:-2}"]
