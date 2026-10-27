# MercuryMail 后端镜像（宪法 v14.0）：Python 3.11 + FastAPI，容器内监听 8000
ARG APP_GIT_SHA=unknown
ARG APP_IMAGE_DIGEST=local-build

FROM node:20-alpine AS frontend-builder
WORKDIR /frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.11-slim AS backend-builder
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
WORKDIR /build
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY backend/requirements.txt ./requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.11-slim AS backend-runtime
ARG APP_GIT_SHA=unknown
ARG APP_IMAGE_DIGEST=local-build
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV APP_GIT_SHA=${APP_GIT_SHA}
ENV APP_IMAGE_DIGEST=${APP_IMAGE_DIGEST}
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=backend-builder /install /usr/local
COPY backend/ ./
COPY --from=frontend-builder /frontend/dist /app/frontend/dist
RUN mkdir -p /app/data
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=5s --retries=10 --start-period=180s \
    CMD curl -f -sS http://127.0.0.1:8000/api/health/live || exit 1
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
