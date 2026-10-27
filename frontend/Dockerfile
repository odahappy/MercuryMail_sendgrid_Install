# MercuryMail 前端镜像（宪法 v11.1）：Node 22 构建 + Nginx 托管
# build context 为项目根目录（docker-compose.yml 中 context: .），路径均相对仓库根
# 上游服务名 mercury-mail-app 须与 docker-compose.yml 中后端 services 键名一致
FROM node:22-alpine AS frontend-builder
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ .
RUN npm run build \
    && test -f dist/index.html \
    && grep -q '/assets/' dist/index.html

FROM nginx:alpine AS frontend-runtime
# 移除镜像自带默认站点，避免与自定义配置冲突
RUN rm -f /etc/nginx/conf.d/default.conf
COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=frontend-builder /app/dist /usr/share/nginx/html
# 使用变量 proxy_pass + Docker DNS，构建阶段 nginx -t 不会解析 mercury-mail-app
RUN nginx -t \
    && test -f /usr/share/nginx/html/index.html \
    && grep -q '/assets/' /usr/share/nginx/html/index.html
EXPOSE 80
HEALTHCHECK --interval=15s --timeout=5s --retries=5 --start-period=10s \
    CMD wget -qO- http://127.0.0.1/ >/dev/null 2>&1 || exit 1
CMD ["nginx", "-g", "daemon off;"]
