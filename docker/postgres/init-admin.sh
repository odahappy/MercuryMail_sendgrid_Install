#!/usr/bin/env sh
set -eu

PG_ADMIN_USER="${PG_ADMIN_USER:-admin}"
PG_ADMIN_PASSWORD="${PG_ADMIN_PASSWORD:-}"
APP_USER="${POSTGRES_USER:-mercury}"
APP_PASSWORD="${POSTGRES_PASSWORD:-}"
APP_DB="${POSTGRES_DB:-mercurymail}"

if [ -z "$PG_ADMIN_PASSWORD" ] || [ -z "$APP_PASSWORD" ]; then
  echo "[MercuryMail][PostgreSQL] 数据库初始化密码缺失，跳过自定义账号初始化" >&2
  exit 0
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$APP_DB" \
  -v pg_admin_user="$PG_ADMIN_USER" \
  -v pg_admin_password="$PG_ADMIN_PASSWORD" \
  -v app_user="$APP_USER" \
  -v app_password="$APP_PASSWORD" \
  -v app_db="$APP_DB" <<'SQL'
SELECT format(
  'CREATE ROLE %I WITH LOGIN SUPERUSER PASSWORD %L',
  :'pg_admin_user',
  :'pg_admin_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'pg_admin_user')\gexec

SELECT format(
  'ALTER ROLE %I WITH LOGIN SUPERUSER PASSWORD %L',
  :'pg_admin_user',
  :'pg_admin_password'
)\gexec

SELECT format(
  'ALTER ROLE %I WITH LOGIN PASSWORD %L',
  :'app_user',
  :'app_password'
)\gexec

SELECT format(
  'GRANT ALL PRIVILEGES ON DATABASE %I TO %I',
  :'app_db',
  :'app_user'
)\gexec
SQL

echo "[MercuryMail][PostgreSQL] 管理用户与应用用户初始化完成"
