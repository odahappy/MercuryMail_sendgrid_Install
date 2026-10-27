#!/usr/bin/env bash
# MercuryMail 一键部署脚本（阿里云 Ubuntu 22.04）
# 功能：环境准备、预部署备份、幂等 Docker 构建、冒烟测试、源码清理、密码状态提示
# 幂等部署：重复执行 bash deploy.sh 会自动清理孤儿容器/名称冲突并 force-recreate 重建
set -euo pipefail
# 修复本脚本 Windows CRLF 行尾（FileZilla 二进制上传场景）
sed -i 's/\r$//' "$0" 2>/dev/null || sed -i '' 's/\r$//' "$0" 2>/dev/null || true

fix_crlf_for_file() {
  TARGET="$1"
  if [ -f "$TARGET" ]; then
    sed -i 's/\r$//' "$TARGET" 2>/dev/null || sed -i '' 's/\r$//' "$TARGET" 2>/dev/null || true
  fi
}

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="${PROJECT_DIR}/docker-compose.yml"
ENV_FILE="${PROJECT_DIR}/.env"
DATA_DIR="${PROJECT_DIR}/data"
BACKUP_DIR="${PROJECT_DIR}/backups"
DOCKER_DAEMON_DIR="/etc/docker"
DOCKER_DAEMON_FILE="${DOCKER_DAEMON_DIR}/daemon.json"
APP_PORT="${APP_PORT:-8000}"
FRONTEND_PORT="${FRONTEND_PORT:-80}"
# 构建元数据从当前部署环境读取；未注入时使用明确的安全默认值。
APP_GIT_SHA="${APP_GIT_SHA:-unknown}"
APP_IMAGE_DIGEST="${APP_IMAGE_DIGEST:-unknown}"
export APP_GIT_SHA APP_IMAGE_DIGEST
# 与 docker-compose.yml 中后端 services 键名一致；deploy.sh 不修改 nginx.conf
BACKEND_COMPOSE_SERVICE="${BACKEND_COMPOSE_SERVICE:-mercury-mail-app}"
BACKEND_CONTAINER_PORT="${BACKEND_CONTAINER_PORT:-8000}"
USE_ALIYUN_APT="${USE_ALIYUN_APT:-0}"
DOCKER_MIRROR="${DOCKER_MIRROR:-}"
PUBLIC_BASE_URL_OVERRIDE="${PUBLIC_BASE_URL:-}"
ADMIN_PASSWORD=""
ADMIN_PASSWORD_AUTO_GENERATED=0
ADMIN_USERNAME="admin"
DEPLOY_TIMESTAMP=""
ENV_BACKUP=""
COMPOSE_BACKUP=""
DB_BACKUP=""
ADMIN_EXISTED_BEFORE=0

log() {
  printf '\n[MercuryMail] %s\n' "$1"
}

warn() {
  printf '[MercuryMail][警告] %s\n' "$1"
}

warn_red() {
  printf '\033[31m[MercuryMail][警告] %s\033[0m\n' "$1"
}

fix_project_crlf() {
  log "修复 Windows 上传导致的 CRLF 行尾"
  fix_crlf_for_file "$0"
  find "$PROJECT_DIR" -type f \( -name '*.py' -o -name '*.sh' -o -name '*.js' -o -name '*.jsx' -o -name '*.json' -o -name '*.yml' -o -name '*.yaml' -o -name '*.conf' -o -name '*.env*' -o -name 'Dockerfile' \) -print0 2>/dev/null | while IFS= read -r -d '' file; do
    fix_crlf_for_file "$file"
  done
}

require_root() {
  if [ "$(id -u)" -ne 0 ]; then
    echo "请使用 root 用户运行，或执行：sudo bash deploy.sh"
    exit 1
  fi
}

require_ubuntu_2204() {
  if [ -f /etc/os-release ]; then
    # shellcheck disable=SC1091
    . /etc/os-release
  fi
  if [ "${ID:-}" != "ubuntu" ]; then
    echo "当前系统不是 Ubuntu，目标部署环境为阿里云 Ubuntu 22.04。"
    exit 1
  fi
  if [ "${VERSION_ID:-}" != "22.04" ]; then
    echo "当前 Ubuntu 版本为 ${VERSION_ID:-unknown}，推荐使用 Ubuntu 22.04。"
  fi
}

detect_domestic_network() {
  if [ "$USE_ALIYUN_APT" = "1" ]; then
    return 0
  fi
  if curl -fsS --max-time 2 http://100.100.100.200/latest/meta-data/instance-id >/dev/null 2>&1; then
    USE_ALIYUN_APT=1
    log "检测到阿里云内网环境，已自动启用国内 apt 源"
  fi
}

replace_apt_sources_with_aliyun() {
  log "配置阿里云 apt 源"
  if [ -f /etc/os-release ]; then
    # shellcheck disable=SC1091
    . /etc/os-release
  fi
  UBUNTU_CODENAME="${VERSION_CODENAME:-jammy}"
  if [ -f /etc/apt/sources.list ]; then
    cp /etc/apt/sources.list "/etc/apt/sources.list.bak.$(date +%Y%m%d%H%M%S)"
  fi
  if [ -d /etc/apt/sources.list.d ]; then
    mkdir -p /etc/apt/sources.list.d/mercurymail-disabled
    find /etc/apt/sources.list.d -maxdepth 1 -type f \( -name '*.list' -o -name '*.sources' \) -exec mv {} /etc/apt/sources.list.d/mercurymail-disabled/ \;
  fi
  cat > /etc/apt/sources.list <<EOF
deb http://mirrors.aliyun.com/ubuntu/ ${UBUNTU_CODENAME} main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ ${UBUNTU_CODENAME}-security main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ ${UBUNTU_CODENAME}-updates main restricted universe multiverse
deb http://mirrors.aliyun.com/ubuntu/ ${UBUNTU_CODENAME}-backports main restricted universe multiverse
EOF
  apt-get update
}

prepare_apt() {
  if [ "$USE_ALIYUN_APT" = "1" ]; then
    replace_apt_sources_with_aliyun
  else
    log "更新 apt 软件包索引"
    apt-get update
  fi
}

install_base_packages() {
  log "安装基础依赖"
  DEBIAN_FRONTEND=noninteractive apt-get install -y ca-certificates curl gnupg lsb-release openssl coreutils iproute2 jq gzip
}

configure_system_nofile_limits() {
  log "配置系统级文件描述符上限"
  cat > /etc/security/limits.d/99-mercurymail-nofile.conf <<'EOF'
* soft nofile 65535
* hard nofile 65535
root soft nofile 65535
root hard nofile 65535
EOF
  mkdir -p /etc/systemd/system/docker.service.d
  cat > /etc/systemd/system/docker.service.d/99-mercurymail-nofile.conf <<'EOF'
[Service]
LimitNOFILE=65535
EOF
  systemctl daemon-reload || true
}

write_docker_daemon_config() {
  mkdir -p "$DOCKER_DAEMON_DIR"
  if [ -n "$DOCKER_MIRROR" ]; then
    REGISTRY_MIRRORS_JSON="[\"${DOCKER_MIRROR}\"]"
  else
    REGISTRY_MIRRORS_JSON='["https://registry.cn-hangzhou.aliyuncs.com","https://docker.m.daocloud.io","https://dockerproxy.com"]'
  fi
  cat > "$DOCKER_DAEMON_FILE" <<EOF
{
  "registry-mirrors": ${REGISTRY_MIRRORS_JSON},
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "20m",
    "max-file": "3"
  }
}
EOF
}

install_docker_with_aliyun() {
  log "安装并配置 Docker"
  if ! command -v docker >/dev/null 2>&1; then
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://mirrors.tuna.tsinghua.edu.cn/docker-ce/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg
    # shellcheck disable=SC1091
    . /etc/os-release
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://mirrors.tuna.tsinghua.edu.cn/docker-ce/linux/ubuntu ${VERSION_CODENAME} stable" > /etc/apt/sources.list.d/docker.list
    apt-get update
    DEBIAN_FRONTEND=noninteractive apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  fi
  configure_system_nofile_limits
  systemctl enable --now docker
  if ! docker compose version >/dev/null 2>&1; then
    DEBIAN_FRONTEND=noninteractive apt-get install -y docker-compose-plugin
  fi
}

generate_secret() {
  openssl rand -base64 48 | tr -d '\n='
}

generate_password() {
  openssl rand -base64 24 | tr -dc 'A-Za-z0-9' | head -c 16
}

generate_admin_password() {
  openssl rand -base64 12 2>/dev/null | tr -dc 'A-Za-z0-9' | head -c 12
}

generate_random_32() {
  openssl rand -hex 16 2>/dev/null | tr -d '\n' | head -c 32
}

generate_db_password() {
  openssl rand -base64 24 2>/dev/null | tr -dc 'A-Za-z0-9' | head -c 24
}

update_env_line() {
  KEY="$1"
  VALUE="$2"
  if [ ! -f "$ENV_FILE" ]; then
    return 1
  fi
  if grep -q "^${KEY}=" "$ENV_FILE" 2>/dev/null; then
    sed -i "s|^${KEY}=.*|${KEY}=${VALUE}|" "$ENV_FILE" || return 1
  else
    printf '\n%s=%s\n' "$KEY" "$VALUE" >> "$ENV_FILE" || return 1
  fi
}

sync_compose_postgres_password() {
  _PASS="$1"
  # 宪法 v13.1：docker-compose.yml 保持模板化，不再由部署脚本改写。
  # PostgreSQL 密码仅写入 .env，并通过 compose 的 ${POSTGRES_PASSWORD:-...} 注入容器。
  return 0
}

PGDATA_DIR="${PROJECT_DIR}/pgdata"
DB_CONTAINER_NAME="mercury-mail-db"
DB_ROLE_NAME="mercury"
DB_NAME="mercurymail"
PG_ADMIN_USER_DEFAULT="admin"

extract_password_from_database_url() {
  URL="$1"
  if parse_database_url_credentials "$URL"; then
    printf '%s' "$PARSED_DB_PASS"
    return 0
  fi
  printf '%s' "$URL" | sed -n 's|^postgresql+asyncpg://mercury:\([^@]*\)@.*|\1|p'
}

# 从 DATABASE_URL 解析 postgresql(+asyncpg)://用户:密码@主机:端口/库名
parse_database_url_credentials() {
  URL="$(printf '%s' "$1" | tr -d '\r' | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  PARSED_DB_USER=""
  PARSED_DB_PASS=""
  PARSED_DB_NAME=""
  if [ -z "$URL" ]; then
    return 1
  fi
  if ! printf '%s' "$URL" | grep -qE '^postgresql(\+asyncpg)?://'; then
    return 1
  fi
  PARSED_DB_USER="$(printf '%s' "$URL" | sed -n 's|^postgresql+asyncpg://\([^:]*\):.*|\1|p')"
  PARSED_DB_PASS="$(printf '%s' "$URL" | sed -n 's|^postgresql+asyncpg://[^:]*:\([^@]*\)@.*|\1|p')"
  PARSED_DB_NAME="$(printf '%s' "$URL" | sed -n 's|^postgresql+asyncpg://[^@]*@[^/]*/\([^/?]*\).*|\1|p')"
  if [ -z "$PARSED_DB_USER" ]; then
    PARSED_DB_USER="$(printf '%s' "$URL" | sed -n 's|^postgresql://\([^:]*\):.*|\1|p')"
    PARSED_DB_PASS="$(printf '%s' "$URL" | sed -n 's|^postgresql://[^:]*:\([^@]*\)@.*|\1|p')"
    PARSED_DB_NAME="$(printf '%s' "$URL" | sed -n 's|^postgresql://[^@]*@[^/]*/\([^/?]*\).*|\1|p')"
  fi
  if [ -z "$PARSED_DB_USER" ] || [ -z "$PARSED_DB_PASS" ] || [ -z "$PARSED_DB_NAME" ]; then
    return 1
  fi
  return 0
}

extract_password_from_env_file() {
  FILE="$1"
  DB_URL=""
  POSTGRES_PASS=""
  PASS=""
  if [ ! -f "$FILE" ]; then
    return 1
  fi
  DB_URL="$(grep -E '^DATABASE_URL=' "$FILE" 2>/dev/null | tail -n 1 | cut -d= -f2- || true)"
  POSTGRES_PASS="$(grep -E '^POSTGRES_PASSWORD=' "$FILE" 2>/dev/null | tail -n 1 | cut -d= -f2- || true)"
  if [ -n "$DB_URL" ]; then
    PASS="$(extract_password_from_database_url "$DB_URL")"
    if [ -n "$PASS" ] && [ "$PASS" != "请修改为真实值" ]; then
      printf '%s' "$PASS"
      return 0
    fi
  fi
  if [ -n "$POSTGRES_PASS" ]; then
    printf '%s' "$POSTGRES_PASS"
    return 0
  fi
  return 1
}

find_latest_env_backup() {
  if [ ! -d "$BACKUP_DIR" ]; then
    return 1
  fi
  ls -1t "$BACKUP_DIR"/.env.*.bak 2>/dev/null | head -n 1 || true
}

read_postgres_password_from_compose() {
  COMPOSE_FILE="${PROJECT_DIR}/docker-compose.yml"
  LINE=""
  PASS=""
  if [ ! -f "$COMPOSE_FILE" ]; then
    return 1
  fi
  LINE="$(grep -E '^[[:space:]]*POSTGRES_PASSWORD:' "$COMPOSE_FILE" 2>/dev/null | tail -n 1 || true)"
  if [ -z "$LINE" ]; then
    return 1
  fi
  PASS="$(printf '%s' "$LINE" | sed -E 's/^[[:space:]]*POSTGRES_PASSWORD:[[:space:]]*//')"
  if printf '%s' "$PASS" | grep -qE '^\$\{POSTGRES_PASSWORD:-'; then
    PASS="$(printf '%s' "$PASS" | sed -E 's/^\$\{POSTGRES_PASSWORD:-([^}]*)\}.*/\1/')"
  elif printf '%s' "$PASS" | grep -qE '^\$\{POSTGRES_PASSWORD\}$'; then
    return 1
  fi
  PASS="$(printf '%s' "$PASS" | tr -d '"' | tr -d "'")"
  if [ -z "$PASS" ]; then
    return 1
  fi
  printf '%s' "$PASS"
}

has_existing_pgdata() {
  if [ -d "$PGDATA_DIR" ] && [ -n "$(ls -A "$PGDATA_DIR" 2>/dev/null)" ]; then
    return 0
  fi
  if command -v docker >/dev/null 2>&1; then
    VOLUME_NAME=""
    for VOLUME_NAME in $(docker volume ls -q 2>/dev/null | grep 'pgdata$' || true); do
      if docker run --rm -v "${VOLUME_NAME}:/var/lib/postgresql/data:ro" alpine:3.19 \
        sh -c 'test -f /var/lib/postgresql/data/PG_VERSION' >/dev/null 2>&1; then
        return 0
      fi
    done
  fi
  return 1
}

resolve_legacy_db_password() {
  BACKUP_FILE=""
  PASS=""
  BACKUP_FILE="$(find_latest_env_backup)"
  if [ -n "$BACKUP_FILE" ] && PASS="$(extract_password_from_env_file "$BACKUP_FILE")"; then
    printf '\n[MercuryMail] %s\n' "从备份 ${BACKUP_FILE} 读取旧数据库密码" >&2
    printf '%s' "$PASS"
    return 0
  fi
  if PASS="$(read_postgres_password_from_compose)"; then
    printf '\n[MercuryMail] %s\n' "从 docker-compose.yml 读取 POSTGRES_PASSWORD" >&2
    printf '%s' "$PASS"
    return 0
  fi
  return 1
}

prompt_legacy_db_password() {
  PASS=""
  printf '%s' "检测到旧数据库数据，但无法找到旧密码，请手动输入旧数据库密码："
  read -rs PASS
  echo ""
  if [ -z "$PASS" ]; then
    echo "数据库密码不能为空，部署已中止。"
    exit 1
  fi
  printf '%s' "$PASS"
}

apply_db_password_to_env() {
  PASS="$1"
  if [ -z "$PASS" ]; then
    warn "数据库密码为空，跳过写入"
    return 1
  fi
  update_env_line POSTGRES_PASSWORD "$PASS" || warn "写入 POSTGRES_PASSWORD 失败"
  update_env_line DATABASE_URL "postgresql+asyncpg://mercury:${PASS}@db:5432/mercurymail" || warn "写入 DATABASE_URL 失败"
  sync_compose_postgres_password "$PASS"
}

escape_sql_string() {
  printf '%s' "$1" | sed "s/'/''/g"
}

wait_for_db_ready() {
  DB_USER="${1:-$DB_ROLE_NAME}"
  DB_DATABASE="${2:-$DB_NAME}"
  log "等待 PostgreSQL 服务可连接（pg_isready）"
  for attempt in $(seq 1 60); do
    if is_db_container_running && \
      docker exec "$DB_CONTAINER_NAME" pg_isready -U "$DB_USER" -d "$DB_DATABASE" >/dev/null 2>&1; then
      echo "PostgreSQL 已就绪（第 ${attempt} 次检测）"
      return 0
    fi
    sleep 2
  done
  warn "PostgreSQL pg_isready 等待超时"
  return 1
}

wait_for_db_container_healthy() {
  log "等待 PostgreSQL 容器健康检查通过（docker health）"
  for attempt in $(seq 1 60); do
    if ! is_db_container_running; then
      sleep 2
      continue
    fi
    STATUS="$(container_health_status "$DB_CONTAINER_NAME")"
    if [ "$STATUS" = "healthy" ]; then
      echo "PostgreSQL 容器已 healthy（第 ${attempt} 次检测）"
      return 0
    fi
    if [ "$STATUS" = "unhealthy" ]; then
      warn_red "PostgreSQL 容器健康检查失败（status=unhealthy）"
      return 1
    fi
    sleep 2
  done
  warn_red "PostgreSQL 容器健康检查等待超时（120 秒）"
  return 1
}

psql_escape_literal() {
  printf '%s' "$1" | sed "s/'/''/g"
}

psql_connect_and_alter_mercury_password() {
  CONNECT_USER="$1"
  CONNECT_PASSWORD="$2"
  TARGET_PASSWORD="$3"
  TARGET_PASSWORD_ESCAPED="$(psql_escape_literal "$TARGET_PASSWORD")"
  docker exec -e PGPASSWORD="$CONNECT_PASSWORD" "$DB_CONTAINER_NAME" \
    psql -h 127.0.0.1 -U "$CONNECT_USER" -d "$DB_NAME" -v ON_ERROR_STOP=1 \
    -c "ALTER USER ${DB_ROLE_NAME} WITH PASSWORD '${TARGET_PASSWORD_ESCAPED}';" >/dev/null 2>&1
}

psql_local_superuser_sync_passwords() {
  TARGET_PASSWORD="$1"
  ADMIN_USER_VALUE="$2"
  ADMIN_PASSWORD_VALUE="$3"
  TARGET_PASSWORD_ESCAPED="$(psql_escape_literal "$TARGET_PASSWORD")"
  ADMIN_PASSWORD_ESCAPED="$(psql_escape_literal "$ADMIN_PASSWORD_VALUE")"
  case "$ADMIN_USER_VALUE" in
    ''|*[!A-Za-z0-9_]*) ADMIN_USER_VALUE="$PG_ADMIN_USER_DEFAULT" ;;
  esac
  docker exec -u postgres "$DB_CONTAINER_NAME" psql -d "$DB_NAME" -v ON_ERROR_STOP=1 \
    -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${DB_ROLE_NAME}') THEN CREATE ROLE ${DB_ROLE_NAME} WITH LOGIN PASSWORD '${TARGET_PASSWORD_ESCAPED}'; ELSE ALTER ROLE ${DB_ROLE_NAME} WITH LOGIN PASSWORD '${TARGET_PASSWORD_ESCAPED}'; END IF; END \$\$;" \
    -c "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${ADMIN_USER_VALUE}') THEN CREATE ROLE ${ADMIN_USER_VALUE} WITH LOGIN SUPERUSER PASSWORD '${ADMIN_PASSWORD_ESCAPED}'; ELSE ALTER ROLE ${ADMIN_USER_VALUE} WITH LOGIN SUPERUSER PASSWORD '${ADMIN_PASSWORD_ESCAPED}'; END IF; END \$\$;" \
    -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_ROLE_NAME};" >/dev/null 2>&1
}

ensure_pg_admin_password_in_env() {
  PG_ADMIN_PASS_VALUE="$(read_env_value_from_file PG_ADMIN_PASSWORD)"
  if [ -n "$PG_ADMIN_PASS_VALUE" ] && [ "$PG_ADMIN_PASS_VALUE" != "CHANGE_ME" ] && [ "$PG_ADMIN_PASS_VALUE" != "password" ]; then
    ensure_env_key PG_ADMIN_USER "$PG_ADMIN_USER_DEFAULT"
    return 0
  fi
  PG_ADMIN_PASS_VALUE="$(generate_db_password || true)"
  if [ -n "$PG_ADMIN_PASS_VALUE" ]; then
    update_env_line PG_ADMIN_USER "$PG_ADMIN_USER_DEFAULT" || warn "写入 PG_ADMIN_USER 失败"
    update_env_line PG_ADMIN_PASSWORD "$PG_ADMIN_PASS_VALUE" || warn "写入 PG_ADMIN_PASSWORD 失败"
    chmod 600 "$ENV_FILE" 2>/dev/null || true
    log "PG_ADMIN_PASSWORD 未设置或仍为占位符，已自动生成并写入 .env"
  else
    warn "PG_ADMIN_PASSWORD 随机生成失败"
  fi
}

verify_postgres_password_from_env() {
  DB_URL=""
  PARSED_DB_USER=""
  PARSED_DB_PASS=""
  PARSED_DB_NAME=""

  if ! is_db_container_running; then
    warn_red "数据库容器未运行，无法验证密码"
    return 1
  fi
  if ! wait_for_db_ready "$DB_ROLE_NAME" "$DB_NAME"; then
    warn_red "数据库未就绪，无法验证密码"
    return 1
  fi
  align_database_credentials_in_env
  DB_URL="$(get_env_value DATABASE_URL)"
  if [ -z "$DB_URL" ]; then
    warn_red "未找到 DATABASE_URL，无法验证数据库密码"
    return 1
  fi
  if ! parse_database_url_credentials "$DB_URL"; then
    warn_red "无法解析 DATABASE_URL（格式应为 postgresql+asyncpg://用户:密码@主机:端口/库名）"
    return 1
  fi
  log "正在验证 PostgreSQL 用户 ${PARSED_DB_USER} 密码与 .env 一致（日志不会输出明文密码）"
  if docker exec -e PGPASSWORD="$PARSED_DB_PASS" "$DB_CONTAINER_NAME" \
    psql -h 127.0.0.1 -U "$PARSED_DB_USER" -d "$PARSED_DB_NAME" -v ON_ERROR_STOP=1 -c "SELECT 1" >/dev/null 2>&1; then
    log "数据库密码验证通过（${PARSED_DB_USER}@${PARSED_DB_NAME}）"
    return 0
  fi
  warn_red "数据库密码验证失败：应用用户密码仍未与 .env 同步"
  return 1
}

sync_database_password() {
  DB_URL=""
  TARGET_PASS=""
  ADMIN_USER=""
  ADMIN_PASS=""
  POSTGRES_PASS=""

  if ! is_db_container_running; then
    warn_red "数据库容器未运行，无法同步数据库密码"
    return 1
  fi
  align_database_credentials_in_env
  DB_URL="$(get_env_value DATABASE_URL)"
  if ! parse_database_url_credentials "$DB_URL"; then
    warn_red "DATABASE_URL 无法解析，无法同步数据库密码"
    return 1
  fi
  TARGET_PASS="$PARSED_DB_PASS"
  ADMIN_USER="$(read_env_value_from_file PG_ADMIN_USER)"
  ADMIN_PASS="$(read_env_value_from_file PG_ADMIN_PASSWORD)"
  POSTGRES_PASS="$(read_env_value_from_file POSTGRES_PASSWORD)"
  if [ -z "$ADMIN_USER" ]; then
    ADMIN_USER="$PG_ADMIN_USER_DEFAULT"
  fi

  log "开始同步 PostgreSQL 应用用户密码（仅输出脱敏状态，不输出明文）"
  if [ -n "$ADMIN_PASS" ] && psql_connect_and_alter_mercury_password "$ADMIN_USER" "$ADMIN_PASS" "$TARGET_PASS"; then
    log "已通过管理用户 ${ADMIN_USER} 同步 ${DB_ROLE_NAME} 数据库密码"
    return 0
  fi

  warn "管理用户 ${ADMIN_USER} 连接失败或不存在，尝试旧版数据卷兼容同步"
  if [ -n "$POSTGRES_PASS" ] && psql_connect_and_alter_mercury_password "postgres" "$POSTGRES_PASS" "$TARGET_PASS"; then
    log "已通过旧版 postgres 超级用户同步 ${DB_ROLE_NAME} 数据库密码"
    return 0
  fi
  if [ -n "$TARGET_PASS" ] && psql_connect_and_alter_mercury_password "postgres" "$TARGET_PASS" "$TARGET_PASS"; then
    log "已通过当前 .env 密码连接 postgres 用户并同步 ${DB_ROLE_NAME} 数据库密码"
    return 0
  fi

  if verify_postgres_password_from_env >/dev/null 2>&1; then
    log "应用用户密码已可用，无需额外同步"
    return 0
  fi

  warn "远程密码方式均不可用，尝试以容器内 postgres 系统用户执行本地维护同步"
  if psql_local_superuser_sync_passwords "$TARGET_PASS" "$ADMIN_USER" "$ADMIN_PASS"; then
    log "已通过容器内本地超级用户通道同步 ${DB_ROLE_NAME} 密码，并补齐管理用户 ${ADMIN_USER}"
    return 0
  fi

  warn_red "数据库密码同步失败；为保护数据已停止，请检查配置并恢复备份。"
  return 1
}

detect_pgdata_volume_names() {
  {
    docker inspect "$DB_CONTAINER_NAME" -f '{{range .Mounts}}{{if eq .Destination "/var/lib/postgresql/data"}}{{.Name}}{{end}}{{end}}' 2>/dev/null || true
    docker volume ls -q 2>/dev/null | grep -E '(^|_)pgdata$' || true
    docker volume ls -q 2>/dev/null | grep -E 'mercury.*pgdata|pgdata' || true
  } | awk 'NF && !seen[$0]++'
}

remove_postgres_volumes_force() {
  TS="$(date +%Y%m%d_%H%M%S)"
  cd "$PROJECT_DIR"
  compose_cmd stop db >/dev/null 2>&1 || true
  docker rm -f "$DB_CONTAINER_NAME" >/dev/null 2>&1 || true
  for OLD_VOLUME in $(detect_pgdata_volume_names); do
    if [ -n "$OLD_VOLUME" ] && docker volume inspect "$OLD_VOLUME" >/dev/null 2>&1; then
      BACKUP_VOLUME="${OLD_VOLUME}_broken_${TS}"
      log "隔离旧 PostgreSQL 卷：${OLD_VOLUME} -> ${BACKUP_VOLUME}"
      docker volume create "$BACKUP_VOLUME" >/dev/null 2>&1 || true
      docker run --rm -v "${OLD_VOLUME}:/from:ro" -v "${BACKUP_VOLUME}:/to" alpine:3.19 sh -c 'cd /from 2>/dev/null && tar cf - . | (cd /to && tar xpf -)' >/dev/null 2>&1 || warn "旧数据卷 ${OLD_VOLUME} 复制备份失败，将继续移除原卷"
      docker volume rm -f "$OLD_VOLUME" >/dev/null 2>&1 || warn "旧数据卷 ${OLD_VOLUME} 移除失败"
    fi
  done
  if [ -d "$PGDATA_DIR" ] && [ -n "$(ls -A "$PGDATA_DIR" 2>/dev/null)" ]; then
    mv "$PGDATA_DIR" "${PGDATA_DIR}.broken.${TS}" || warn "本地 pgdata 目录隔离失败"
  fi
}

recreate_postgres_data_volume() {
  log "进入 PostgreSQL 数据卷自动重建流程：强制隔离真实 pgdata 卷并新建数据库"
  remove_postgres_volumes_force
  ensure_db_password_consistency
  ensure_pg_admin_password_in_env
  log "重新创建 PostgreSQL 数据库容器和全新数据卷"
  compose_cmd up -d --force-recreate db
  if ! wait_for_db_container_healthy; then
    warn_red "重建后的 PostgreSQL 容器健康检查失败"
    compose_cmd logs db --tail=120 || true
    return 1
  fi
  if ! wait_for_db_ready "$DB_ROLE_NAME" "$DB_NAME"; then
    warn_red "重建后的 PostgreSQL 服务不可连接"
    compose_cmd logs db --tail=120 || true
    return 1
  fi
  if verify_postgres_password_from_env >/dev/null 2>&1; then
    log "全新 PostgreSQL 数据卷已创建且密码验证通过"
    return 0
  fi
  warn_red "全新 PostgreSQL 数据卷创建后密码仍验证失败，输出 PostgreSQL 初始化日志用于定位"
  compose_cmd logs db --tail=160 || true
  return 1
}

ensure_db_password_consistency() {
  PASS=""
  DB_URL=""
  COMPOSE_PASS=""
  URL_PASS=""
  if [ ! -f "$ENV_FILE" ]; then
    return 0
  fi
  align_database_credentials_in_env
  PASS="$(get_env_value POSTGRES_PASSWORD)"
  if [ -z "$PASS" ]; then
    DB_URL="$(get_env_value DATABASE_URL)"
    PASS="$(extract_password_from_database_url "$DB_URL")"
  fi
  if [ -z "$PASS" ] || [ "$PASS" = "CHANGE_ME" ] || [ "$PASS" = "password" ]; then
    PASS="$(generate_db_password || true)"
    if [ -z "$PASS" ]; then
      return 0
    fi
    apply_db_password_to_env "$PASS"
  fi
  if ! grep -q "^POSTGRES_PASSWORD=" "$ENV_FILE" 2>/dev/null; then
    update_env_line POSTGRES_PASSWORD "$PASS" || true
  fi
  COMPOSE_PASS="$(read_postgres_password_from_compose || true)"
  if [ -n "$COMPOSE_PASS" ] && [ "$COMPOSE_PASS" = "$PASS" ]; then
    return 0
  fi
  log "同步 docker-compose.yml 中的 POSTGRES_PASSWORD 与 .env 一致"
  sync_compose_postgres_password "$PASS"
}

# 确保 DATABASE_URL 与 POSTGRES_PASSWORD 使用同一密码（以 DATABASE_URL 为准）
align_database_credentials_in_env() {
  DB_URL=""
  POSTGRES_PASS=""
  URL_PASS=""
  if [ ! -f "$ENV_FILE" ]; then
    return 0
  fi
  DB_URL="$(get_env_value DATABASE_URL)"
  POSTGRES_PASS="$(get_env_value POSTGRES_PASSWORD)"
  URL_PASS="$(extract_password_from_database_url "$DB_URL" || true)"
  if [ -n "$POSTGRES_PASS" ] && [ "$POSTGRES_PASS" != "CHANGE_ME" ] && [ "$POSTGRES_PASS" != "password" ]; then
    if ! parse_database_url_credentials "$DB_URL" || [ "$PARSED_DB_USER" != "$DB_ROLE_NAME" ] || [ "$PARSED_DB_NAME" != "$DB_NAME" ] || [ "$URL_PASS" != "$POSTGRES_PASS" ]; then
      log "规范化 DATABASE_URL：统一使用 ${DB_ROLE_NAME}@db/${DB_NAME} 和 POSTGRES_PASSWORD"
      apply_db_password_to_env "$POSTGRES_PASS" || warn "写入 DATABASE_URL 密码失败"
    fi
    return 0
  fi
  if [ -n "$URL_PASS" ] && [ "$URL_PASS" != "CHANGE_ME" ] && [ "$URL_PASS" != "password" ]; then
    log "补写 POSTGRES_PASSWORD（与 DATABASE_URL 密码一致）"
    update_env_line POSTGRES_PASSWORD "$URL_PASS" || warn "写入 POSTGRES_PASSWORD 失败"
    apply_db_password_to_env "$URL_PASS" || warn "规范化 DATABASE_URL 失败"
    return 0
  fi
  log "数据库密码为空或仍为占位符，自动生成新密码并规范化 DATABASE_URL"
  NEW_DB_PASS="$(generate_db_password || true)"
  if [ -n "$NEW_DB_PASS" ]; then
    apply_db_password_to_env "$NEW_DB_PASS" || warn "写入数据库密码失败"
  fi
}

validate_critical_env_before_compose() {
  SECRET_VALUE=""
  ENCRYPTION_VALUE=""
  DB_URL_VALUE=""
  if [ ! -f "$ENV_FILE" ]; then
    warn_red ".env 不存在，无法启动容器"
    return 1
  fi
  align_database_credentials_in_env
  SECRET_VALUE="$(get_env_value SECRET_KEY)"
  ENCRYPTION_VALUE="$(get_env_value ENCRYPTION_KEY)"
  DB_URL_VALUE="$(get_env_value DATABASE_URL)"
  if [ -z "$SECRET_VALUE" ] || [ "${#SECRET_VALUE}" -lt 32 ]; then
    warn_red "SECRET_KEY 未设置或长度不足 32 字符，请检查 .env（deploy.sh 会自动补全）"
    return 1
  fi
  if [ -z "$ENCRYPTION_VALUE" ]; then
    warn_red "ENCRYPTION_KEY 未设置，生产环境拒绝启动，请检查 .env"
    return 1
  fi
  if [ -z "$DB_URL_VALUE" ]; then
    warn_red "DATABASE_URL 未设置"
    return 1
  fi
  if ! parse_database_url_credentials "$DB_URL_VALUE"; then
    warn_red "DATABASE_URL 格式无效，须为 postgresql+asyncpg://用户:密码@db:5432/库名"
    return 1
  fi
  log "关键环境变量校验通过（SECRET_KEY、ENCRYPTION_KEY、DATABASE_URL）"
  return 0
}

container_health_status() {
  NAME="$1"
  docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$NAME" 2>/dev/null || echo "missing"
}

verify_container_healthy() {
  NAME="$1"
  LABEL="$2"
  RUNNING="$(docker inspect -f '{{.State.Running}}' "$NAME" 2>/dev/null || echo false)"
  HEALTH="$(container_health_status "$NAME")"
  if [ "$RUNNING" != "true" ]; then
    echo "${LABEL} 未运行（${NAME}）"
    return 1
  fi
  if [ "$HEALTH" = "unhealthy" ]; then
    echo "${LABEL} 健康检查失败（${NAME}，status=${HEALTH}）"
    return 1
  fi
  return 0
}

generate_initial_env_file() {
  if [ -f "$ENV_FILE" ]; then
    return 0
  fi

  DB_PASSWORD_VALUE=""

  log "首次部署：自动生成 .env，并使用全新 PostgreSQL 数据卷初始化数据库"
  EXAMPLE_FILE="${PROJECT_DIR}/.env.example"
  if [ ! -f "$EXAMPLE_FILE" ]; then
    warn "未找到 .env.example，跳过自动生成 .env"
    return 0
  fi
  if ! cp "$EXAMPLE_FILE" "$ENV_FILE" 2>/dev/null; then
    warn "复制 .env.example 失败，跳过自动生成 .env"
    return 0
  fi
  SECRET_KEY_VALUE="$(generate_random_32 || true)"
  ENCRYPTION_KEY_VALUE="$(generate_random_32 || true)"
  if [ -n "$SECRET_KEY_VALUE" ]; then
    update_env_line SECRET_KEY "$SECRET_KEY_VALUE" || warn "写入 SECRET_KEY 失败"
  else
    warn "SECRET_KEY 随机生成失败，保留模板默认值"
  fi
  if [ -n "$ENCRYPTION_KEY_VALUE" ]; then
    update_env_line ENCRYPTION_KEY "$ENCRYPTION_KEY_VALUE" || warn "写入 ENCRYPTION_KEY 失败"
  else
    warn "ENCRYPTION_KEY 随机生成失败，保留模板默认值"
  fi
  PG_ADMIN_PASSWORD_VALUE="$(generate_db_password || true)"
  if [ -n "$PG_ADMIN_PASSWORD_VALUE" ]; then
    update_env_line PG_ADMIN_USER "$PG_ADMIN_USER_DEFAULT" || warn "写入 PG_ADMIN_USER 失败"
    update_env_line PG_ADMIN_PASSWORD "$PG_ADMIN_PASSWORD_VALUE" || warn "写入 PG_ADMIN_PASSWORD 失败"
  else
    warn "PG_ADMIN_PASSWORD 随机生成失败，后续将尝试补全"
  fi
  DB_PASSWORD_VALUE="$(generate_db_password || true)"
  if [ -n "$DB_PASSWORD_VALUE" ]; then
    apply_db_password_to_env "$DB_PASSWORD_VALUE"
  else
    warn "数据库密码随机生成失败，保留模板默认值"
  fi
  ADMIN_PASSWORD_VALUE="$(generate_admin_password || true)"
  if [ -n "$ADMIN_PASSWORD_VALUE" ]; then
    update_env_line ADMIN_PASSWORD "$ADMIN_PASSWORD_VALUE" || warn "写入 ADMIN_PASSWORD 失败"
  else
    warn "管理员密码随机生成失败，应用启动时将自动生成并写入容器日志"
    update_env_line ADMIN_PASSWORD "" || warn "清空 ADMIN_PASSWORD 失败"
  fi
  update_env_line OPENAI_API_KEY "" || warn "清空 OPENAI_API_KEY 失败"
  REDIS_PASSWORD_VALUE="$(generate_db_password || true)"
  if [ -n "$REDIS_PASSWORD_VALUE" ]; then
    update_env_line REDIS_PASSWORD "$REDIS_PASSWORD_VALUE" || warn "写入 REDIS_PASSWORD 失败"
    update_env_line REDIS_URL "redis://:${REDIS_PASSWORD_VALUE}@redis:6379/0" || warn "写入 REDIS_URL Redis 密码失败"
  else
    ensure_env_key REDIS_PASSWORD ""
  fi
  chmod 600 "$ENV_FILE" 2>/dev/null || true
  echo "已自动生成 .env 及随机密钥，数据库密码已同步；数据库将使用新卷初始化。"
  if [ -n "$ADMIN_PASSWORD_VALUE" ]; then
    echo "管理员密码已写入 .env，部署完成后将在终端显示。"
  fi
}

is_private_ip() {
  case "$1" in
    10.*|192.168.*|127.*|0.*|169.254.*)
      return 0
      ;;
    172.*)
      SECOND_OCTET="$(printf '%s' "$1" | cut -d. -f2)"
      if [ "$SECOND_OCTET" -ge 16 ] 2>/dev/null && [ "$SECOND_OCTET" -le 31 ] 2>/dev/null; then
        return 0
      fi
      ;;
  esac
  return 1
}

get_public_ip_from_metadata() {
  TOKEN="$(curl -fsS --max-time 2 -X PUT 'http://100.100.100.200/latest/api/token' -H 'X-aliyun-ecs-metadata-token-ttl-seconds: 300' 2>/dev/null || true)"
  if [ -n "$TOKEN" ]; then
    curl -fsS --max-time 3 -H "X-aliyun-ecs-metadata-token: ${TOKEN}" 'http://100.100.100.200/latest/meta-data/eipv4' 2>/dev/null || \
      curl -fsS --max-time 3 -H "X-aliyun-ecs-metadata-token: ${TOKEN}" 'http://100.100.100.200/latest/meta-data/public-ipv4' 2>/dev/null || true
    return 0
  fi
  curl -fsS --max-time 3 'http://100.100.100.200/latest/meta-data/eipv4' 2>/dev/null || \
    curl -fsS --max-time 3 'http://100.100.100.200/latest/meta-data/public-ipv4' 2>/dev/null || true
}

get_server_ip() {
  for endpoint in \
    "https://4.ipw.cn" \
    "https://api.ipify.org" \
    "https://ifconfig.me/ip" \
    "https://checkip.amazonaws.com"; do
    PUBLIC_IP="$(curl -fsS --max-time 5 "$endpoint" 2>/dev/null | tr -d '[:space:]' || true)"
    if [ -n "$PUBLIC_IP" ] && ! is_private_ip "$PUBLIC_IP"; then
      printf '%s' "$PUBLIC_IP"
      return 0
    fi
  done

  PUBLIC_IP="$(get_public_ip_from_metadata | tr -d '[:space:]')"
  if [ -n "$PUBLIC_IP" ] && ! is_private_ip "$PUBLIC_IP"; then
    printf '%s' "$PUBLIC_IP"
    return 0
  fi

  PUBLIC_IP="$(ip -4 route get 1.1.1.1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="src") {print $(i+1); exit}}')"
  if [ -z "$PUBLIC_IP" ]; then
    PUBLIC_IP="$(hostname -I | awk '{print $1}')"
  fi
  printf '%s' "$PUBLIC_IP"
}

get_public_base_url() {
  if [ -n "$PUBLIC_BASE_URL_OVERRIDE" ]; then
    printf '%s' "${PUBLIC_BASE_URL_OVERRIDE%/}"
    return 0
  fi
  SERVER_IP="$(get_server_ip)"
  # 退订链接等对外 URL 走 Nginx 前端 80 端口
  printf 'http://%s' "$SERVER_IP"
}

get_env_value() {
  KEY="$1"
  if [ ! -f "$ENV_FILE" ]; then
    return 0
  fi
  grep -E "^${KEY}=" "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tail -n 1 | normalize_env_value || true
}

# 规范化 .env 变量值：去除 CRLF、首尾空白与成对引号
normalize_env_value() {
  VALUE="$(cat)"
  VALUE="$(printf '%s' "$VALUE" | tr -d '\r')"
  VALUE="$(printf '%s' "$VALUE" | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//')"
  if [ "${#VALUE}" -ge 2 ]; then
    FIRST_CHAR="$(printf '%s' "$VALUE" | cut -c1)"
    LAST_CHAR="$(printf '%s' "$VALUE" | rev | cut -c1 | rev)"
    if [ "$FIRST_CHAR" = "$LAST_CHAR" ] && { [ "$FIRST_CHAR" = '"' ] || [ "$FIRST_CHAR" = "'" ]; }; then
      VALUE="$(printf '%s' "$VALUE" | sed -e "s/^${FIRST_CHAR}//" -e "s/${LAST_CHAR}$//")"
    fi
  fi
  printf '%s' "$VALUE"
}

# 从 .env 读取指定键：优先取最后一个非空值，避免重复键或空行覆盖用户配置
read_env_value_from_file() {
  KEY="$1"
  MATCHED=""
  if [ ! -f "$ENV_FILE" ]; then
    return 0
  fi
  while IFS= read -r line || [ -n "$line" ]; do
    line="$(printf '%s' "$line" | tr -d '\r')"
    case "$line" in
      ''|'#'*) continue ;;
    esac
    case "$line" in
      "${KEY}="*)
        CANDIDATE="$(printf '%s' "${line#*=}" | normalize_env_value)"
        if [ -n "$CANDIDATE" ]; then
          MATCHED="$CANDIDATE"
        fi
        ;;
    esac
  done < "$ENV_FILE"
  if [ -n "$MATCHED" ]; then
    printf '%s' "$MATCHED"
    return 0
  fi
  get_env_value "$KEY"
}

ensure_env_key() {
  KEY="$1"
  VALUE="$2"
  if ! grep -q "^${KEY}=" "$ENV_FILE"; then
    printf '\n%s=%s\n' "$KEY" "$VALUE" >> "$ENV_FILE"
  fi
}

replace_empty_env_key() {
  KEY="$1"
  VALUE="$2"
  if grep -Eq "^${KEY}=[[:space:]]*$" "$ENV_FILE"; then
    sed -i "s|^${KEY}=.*|${KEY}=${VALUE}|" "$ENV_FILE"
  fi
}

sync_redis_url_with_password() {
  if [ ! -f "$ENV_FILE" ]; then
    return 0
  fi
  REDIS_PASSWORD_VALUE="$(read_env_value_from_file REDIS_PASSWORD)"
  if [ -z "$REDIS_PASSWORD_VALUE" ]; then
    return 0
  fi
  REDIS_URL_VALUE="$(read_env_value_from_file REDIS_URL)"
  case "$REDIS_URL_VALUE" in
    redis://:*@*|rediss://:*@*|redis://*@*|rediss://*@*)
      return 0
      ;;
  esac
  update_env_line REDIS_URL "redis://:${REDIS_PASSWORD_VALUE}@redis:6379/0" || warn "同步 REDIS_URL Redis 密码失败"
}

# 旧版 deploy.sh 曾自动写入 SENDGRID_DAILY_QUOTA=100000，迁移为 0 表示未启用配额展示
migrate_legacy_sendgrid_daily_quota() {
  if [ ! -f "$ENV_FILE" ]; then
    return 0
  fi
  CURRENT="$(get_env_value SENDGRID_DAILY_QUOTA)"
  if [ "$CURRENT" = "100000" ]; then
    update_env_line SENDGRID_DAILY_QUOTA "0" || warn "迁移 SENDGRID_DAILY_QUOTA 失败"
    log "已将 SENDGRID_DAILY_QUOTA 从旧默认 100000 迁移为 0（未启用每日配额展示）"
  fi
}

complete_env_file() {
  log "补全环境变量（启动容器前）"
  mkdir -p "$DATA_DIR" "$BACKUP_DIR"
  chmod 700 "$DATA_DIR"
  chmod 700 "$BACKUP_DIR"
  PUBLIC_BASE_URL_VALUE="$(get_public_base_url)"
  if [ ! -f "$ENV_FILE" ]; then
    warn ".env 不存在且自动生成未成功，将尝试继续补全其他配置"
    return 0
  fi
  chmod 600 "$ENV_FILE"
  migrate_legacy_sendgrid_daily_quota
  ensure_env_key ADMIN_USERNAME "admin"
  ensure_env_key ADMIN_EMAIL "admin@example.com"
  ensure_env_key SENDGRID_API_KEY ""
  ensure_env_key ADMIN_PASSWORD ""
  ensure_env_key RESET_ADMIN_PASSWORD "false"
  ensure_env_key REDIS_PASSWORD ""
  ensure_env_key SUPERADMIN_USERNAME "superadmin"
  ensure_env_key SUPER_ADMIN_USERNAME "superadmin"
  ensure_env_key PERF_DEBUG "false"
  ensure_env_key DEBUG_SEND_LOG_VARIABLES "false"
  ensure_env_key POLL_INTERVAL_SECONDS "120"
  ensure_env_key HOT_RETENTION_DAYS "3"
  ensure_env_key ARCHIVE_RETENTION_DAYS "30"
  ensure_env_key LOST_EVENT_RETENTION_DAYS "14"
  ensure_env_key CLEAN_SOURCE_AFTER_DEPLOY "false"
  ensure_env_key GLOBAL_MAX_CONCURRENCY "1000"
  ensure_env_key TASK_MAX_CONCURRENCY_RATIO "0.8"
  ensure_env_key DB_POOL_SIZE "100"
  ensure_env_key DB_MAX_OVERFLOW "100"
  ensure_env_key DEFAULT_HTTP_BATCH_SIZE "50"
  ensure_env_key WARMUP_DEFAULT_HTTP_BATCH_SIZE "10"
  ensure_env_key ESTIMATED_PROXY_RTT_SECONDS "1.5"
  ensure_env_key ENABLE_SENDGRID_KEEPALIVE "true"
  ensure_env_key ENABLE_SENDGRID_HTTP2 "true"
  ensure_env_key WRITER_BATCH_SIZE "100"
  ensure_env_key WRITER_TURBO_BATCH_SIZE "300"
  ensure_env_key SENDGRID_DAILY_QUOTA "0"
  replace_empty_env_key SENDGRID_DAILY_QUOTA "0"
  ensure_env_key SENDGRID_WEBHOOK_VERIFICATION_KEY ""
  ensure_env_key SENDER_POOL_WORKERS "8"
  ensure_env_key WRITER_MAX_FLUSHES_PER_SECOND "10"
  ensure_env_key GC_GENERATION0_THRESHOLD "50000"
  ensure_env_key ENABLE_PID_CONCURRENCY_TUNING "true"
  ensure_env_key ENABLE_FEEDFORWARD_CONCURRENCY "true"
  ensure_env_key ENABLE_QUEUE_AWARE_CONCURRENCY "true"
  ensure_env_key SEND_EMAIL_TIMEOUT_SECONDS "30"
  ensure_env_key EXPORT_ASYNC_ROW_THRESHOLD "5000000"
  ensure_env_key EXPORT_ASYNC_DIR "/tmp/mercury_exports"
  ensure_env_key EXPORT_ASYNC_MAX_TASKS "3"
  ensure_env_key EXPORT_ASYNC_FILE_TTL_HOURS "24"
  ensure_env_key SECRET_KEY "$(generate_secret)"
  ensure_env_key REDIS_URL "redis://redis:6379/0"
  ensure_env_key DATABASE_URL "postgresql+asyncpg://mercury:CHANGE_ME@db:5432/mercurymail"
  ensure_env_key POSTGRES_PASSWORD "CHANGE_ME"
  ensure_env_key PG_ADMIN_USER "$PG_ADMIN_USER_DEFAULT"
  ensure_env_key PG_ADMIN_PASSWORD "$(generate_db_password)"
  replace_empty_env_key PG_ADMIN_PASSWORD "$(generate_db_password)"
  ensure_env_key ACCESS_TOKEN_EXPIRE_MINUTES "720"
  ensure_env_key APP_ENV "production"
  ensure_env_key PUBLIC_BASE_URL "$PUBLIC_BASE_URL_VALUE"
  ensure_env_key FRONTEND_HTTP_PORT "${FRONTEND_PORT}"
  ensure_env_key CORS_ORIGINS ""
  ensure_env_key OPENAI_API_KEY ""
  ensure_env_key OPENROUTER_BASE_URL "https://openrouter.ai/api/v1/chat/completions"
  ensure_env_key ENCRYPTION_KEY "$(generate_secret)"
  replace_empty_env_key SECRET_KEY "$(generate_secret)"
  replace_empty_env_key REDIS_URL "redis://redis:6379/0"
  replace_empty_env_key DATABASE_URL "postgresql+asyncpg://mercury:CHANGE_ME@db:5432/mercurymail"
  replace_empty_env_key ACCESS_TOKEN_EXPIRE_MINUTES "720"
  replace_empty_env_key APP_ENV "production"
  replace_empty_env_key PUBLIC_BASE_URL "$PUBLIC_BASE_URL_VALUE"
  if grep -Eq '^PUBLIC_BASE_URL=(http://your-server-ip(:[0-9]+)?|[[:space:]]*)$' "$ENV_FILE" 2>/dev/null; then
    sed -i "s|^PUBLIC_BASE_URL=.*|PUBLIC_BASE_URL=${PUBLIC_BASE_URL_VALUE}|" "$ENV_FILE" 2>/dev/null || true
  fi
  replace_empty_env_key OPENROUTER_BASE_URL "https://openrouter.ai/api/v1/chat/completions"
  replace_empty_env_key ENCRYPTION_KEY "$(generate_secret)"
  sync_redis_url_with_password
  ensure_db_password_consistency
}

load_env_values() {
  ADMIN_USERNAME="$(read_env_value_from_file ADMIN_USERNAME)"
  ADMIN_EMAIL="$(read_env_value_from_file ADMIN_EMAIL)"
  ADMIN_PASSWORD="$(read_env_value_from_file ADMIN_PASSWORD)"
  if [ -z "$ADMIN_USERNAME" ]; then
    ADMIN_USERNAME="admin"
  fi
}

is_db_container_running() {
  [ "$(docker inspect -f '{{.State.Running}}' "$DB_CONTAINER_NAME" 2>/dev/null || echo false)" = "true" ]
}

count_regular_admin_users() {
  if ! is_db_container_running; then
    printf '0'
    return 1
  fi
  docker exec mercury-mail-db psql -U mercury -d mercurymail -tAc \
    "SELECT COUNT(*) FROM users WHERE COALESCE(is_super_admin, FALSE) = FALSE" \
    2>/dev/null | tr -d '[:space:]' || printf '0'
}

admin_user_exists_in_db() {
  COUNT="$(count_regular_admin_users || true)"
  [ "${COUNT:-0}" -gt 0 ] 2>/dev/null
}

detect_admin_user_before_deploy() {
  ADMIN_EXISTED_BEFORE=0
  COUNT="$(count_regular_admin_users || true)"
  if [ "${COUNT:-0}" -gt 0 ] 2>/dev/null; then
    ADMIN_EXISTED_BEFORE=1
  fi
}

backup_before_deploy() {
  DEPLOY_TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
  log "预部署备份"
  mkdir -p "$BACKUP_DIR"
  if [ -f "$ENV_FILE" ]; then
    ENV_BACKUP="${BACKUP_DIR}/.env.${DEPLOY_TIMESTAMP}.bak"
    cp "$ENV_FILE" "$ENV_BACKUP"
    echo "已备份 .env → ${ENV_BACKUP}"
  fi
  if [ -f "${PROJECT_DIR}/docker-compose.yml" ]; then
    COMPOSE_BACKUP="${BACKUP_DIR}/docker-compose.yml.${DEPLOY_TIMESTAMP}.bak"
    cp "${PROJECT_DIR}/docker-compose.yml" "$COMPOSE_BACKUP"
    echo "已备份 docker-compose.yml → ${COMPOSE_BACKUP}"
  fi
  if is_db_container_running; then
    DB_BACKUP="${BACKUP_DIR}/mercurymail_${DEPLOY_TIMESTAMP}.sql.gz"
    log "尝试备份 PostgreSQL 数据库"
    if docker exec mercury-mail-db pg_dump -U mercury -d mercurymail | gzip > "$DB_BACKUP"; then
      echo "数据库备份已保存：${DB_BACKUP}"
    else
      warn "数据库备份失败，部署将继续执行"
      DB_BACKUP=""
    fi
  else
    warn "数据库容器未运行，跳过 pg_dump 备份"
  fi
}

rollback_on_failure() {
  warn "冒烟测试失败，正在尝试恢复配置文件"
  if [ -n "$ENV_BACKUP" ] && [ -f "$ENV_BACKUP" ]; then
    cp "$ENV_BACKUP" "$ENV_FILE"
    echo "已恢复 .env"
  fi
  if [ -n "$COMPOSE_BACKUP" ] && [ -f "$COMPOSE_BACKUP" ]; then
    cp "$COMPOSE_BACKUP" "${PROJECT_DIR}/docker-compose.yml"
    echo "已恢复 docker-compose.yml"
  fi
  echo "请检查容器日志：cd ${PROJECT_DIR} && docker compose --project-directory ${PROJECT_DIR} -f ${COMPOSE_FILE} logs --tail=120"
  echo "确认问题后可重新执行：bash deploy.sh"
}

# 幂等部署：检测 compose 文件是否存在
compose_file_exists() {
  [ -f "${PROJECT_DIR}/docker-compose.yml" ] || \
  [ -f "${PROJECT_DIR}/compose.yaml" ] || \
  [ -f "${PROJECT_DIR}/compose.yml" ]
}

# 幂等部署：确保使用 Docker Compose v2 插件（docker compose，非 docker-compose）
ensure_compose_cli() {
  if ! command -v docker >/dev/null 2>&1; then
    warn "Docker 未安装，跳过 compose 操作"
    return 1
  fi
  if ! docker compose version >/dev/null 2>&1; then
    warn "未检测到 docker compose 插件，请安装 docker-compose-plugin"
    return 1
  fi
  return 0
}

# 固定 compose 项目目录，避免在非项目根执行时 build.context 相对路径解析失败
compose_cmd() {
  docker compose --project-directory "${PROJECT_DIR}" -f "${COMPOSE_FILE}" "$@"
}

verify_deploy_layout() {
  log "校验部署目录与前端构建上下文"
  if [ ! -f "${COMPOSE_FILE}" ]; then
    echo "缺少 docker-compose.yml：${COMPOSE_FILE}"
    exit 1
  fi
  REQUIRED_PATHS=(
    "Dockerfile"
    "backend/main.py"
    "backend/requirements.txt"
    "frontend/Dockerfile"
    "frontend/package.json"
    "frontend/package-lock.json"
    "frontend/nginx.conf"
    "frontend/index.html"
    "frontend/vite.config.js"
    "frontend/src/main.jsx"
  )
  MISSING=0
  for rel_path in "${REQUIRED_PATHS[@]}"; do
    if [ ! -e "${PROJECT_DIR}/${rel_path}" ]; then
      echo "缺少部署必需文件：${PROJECT_DIR}/${rel_path}"
      MISSING=1
    fi
  done
  if [ "$MISSING" -ne 0 ]; then
    echo "请确认已在项目根目录 ${PROJECT_DIR} 上传完整 MercuryMail 源码（含 frontend/ 与 backend/）"
    exit 1
  fi
  chmod -R a+rX "${PROJECT_DIR}/frontend" 2>/dev/null || true
  chmod a+r "${PROJECT_DIR}/frontend/Dockerfile" "${PROJECT_DIR}/frontend/nginx.conf" 2>/dev/null || true
  log "部署目录校验通过（项目根=${PROJECT_DIR}，前端 Dockerfile=${PROJECT_DIR}/frontend/Dockerfile）"
}

# 幂等部署：部署前强制清理旧容器、孤儿容器与冲突网络（保留 pgdata/redis_data 数据卷）
cleanup_before_deploy() {
  log "幂等部署：清理旧容器、孤儿容器与冲突网络"
  cd "$PROJECT_DIR"
  if ! ensure_compose_cli; then
    return 0
  fi
  if compose_file_exists; then
    # 不使用 --volumes，避免重复部署清空 PostgreSQL/Redis 命名卷（pgdata、redis_data）
    # 核心停机命令；不使用 --volumes，避免清空 PostgreSQL/Redis 数据卷。
    docker compose down --remove-orphans 2>/dev/null || true
    # 若需彻底清空数据卷可手动执行：docker compose down --remove-orphans --volumes
  fi
  # 强制删除可能残留的同项目容器，消除 container name already in use 冲突
  docker rm -f mercury-mail-app mercury-mail-frontend mercury-mail-db mercury-mail-redis 2>/dev/null || true
  # 删除项目专用网络，避免网络名占用
  docker network rm mercury-network 2>/dev/null || true
  docker network prune -f >/dev/null 2>&1 || true
}

# 启动容器前确保 .env 中 ADMIN_PASSWORD 已设置（未设置则自动生成随机密码）
ensure_admin_password_before_deploy() {
  if [ ! -f "$ENV_FILE" ]; then
    warn ".env 不存在，跳过 ADMIN_PASSWORD 自动补全"
    return 0
  fi
  load_env_values
  if [ -n "${ADMIN_PASSWORD:-}" ]; then
    ADMIN_PASSWORD_AUTO_GENERATED=0
    if [ "$(read_env_value_from_file RESET_ADMIN_PASSWORD)" = "true" ]; then
      log "检测到 .env 已配置 ADMIN_PASSWORD，并启用了 RESET_ADMIN_PASSWORD=true；后端启动时将重置普通管理员密码"
    else
      log "检测到 .env 已配置 ADMIN_PASSWORD；若普通管理员已存在，后端启动时不会覆盖现有登录密码"
    fi
    return 0
  fi
  NEW_PASS="$(generate_admin_password || true)"
  if [ -z "$NEW_PASS" ]; then
    warn "管理员密码随机生成失败，后端可能无法在启动时重置普通管理员密码"
    ADMIN_PASSWORD_AUTO_GENERATED=0
    return 0
  fi
  if update_env_line ADMIN_PASSWORD "$NEW_PASS"; then
    ADMIN_PASSWORD="$NEW_PASS"
    ADMIN_PASSWORD_AUTO_GENERATED=1
    chmod 600 "$ENV_FILE" 2>/dev/null || true
    log "ADMIN_PASSWORD 未设置，已自动生成随机密码并写入 .env；仅首次创建普通管理员或显式 RESET_ADMIN_PASSWORD=true 时生效"
  else
    warn "写入 ADMIN_PASSWORD 到 .env 失败"
    ADMIN_PASSWORD_AUTO_GENERATED=0
  fi
}

build_and_start() {
  log "构建并启动容器（全新安装自动新建数据库卷，重复部署自动复用或重建坏卷）"
  cd "$PROJECT_DIR"
  verify_deploy_layout
  FRESH_INSTALL=0
  if [ ! -f "$ENV_FILE" ]; then
    FRESH_INSTALL=1
    warn ".env 不存在，按全新安装处理：生成新配置并强制使用全新 PostgreSQL 数据卷"
    generate_initial_env_file || true
    complete_env_file
  fi
  ensure_admin_password_before_deploy
  ensure_pg_admin_password_in_env
  ensure_db_password_consistency
  if ! validate_critical_env_before_compose; then
    exit 1
  fi
  cleanup_before_deploy
  log "启动 PostgreSQL（保留现有数据卷）"
  compose_cmd up -d db
  if ! wait_for_db_container_healthy || ! wait_for_db_ready "$DB_ROLE_NAME" "$DB_NAME"; then
    warn_red "数据库启动失败；已停止，未删除数据卷。请检查日志。"
    exit 1
  fi
  if [ "$FRESH_INSTALL" = "1" ]; then
    verify_postgres_password_from_env || { warn_red "数据库密码验证失败；未删除数据卷。"; exit 1; }
  else
    if ! sync_database_password || ! verify_postgres_password_from_env; then
      warn_red "数据库密码同步失败；已停止，未删除数据卷。"
      exit 1
    fi
  fi
  log "正在初始化数据库表（由应用启动事件自动建表，请等待 mercury-mail-app 健康检查通过）…"
  log "构建并启动全部服务（APP_GIT_SHA=${APP_GIT_SHA}，APP_IMAGE_DIGEST=${APP_IMAGE_DIGEST}）"
  # 核心构建启动命令，--build 确保镜像包含最新代码。
  docker compose up -d --build --force-recreate
}

# 从后端容器启动日志中提取普通管理员密码（严格匹配，过滤 superadmin，绝不显示超级管理员密码）
extract_admin_password_from_logs() {
  PASS=""
  LOG_LINES=""
  cd "$PROJECT_DIR"
  LOG_LINES="$(compose_cmd logs --no-color "${BACKEND_COMPOSE_SERVICE}" 2>/dev/null || true)"
  if [ -z "$LOG_LINES" ]; then
    return 1
  fi
  # 英文日志：Admin password: xxxxxxxx（排除 superadmin 行）
  PASS="$(printf '%s\n' "$LOG_LINES" \
    | grep -iE 'Admin password:' \
    | grep -iv 'superadmin' \
    | tail -1 \
    | sed -n 's/.*[Aa]dmin[[:space:]]+[Pp]assword[[:space:]]*:[[:space:]]*\([^[:space:]]*\).*/\1/p' || true)"
  if [ -n "$PASS" ]; then
    printf '%s' "$PASS"
    return 0
  fi
  # 当前后端中文日志（user_seed.py）
  PASS="$(printf '%s\n' "$LOG_LINES" \
    | grep '管理员密码未设置，本次已生成随机密码：' \
    | grep -iv 'superadmin' \
    | tail -1 \
    | sed -n 's/.*管理员密码未设置，本次已生成随机密码：\([^，[:space:]]*\).*/\1/p' || true)"
  if [ -n "$PASS" ]; then
    printf '%s' "$PASS"
    return 0
  fi
  # 兼容旧版中文日志文案
  PASS="$(printf '%s\n' "$LOG_LINES" \
    | grep '普通管理员密码未设置，本次已生成随机密码：' \
    | grep -iv 'superadmin' \
    | tail -1 \
    | sed -n 's/.*普通管理员密码未设置，本次已生成随机密码：\([^，[:space:]]*\).*/\1/p' || true)"
  if [ -n "$PASS" ]; then
    printf '%s' "$PASS"
    return 0
  fi
  return 1
}

# 醒目输出管理员密码（不影响部署流程，仅作提示）
print_admin_password_banner() {
  PASS="$1"
  echo ""
  printf '\033[1;33m**************************************************\033[0m\n'
  printf '\033[1;32m\033[1m***          管理员密码：%s              ***\033[0m\n' "$PASS"
  printf '\033[1;33m**************************************************\033[0m\n'
  echo "请尽快使用用户名「${ADMIN_USERNAME:-admin}」登录系统并修改密码。"
  echo ""
}

# 若 .env 尚未设置 ADMIN_PASSWORD，则把日志提取到的随机密码写回 .env，便于后续重部署继续显示
persist_admin_password_if_needed() {
  PASS="$1"
  if [ -z "$PASS" ]; then
    return 0
  fi
  load_env_values
  if [ -n "${ADMIN_PASSWORD:-}" ]; then
    return 0
  fi
  if update_env_line ADMIN_PASSWORD "$PASS"; then
    ADMIN_PASSWORD="$PASS"
    chmod 600 "$ENV_FILE" 2>/dev/null || true
    echo "已将当前管理员密码写入 .env 的 ADMIN_PASSWORD，后续重部署将自动显示。"
  else
    warn "写入 ADMIN_PASSWORD 到 .env 失败，请手动将密码保存到 .env"
  fi
}

# 后端容器健康启动后，从 .env 读取并醒目显示管理员密码（日志抓取仅作兜底）
display_admin_password_hint() {
  log "读取管理员密码信息"
  cd "$PROJECT_DIR"
  PASS_FROM_LOG=""
  load_env_values

  if [ -n "${ADMIN_PASSWORD:-}" ]; then
    print_admin_password_banner "$ADMIN_PASSWORD"
    if [ "$(read_env_value_from_file RESET_ADMIN_PASSWORD)" = "true" ]; then
      echo "本次部署已启用 RESET_ADMIN_PASSWORD=true；后端将按 .env 中的 ADMIN_PASSWORD 重置普通管理员密码。"
    elif [ "${ADMIN_PASSWORD_AUTO_GENERATED:-0}" -eq 1 ]; then
      echo "本次部署已自动生成管理员密码并写入 .env；仅在首次创建普通管理员时会写入账户。"
    else
      echo "当前密码来自 .env 中的 ADMIN_PASSWORD；若普通管理员已存在，后端不会用它覆盖已修改的密码。"
    fi
    echo "密码已保存在：${ENV_FILE}"
    return 0
  fi

  PASS_FROM_LOG="$(extract_admin_password_from_logs 2>/dev/null || true)"
  if [ -z "$PASS_FROM_LOG" ]; then
    sleep 2
    PASS_FROM_LOG="$(extract_admin_password_from_logs 2>/dev/null || true)"
  fi
  if [ -n "$PASS_FROM_LOG" ]; then
    persist_admin_password_if_needed "$PASS_FROM_LOG"
    print_admin_password_banner "$PASS_FROM_LOG"
    echo "（兜底）已从容器日志提取密码，建议确认 .env 中 ADMIN_PASSWORD 是否已正确写入。"
    return 0
  fi

  echo ""
  warn "未能获取管理员密码：.env 中 ADMIN_PASSWORD 为空且容器日志中无匹配项。"
  echo "请检查 ${ENV_FILE} 并重新执行 bash deploy.sh"
  return 0
}

# 部署后快速验证：失败仅输出红色警告，不中止脚本（便于排查）
post_deploy_verify() {
  log "部署后快速验证"
  sleep 5
  FRONTEND_HTTP_PORT_VALUE="$(get_env_value FRONTEND_HTTP_PORT)"
  if [ -z "$FRONTEND_HTTP_PORT_VALUE" ]; then
    FRONTEND_HTTP_PORT_VALUE="${FRONTEND_PORT}"
  fi
  if [ "${FRONTEND_HTTP_PORT_VALUE}" = "80" ]; then
    FRONTEND_CHECK_URL="http://127.0.0.1/"
  else
    FRONTEND_CHECK_URL="http://127.0.0.1:${FRONTEND_HTTP_PORT_VALUE}/"
  fi
  if curl -f -sS "http://127.0.0.1:${APP_PORT}/api/health/live" >/dev/null 2>&1; then
    echo "后端健康检查：通过（http://127.0.0.1:${APP_PORT}/api/health/live）"
  else
    warn_red "后端健康检查失败，请执行：curl -f http://127.0.0.1:${APP_PORT}/api/health/live"
  fi
  if curl -f -sS "$FRONTEND_CHECK_URL" >/dev/null 2>&1; then
    echo "前端页面检查：通过（${FRONTEND_CHECK_URL}）"
  else
    warn_red "前端无法访问，请执行：curl -f ${FRONTEND_CHECK_URL}"
    warn_red "请检查 mercury-mail-frontend 是否运行、端口 ${FRONTEND_HTTP_PORT_VALUE} 是否被占用及云安全组是否放行"
  fi
  cd "$PROJECT_DIR"
  compose_cmd ps 2>/dev/null || true
}

wait_for_health() {
  log "冒烟测试：curl -f http://127.0.0.1:${APP_PORT}/api/health/live"
  for attempt in $(seq 1 60); do
    if curl -f -sS "http://127.0.0.1:${APP_PORT}/api/health/live" >/dev/null 2>&1; then
      echo "API 健康检查通过（第 ${attempt} 次尝试）"
      break
    fi
    if [ "$attempt" -eq 60 ]; then
      echo "API 健康检查失败，最近容器日志如下："
      cd "$PROJECT_DIR"
      compose_cmd logs --tail=120
      rollback_on_failure
      exit 1
    fi
    sleep 2
  done
  FRONTEND_HTTP_PORT_VALUE="$(get_env_value FRONTEND_HTTP_PORT)"
  if [ -z "$FRONTEND_HTTP_PORT_VALUE" ]; then
    FRONTEND_HTTP_PORT_VALUE="${FRONTEND_PORT}"
  fi
  log "冒烟测试：curl -f http://127.0.0.1:${FRONTEND_HTTP_PORT_VALUE}/"
  for attempt in $(seq 1 30); do
    if curl -f -sS "http://127.0.0.1:${FRONTEND_HTTP_PORT_VALUE}/" >/dev/null 2>&1; then
      echo "前端页面检查通过（第 ${attempt} 次尝试，端口 ${FRONTEND_HTTP_PORT_VALUE}）"
      break
    fi
    if [ "$attempt" -eq 30 ]; then
      echo "前端页面检查失败，请确认 mercury-mail-frontend 容器运行且端口 ${FRONTEND_HTTP_PORT_VALUE} 未被占用/拦截"
      cd "$PROJECT_DIR"
      compose_cmd logs mercury-mail-frontend --tail=120
      compose_cmd ps
      rollback_on_failure
      exit 1
    fi
    sleep 2
  done
  log "冒烟测试：curl -f http://127.0.0.1:${FRONTEND_HTTP_PORT_VALUE}/api/health/live（经 Nginx 反代）"
  for attempt in $(seq 1 15); do
    if curl -f -sS "http://127.0.0.1:${FRONTEND_HTTP_PORT_VALUE}/api/health/live" >/dev/null 2>&1; then
      echo "Nginx API 反代检查通过（第 ${attempt} 次尝试）"
      return 0
    fi
    sleep 2
  done
  echo "Nginx /api/health/live 反代失败，请检查 frontend/nginx.conf 上游是否为 ${BACKEND_COMPOSE_SERVICE}:${BACKEND_CONTAINER_PORT}"
  cd "$PROJECT_DIR"
  compose_cmd logs mercury-mail-frontend --tail=80
  rollback_on_failure
  exit 1
}

verify_containers() {
  log "验证容器状态与健康检查"
  cd "$PROJECT_DIR"
  compose_cmd ps
  FAILED=0
  verify_container_healthy "mercury-mail-db" "PostgreSQL" || FAILED=1
  verify_container_healthy "mercury-mail-redis" "Redis" || FAILED=1
  verify_container_healthy "mercury-mail-app" "后端 API" || FAILED=1
  verify_container_healthy "mercury-mail-frontend" "前端 Nginx" || FAILED=1
  if [ "$FAILED" -ne 0 ]; then
    echo "容器健康检查未全部通过，最近日志如下："
    compose_cmd logs --tail=120
    rollback_on_failure
    exit 1
  fi
  echo "全部容器运行正常且健康检查通过"
}

should_keep_deploy_path() {
  NAME="$1"
  case "$NAME" in
    .|..|.env|docker-compose.yml|deploy.sh|data|backups|.env.example|POST_REFACTOR_CHECKLIST.md)
      return 0
      ;;
  esac
  case "$NAME" in
    *.bak|*.bak.*|mercurymail_*.sql.gz)
      return 0
      ;;
  esac
  return 1
}

clean_source_after_deploy() {
  CLEAN_FLAG="$(get_env_value CLEAN_SOURCE_AFTER_DEPLOY)"
  if [ "${CLEAN_FLAG}" != "true" ]; then
    log "保留源代码目录（CLEAN_SOURCE_AFTER_DEPLOY=${CLEAN_FLAG:-false}）"
    return 0
  fi
  log "部署后清理源码（保留 .env、docker-compose.yml、data、backups、deploy.sh 及备份文件）"
  cd "$PROJECT_DIR"
  for entry in * .[^.]*; do
    [ "$entry" = "*" ] && continue
    [ "$entry" = ".[^.]*" ] && continue
    if should_keep_deploy_path "$entry"; then
      continue
    fi
    rm -rf "$entry"
  done
}

print_result() {
  load_env_values
  SERVER_IP="$(get_server_ip)"
  PUBLIC_BASE_URL_VALUE="$(get_env_value PUBLIC_BASE_URL)"
  if [ -z "$PUBLIC_BASE_URL_VALUE" ]; then
    PUBLIC_BASE_URL_VALUE="http://${SERVER_IP}"
  fi
  echo ""
  echo "MercuryMail 部署完成"
  FRONTEND_HTTP_PORT_VALUE="$(get_env_value FRONTEND_HTTP_PORT)"
  if [ -z "$FRONTEND_HTTP_PORT_VALUE" ]; then
    FRONTEND_HTTP_PORT_VALUE="${FRONTEND_PORT}"
  fi
  if [ "${FRONTEND_HTTP_PORT_VALUE}" = "80" ]; then
    echo "前端访问：http://${SERVER_IP}"
  else
    echo "前端访问：http://${SERVER_IP}:${FRONTEND_HTTP_PORT_VALUE}"
  fi
  echo "API 直连：http://${SERVER_IP}:${APP_PORT}/api/health/live"
  echo "API 经 Nginx：http://${SERVER_IP}$([ "${FRONTEND_HTTP_PORT_VALUE}" != "80" ] && echo ":${FRONTEND_HTTP_PORT_VALUE}")/api/health/live"
  echo "服务器 IP：${SERVER_IP}"
  echo "前端端口：${FRONTEND_HTTP_PORT_VALUE}，API 端口：${APP_PORT}"
  echo "后端 compose 服务名：${BACKEND_COMPOSE_SERVICE}（Nginx 上游 ${BACKEND_COMPOSE_SERVICE}:${BACKEND_CONTAINER_PORT}）"
  if [ -n "$ADMIN_PASSWORD" ]; then
    echo "管理员密码：${ADMIN_PASSWORD}"
  fi
  echo "环境文件：${ENV_FILE}"
  echo "或通过前端反代：${PUBLIC_BASE_URL_VALUE%/}/api/health/live"
  if [ -n "$ENV_BACKUP" ]; then
    echo "配置备份：${ENV_BACKUP}"
  fi
  if [ -n "$DB_BACKUP" ]; then
    echo "数据库备份：${DB_BACKUP}"
  fi
}

main() {
  require_root
  require_ubuntu_2204
  fix_project_crlf
  detect_domestic_network
  prepare_apt
  install_base_packages
  configure_system_nofile_limits
  install_docker_with_aliyun
  generate_initial_env_file || true
  complete_env_file
  load_env_values
  detect_admin_user_before_deploy
  backup_before_deploy
  build_and_start
  wait_for_health
  verify_containers
  post_deploy_verify
  display_admin_password_hint
  clean_source_after_deploy
  print_result
}

main "$@"
