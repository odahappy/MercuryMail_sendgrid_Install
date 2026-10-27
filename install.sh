#!/usr/bin/env bash
set -euo pipefail
# Usage: sudo bash install.sh https://github.com/OWNER/REPOSITORY.git
repo="${1:-}"
target="${MERCURY_INSTALL_DIR:-/opt/MercuryMail}"
if [ "$(id -u)" -ne 0 ]; then echo '请使用 sudo 运行安装脚本。' >&2; exit 1; fi
if [[ ! "$repo" =~ ^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(\.git)?/?$ ]]; then
  echo '请提供完整 GitHub 仓库地址，例如 https://github.com/用户名/仓库名.git' >&2; exit 1
fi
if [ ! -f /etc/os-release ] || ! grep -q '^ID=ubuntu$' /etc/os-release; then
  echo '此安装脚本仅支持 Ubuntu；推荐使用 Ubuntu 22.04。' >&2; exit 1
fi
if [ -e "$target" ]; then
  echo "安装目录已存在：$target。为保护现有数据，安装已停止。" >&2
  echo '更新请参照 README_GITHUB_INSTALL.md，不要删除该目录。' >&2
  exit 1
fi
if ! command -v git >/dev/null 2>&1; then
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y git ca-certificates
fi
mkdir -p "$(dirname "$target")"
git clone -- "$repo" "$target"
if [ ! -f "$target/deploy.sh" ] || [ ! -f "$target/docker-compose.yml" ] || [ ! -d "$target/backend" ]; then
  echo "仓库不是完整 MercuryMail 项目；安装停止。请检查 $target" >&2
  exit 1
fi
cd "$target"
bash deploy.sh
