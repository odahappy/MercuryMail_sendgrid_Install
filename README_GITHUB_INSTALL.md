# GitHub 安装说明

本版本适用于 Ubuntu 22.04 全新服务器，安装需要 root 权限、GitHub 和 Docker 软件源网络访问。建议使用独立服务器。Ubuntu 24.04 未做实际部署验证。先上传本目录内全部文件（包括 .env.example、.gitignore）到你自己的 GitHub 仓库根目录，勿上传真实 .env、密钥或数据库。

## 安装

将下面两个地址中的 `用户名/仓库名` 和 `main` 替换为实际值。如果默认分支是 master，将 main 改为 master。

```bash
curl -fsSL https://raw.githubusercontent.com/用户名/仓库名/main/install.sh -o /tmp/mercurymail-install.sh && sudo bash /tmp/mercurymail-install.sh https://github.com/用户名/仓库名.git
```

需预先有 curl；私有仓库还需要自行配置 Git 认证，不要将访问令牌写进命令历史或 URL。脚本克隆仓库到 `/opt/MercuryMail`，目录已存在时停止，避免覆盖。安装失败后不会自动删除目录，检查日志后在该目录处理。

安装时生成 .env 和随机凭据，请安全保存终端显示的管理员密码。前端默认 HTTP 80 端口，后端 8000 仅监听本机。公网使用前应配置 HTTPS 反向代理、访问限制和防火墙；切勿在公网明文 HTTP 输入 API Key。SendGrid Key 当前仍明文存储在数据库中，请保护数据库与备份，使用最小权限 Key。

## 更新

更新前备份数据库和 .env；在 `/opt/MercuryMail` 执行 `sudo git status`，确认无本地源码改动，再执行：

```bash
cd /opt/MercuryMail
sudo git pull --ff-only
sudo bash deploy.sh
```

如果 Git 提示本地改动或冲突，停止并先处理，不要强制覆盖。更新不是自动事务；失败时查看容器日志并根据备份恢复。配置和数据卷不由 git pull 覆盖，脚本在数据库故障时停止，不自动删除数据卷。部署仍会重建本项目容器，服务会短暂中断。

## 日志

```bash
cd /opt/MercuryMail
sudo docker compose ps
sudo docker compose logs --tail=100
```

请勿公开 .env、管理员密码、数据库备份或 API Key。本版本仅经过静态及脚本语法检查，未进行真实云服务器部署测试。
