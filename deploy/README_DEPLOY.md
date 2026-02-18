# Virtuals-Launch-Hunter Deployment Guide (Backend on Server, Frontend on Domain)

This guide covers two production patterns:

1. Split-domain (recommended):
   - Frontend: `https://app.example.com`
   - API: `https://api.example.com`
2. Same-domain with API prefix:
   - Frontend: `https://app.example.com`
   - API via reverse proxy path: `https://app.example.com/api/*`

The project now supports:
- Configurable frontend API base (meta tag `vpulse-api-base` in `dashboard.html`)
- Configurable backend CORS allowlist (`CORS_ALLOW_ORIGINS` in `config.json`)

## 0. One-click installer (Ubuntu 22.04, same-domain mode)

For domain `vps.licheng.website` style deployment:

```bash
cd /opt/vpulse
chmod +x deploy/install_ubuntu22_oneclick.sh
DOMAIN=vps.licheng.website \
LETSENCRYPT_EMAIL=you@example.com \
WS_RPC_URL=wss://... \
HTTP_RPC_URL=https://... \
BACKFILL_HTTP_RPC_URL=https://... \
bash deploy/install_ubuntu22_oneclick.sh
```

The script will:
- install system packages and Python deps
- generate/update `config.json`
- set frontend API base to `/api`
- install `systemd` services (`writer/realtime/backfill`)
- install Nginx same-domain reverse proxy
- try HTTPS via Certbot (if `LETSENCRYPT_EMAIL` provided)

## 1. Server prerequisites

Example target: Ubuntu 22.04+

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip nginx
```

Open firewall ports (if enabled):
- `22` (SSH)
- `80`, `443` (HTTP/HTTPS)

## 2. Project layout on server

Recommended paths:
- App code: `/opt/vpulse`
- Frontend static files: `/var/www/vpulse`

```bash
sudo mkdir -p /opt/vpulse /var/www/vpulse
sudo chown -R $USER:$USER /opt/vpulse /var/www/vpulse
```

Upload or clone this repository into `/opt/vpulse`.

## 3. Python environment

```bash
cd /opt/vpulse
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

## 4. Backend config (`/opt/vpulse/config.json`)

Start from template:

```bash
cp /opt/vpulse/config.example.json /opt/vpulse/config.json
```

Edit required RPC/project values first, then set deployment-related fields:

- Split-domain:
  - `API_HOST`: `"127.0.0.1"`
  - `API_PORT`: `8080`
  - `CORS_ALLOW_ORIGINS`: `["https://app.example.com"]`
- Same-domain with `/api` reverse proxy:
  - `CORS_ALLOW_ORIGINS`: `[]` (same-origin, no CORS needed)

## 5. Backend services (systemd)

Use the template unit:
- `deploy/systemd/vpulse@.service`

Install:

```bash
sudo cp /opt/vpulse/deploy/systemd/vpulse@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now vpulse@writer vpulse@realtime vpulse@backfill
```

Check status/logs:

```bash
sudo systemctl status vpulse@writer
sudo systemctl status vpulse@realtime
sudo systemctl status vpulse@backfill
sudo journalctl -u vpulse@writer -f
```

## 6. Frontend files

Copy frontend assets:

```bash
cp /opt/vpulse/dashboard.html /var/www/vpulse/dashboard.html
cp /opt/vpulse/favicon-vpulse.svg /var/www/vpulse/favicon-vpulse.svg
```

Set API base in `dashboard.html` by editing:

```html
<meta name="vpulse-api-base" content="" />
```

Use:
- Split-domain: `content="https://api.example.com"`
- Same-domain `/api`: `content="/api"`
- Full same-origin direct API: keep empty `content=""`

## 7. Nginx configuration

Choose one pattern.

### Option A: Split-domain

Use:
- `deploy/nginx/vpulse-split-app.conf`
- `deploy/nginx/vpulse-split-api.conf`

Install:

```bash
sudo cp /opt/vpulse/deploy/nginx/vpulse-split-app.conf /etc/nginx/sites-available/vpulse-split-app.conf
sudo cp /opt/vpulse/deploy/nginx/vpulse-split-api.conf /etc/nginx/sites-available/vpulse-split-api.conf
sudo ln -s /etc/nginx/sites-available/vpulse-split-app.conf /etc/nginx/sites-enabled/
sudo ln -s /etc/nginx/sites-available/vpulse-split-api.conf /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

### Option B: Same-domain with `/api`

Use:
- `deploy/nginx/vpulse-same-domain.conf`

Install:

```bash
sudo cp /opt/vpulse/deploy/nginx/vpulse-same-domain.conf /etc/nginx/sites-available/vpulse-same-domain.conf
sudo ln -s /etc/nginx/sites-available/vpulse-same-domain.conf /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl reload nginx
```

## 8. HTTPS (recommended)

With Certbot (example):

```bash
sudo apt install -y certbot python3-certbot-nginx
sudo certbot --nginx -d app.example.com
sudo certbot --nginx -d api.example.com
```

For same-domain, only request the single domain certificate.

## 9. Validation checklist

1. API health:
   - Split-domain: `curl https://api.example.com/health`
   - Same-domain: `curl https://app.example.com/api/health`
2. UI opens:
   - `https://app.example.com/`
3. `writer/realtime/backfill` all active in systemd.
4. UI can load `/meta`, `/minutes`, `/leaderboard`, `/project-tax`.

## 10. Upgrade process

```bash
cd /opt/vpulse
git pull
source .venv/bin/activate
pip install -r requirements.txt
sudo systemctl restart vpulse@writer vpulse@realtime vpulse@backfill
cp /opt/vpulse/dashboard.html /var/www/vpulse/dashboard.html
sudo systemctl reload nginx
```
