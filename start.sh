#!/bin/sh
set -eu

# Render sets PORT (often 10000) — nginx must listen there.
# Gunicorn and Node must use different internal ports or they collide.
PUBLIC_PORT="${PORT:-8080}"
ESSL_PORT="${ESSL_PORT:-10001}"
WA_PORT="${WA_PORT:-3001}"

if [ "$ESSL_PORT" = "$PUBLIC_PORT" ]; then
  if [ "$PUBLIC_PORT" = "10001" ]; then
    ESSL_PORT=10002
  else
    ESSL_PORT=10001
  fi
  echo "start.sh: ESSL_PORT collided with PORT; using ${ESSL_PORT}" >&2
fi

if [ "$WA_PORT" = "$PUBLIC_PORT" ] || [ "$WA_PORT" = "$ESSL_PORT" ]; then
  WA_PORT=3001
  if [ "$WA_PORT" = "$PUBLIC_PORT" ] || [ "$WA_PORT" = "$ESSL_PORT" ]; then
    WA_PORT=3002
  fi
  echo "start.sh: WA_PORT collided; using ${WA_PORT}" >&2
fi

cat > /etc/nginx/conf.d/default.conf <<EOF
server {
    listen ${PUBLIC_PORT};
    server_name _;
    client_max_body_size 50m;

    location /wa/ {
        proxy_pass http://127.0.0.1:${WA_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 120s;
        proxy_send_timeout 120s;
    }

    location / {
        proxy_pass http://127.0.0.1:${ESSL_PORT};
        proxy_http_version 1.1;
        proxy_set_header Host \$host;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_read_timeout 120s;
        proxy_send_timeout 120s;
    }
}
EOF

gunicorn --bind "0.0.0.0:${ESSL_PORT}" --workers 1 --threads 4 --timeout 120 proxy:app &

export PORT="${WA_PORT}"
export BASE_PATH="${BASE_PATH:-/wa}"
cd /app/whatsapp
node dist/index.js &

nginx -g "daemon off;"
