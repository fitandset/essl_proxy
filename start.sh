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

gunicorn --bind "0.0.0.0:${ESSL_PORT}" --workers 1 --threads 6 --timeout 120 proxy:app &
GUNICORN_PID=$!

# Restart WhatsApp Node only. Gunicorn/ESSL is not in this loop.
# On SIGTERM, Node must get the signal and finish exiting: it saves the WhatsApp
# encryption keys and hands the login over to the next deploy before it quits.
(
  export PORT="${WA_PORT}"
  export BASE_PATH="${BASE_PATH:-/wa}"
  cd /app/whatsapp
  stopping=0
  node_pid=""
  trap 'stopping=1; if [ -n "$node_pid" ]; then kill -TERM "$node_pid" 2>/dev/null || true; fi' TERM
  while [ "$stopping" -eq 0 ]; do
    echo "start.sh: starting WhatsApp Node" >&2
    node dist/index.js &
    node_pid=$!
    status=0
    wait "$node_pid" || status=$?
    if [ "$stopping" -eq 1 ]; then
      # The first wait returns as soon as the trap runs, not when Node exits.
      wait "$node_pid" 2>/dev/null || true
      break
    fi
    echo "start.sh: WhatsApp Node exited (${status}); restarting in 5s" >&2
    sleep 5
  done
) &
WA_PID=$!

nginx -g "daemon off;" &
NGINX_PID=$!

stop_children() {
  trap '' TERM INT
  kill -TERM "$WA_PID" "$GUNICORN_PID" 2>/dev/null || true
  wait "$WA_PID" 2>/dev/null || true
  wait "$GUNICORN_PID" 2>/dev/null || true
  kill -TERM "$NGINX_PID" 2>/dev/null || true
  wait "$NGINX_PID" 2>/dev/null || true
}

trap 'echo "start.sh: stopping" >&2; stop_children; exit 0' TERM INT

status=0
wait "$NGINX_PID" || status=$?
echo "start.sh: nginx exited (${status})" >&2
stop_children
exit "$status"
