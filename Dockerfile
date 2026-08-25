FROM python:3.12-slim-bookworm

RUN apt-get update \
  && apt-get install -y --no-install-recommends nginx curl ca-certificates xz-utils \
  && curl -fsSL https://nodejs.org/dist/v22.16.0/node-v22.16.0-linux-x64.tar.xz \
    | tar -xJ -C /usr/local --strip-components=1 \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY proxy.py .

COPY whatsapp/package.json whatsapp/package-lock.json /app/whatsapp/
WORKDIR /app/whatsapp
RUN npm ci
COPY whatsapp/ /app/whatsapp/
RUN npm run build && npm prune --omit=dev

WORKDIR /app
COPY start.sh /start.sh
RUN chmod +x /start.sh \
  && rm -f /etc/nginx/sites-enabled/default

EXPOSE 8080

CMD ["/start.sh"]
