from flask import Flask, request, Response
import requests
import os

app = Flask(__name__)

# 1. Dynamically get the Vercel URL from Render Environment Variables
# In Render Dashboard: Set VERCEL_URL to https://your-app.vercel.app (No trailing slash)
VERCEL_URL = os.environ.get('VERCEL_URL', 'https://fitandset-site-git-demo-sriraj1998s-projects.vercel.app').rstrip('/')

# strict_slashes=False prevents Flask from redirecting /iclock/cdata to /iclock/cdata/
@app.route('/', defaults={'path': ''}, methods=['GET', 'POST'], strict_slashes=False)
@app.route('/<path:path>', methods=['GET', 'POST'], strict_slashes=False)
def proxy(path):
    # 1. Construct the exact URL to hit on Vercel
    # We use path.strip() to ensure no accidental double slashes
    target_url = f"{VERCEL_URL}/{path}"
    
    if request.query_string:
        target_url += f"?{request.query_string.decode('utf-8')}"

    # 2. Prepare Headers
    # Remove 'Host' so Vercel doesn't reject the request
    headers = {key: value for (key, value) in request.headers if key.lower() != 'host'}
    
    # 3. BYPASS VERCEL SECURITY CHECKPOINT: Add a Browser User-Agent
    # This tricks Vercel into thinking the request is from a human on a Mac
    headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

    print(f">>> [{request.method}] Forwarding to: {target_url}")
    if request.method == 'POST':
        print(f"    Payload Size: {len(request.get_data())} bytes")

    try:
        # 4. Make the secure HTTPS request to Vercel
        # we use allow_redirects=True but it's better to hit the exact URL to avoid POST->GET
        vercel_response = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=request.get_data(), 
            allow_redirects=True,
            timeout=30 # Prevent the proxy from hanging
        )

        # 5. Filter out restricted server-level headers
        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers = [
            (name, value) for (name, value) in vercel_response.raw.headers.items()
            if name.lower() not in excluded_headers
        ]

        # 6. Return Vercel's response back to the eSSL machine
        return Response(vercel_response.content, vercel_response.status_code, response_headers)

    except Exception as e:
        print(f"❌ Proxy Error: {e}")
        return Response("ERROR", status=500, content_type="text/plain")


if __name__ == '__main__':
    # Render provides the port automatically via the PORT environment variable
    port = int(os.environ.get('PORT', 10000))
    print(f"🚀 Proxying to: {VERCEL_URL}")
    print(f"🚀 eSSL to Vercel Proxy running on Port {port}...")
    app.run(host='0.0.0.0', port=port)