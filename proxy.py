from flask import Flask, request, Response
import requests
import os

app = Flask(__name__)

# Replace this with your exact Vercel URL (Keep the https:// and NO trailing slash)
VERCEL_URL = "https://fitandset-site-git-demo-sriraj1998s-projects.vercel.app"

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST'])
@app.route('/<path:path>', methods=['GET', 'POST'])
def proxy(path):
    # 1. Construct the exact URL to hit on Vercel
    target_url = f"{VERCEL_URL}/{path}"
    
    # Re-attach the query parameters (e.g., ?SN=123&table=ATTLOG)
    if request.query_string:
        target_url += f"?{request.query_string.decode('utf-8')}"

    # 2. Forward the headers (We MUST remove the 'Host' header so Vercel accepts it)
    headers = {key: value for (key, value) in request.headers if key.lower() != 'host'}

    print(f">>> Forwarding {request.method} request to: {target_url}")

    try:
        # 3. Make the secure HTTPS request to Vercel
        vercel_response = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=request.get_data(),  # This securely forwards the raw text body
            allow_redirects=False
        )

        # 4. Filter out specific server-level headers before sending the response back to the machine
        excluded_headers =['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        response_headers =[
            (name, value) for (name, value) in vercel_response.raw.headers.items()
            if name.lower() not in excluded_headers
        ]

        # 5. Return Vercel's exact response (e.g., "OK") back to the eSSL machine
        return Response(vercel_response.content, vercel_response.status_code, response_headers)

    except Exception as e:
        print(f"❌ Proxy Error: {e}")
        # The machine strictly expects plain text, not HTML errors
        return Response("ERROR", status=500, content_type="text/plain")


if __name__ == '__main__':
    # Render provides the port automatically via the PORT environment variable
    port = int(os.environ.get('PORT', 10000))
    print(f"🚀 eSSL to Vercel Proxy running on Port {port}...")
    app.run(host='0.0.0.0', port=port)