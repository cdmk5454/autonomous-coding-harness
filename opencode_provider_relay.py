"""Credential-free container relay and narrowly scoped host provider forwarding."""
from __future__ import annotations

import base64
import hashlib
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import sys
import urllib.error
import urllib.request

MODEL = "deepseek-v4.1-flash"
ROUTE = "/zen/go/v1/chat/completions"
MAX_BODY = 16_000_000
MAX_RESPONSE = 32_000_000


def validate_request(request):
    if request.get("method") != "POST" or request.get("path") != ROUTE:
        raise ValueError("PROVIDER_ROUTE_REJECTED")
    encoded = request.get("body", "")
    if not isinstance(encoded, str) or len(encoded) > (MAX_BODY * 4 // 3 + 4):
        raise ValueError("PROVIDER_BODY_REJECTED")
    raw = base64.b64decode(encoded, validate=True)
    if not raw or len(raw) > MAX_BODY:
        raise ValueError("PROVIDER_BODY_REJECTED")
    payload = json.loads(raw)
    if not isinstance(payload, dict) or payload.get("model") != MODEL:
        raise ValueError("PROVIDER_MODEL_REJECTED")
    return raw


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def forward(request, credential_loader):
    """Only this host function sees credentials; no remote error body is retained."""
    raw = validate_request(request)
    key = credential_loader()
    if not isinstance(key, str) or not key or "\r" in key or "\n" in key:
        raise ValueError("PROVIDER_CREDENTIAL_UNAVAILABLE")
    headers = {}
    for name, value in request.get("headers", {}).items():
        lower = str(name).lower()
        if lower in {"user-agent", "content-type", "accept"} or lower.startswith("x-opencode-"):
            if not isinstance(value, str) or len(value) > 4096 or "\r" in value or "\n" in value:
                raise ValueError("PROVIDER_HEADER_REJECTED")
            headers[name] = value
    headers["Authorization"] = "Bearer " + key
    headers["Content-Type"] = "application/json"
    req = urllib.request.Request(
        "https://opencode.ai" + ROUTE, data=raw, method="POST",
        headers=headers,
    )
    # Environment proxy configuration must not receive the credential either.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    with opener.open(req, timeout=90) as response:
        data = response.read(MAX_RESPONSE + 1)
        if len(data) > MAX_RESPONSE:
            raise ValueError("PROVIDER_RESPONSE_TOO_LARGE")
        content_type = response.headers.get("Content-Type", "application/json")
        if content_type.split(";", 1)[0] not in {"application/json", "text/event-stream"}:
            raise ValueError("PROVIDER_CONTENT_TYPE_REJECTED")
        result = {"status": response.status, "type": content_type,
                  "body": base64.b64encode(data).decode("ascii")}
    receipt = {"provider": "opencode-go", "model": MODEL, "status": result["status"],
               "request_sha256": hashlib.sha256(raw).hexdigest()}
    return result, receipt


def serve():
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.connection.settimeout(100)
            try:
                if self.path != ROUTE or self.headers.get("Transfer-Encoding"):
                    raise ValueError()
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= MAX_BODY:
                    raise ValueError()
                body = self.rfile.read(size)
                if len(body) != size:
                    raise ValueError()
                request = {"method": "POST", "path": self.path,
                           "body": base64.b64encode(body).decode("ascii"),
                           "headers": {k: v for k, v in self.headers.items()
                                       if k.lower() in {"user-agent", "content-type", "accept"}
                                       or k.lower().startswith("x-opencode-")}}
                validate_request(request)
                print(json.dumps(request), flush=True)
                line = sys.stdin.readline(MAX_RESPONSE * 2)
                result = json.loads(line)
                data = base64.b64decode(result["body"], validate=True)
                self.send_response(result["status"])
                self.send_header("Content-Type", result["type"])
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except Exception:
                self.send_error(502, "BOUNDARY_PROVIDER_REJECTED")

    server = HTTPServer(("127.0.0.1", 18699), Handler)
    print("RELAY_READY", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    serve()
