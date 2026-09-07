#!/usr/bin/env python3
# Copyright 2026 RisingWave Labs
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Mock HTTP server for testing the RisingWave HTTP sink.

Usage:
    python3 http_sink_mock_server.py <body_output_file> <port> [<header_output_file>] [<path_output_file>] [<method_output_file>]

Each handled request body is appended as a line to the body output file.
If a header output file is given, received headers are appended as a JSON line per request.
If a path output file is given, each request path is appended as a line per request.
If a method output file is given, each request method is appended as a line per request.
Responds 200 OK to every handled request.
"""

import json
import signal
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


def main():
    body_output_file = sys.argv[1] if len(sys.argv) > 1 else "/tmp/http_sink_test_body.txt"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 18081
    header_output_file = sys.argv[3] if len(sys.argv) > 3 else None
    path_output_file = sys.argv[4] if len(sys.argv) > 4 and sys.argv[4] else None
    method_output_file = sys.argv[5] if len(sys.argv) > 5 and sys.argv[5] else None

    class Handler(BaseHTTPRequestHandler):
        def _handle_request(self):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode("utf-8")
            with open(body_output_file, "a") as f:
                f.write(body + "\n")
            if path_output_file is not None:
                with open(path_output_file, "a") as f:
                    f.write(self.path + "\n")
            if method_output_file is not None:
                with open(method_output_file, "a") as f:
                    f.write(self.command + "\n")
            if header_output_file is not None:
                headers = {k.lower(): v for k, v in self.headers.items()}
                with open(header_output_file, "a") as f:
                    f.write(json.dumps(headers) + "\n")
            self.send_response(200)
            self.end_headers()

        def do_POST(self):
            self._handle_request()

        def do_PUT(self):
            self._handle_request()

        def do_PATCH(self):
            self._handle_request()

        def do_DELETE(self):
            self._handle_request()

        def do_PROPFIND(self):
            self._handle_request()

        def do_GET(self):
            # Health check endpoint
            self.send_response(200)
            self.end_headers()

        def log_message(self, format, *args):
            pass  # suppress request logs

    server = HTTPServer(("", port), Handler)

    def handle_sigterm(signum, frame):
        server.server_close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_sigterm)

    server.serve_forever()


if __name__ == "__main__":
    main()
