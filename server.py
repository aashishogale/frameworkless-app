from http.server import HTTPServer, BaseHTTPRequestHandler
from urls import urldict
import os


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        response = urldict.get(self.path or "")() if urldict.get(self.path) else None
        file_name = os.path.dirname(os.path.abspath(__file__)) + response
        with open(file_name, "rb") as f:
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(f.read())
        f.close()
        return

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        print(body)
        self.send_response(200)
        self.end_headers()


httpd = HTTPServer(('', 8080), SimpleHTTPRequestHandler)
httpd.serve_forever()
