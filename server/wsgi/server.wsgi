import sys

sys.path.insert(0, '/home/ubuntu/theft-market/server')
from WebServer import WebServer

server = WebServer('/home/ubuntu/theft-market')
_application = server.app

def application(environ, start_response):
    response_headers = [('Content-Type', 'text/plain')]
    return _application(environ, start_response)
