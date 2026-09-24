from http.server import ThreadingHTTPServer
from activity import ActivityHandler

ThreadingHTTPServer(("0.0.0.0", 8004), ActivityHandler).serve_forever()
