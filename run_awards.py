from http.server import ThreadingHTTPServer

from awards import AwardsHandler


ThreadingHTTPServer(("0.0.0.0", 8005), AwardsHandler).serve_forever()
