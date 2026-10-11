from activity import ActivityHandler
from common import BoundedThreadingHTTPServer
from myota_logging import configure_logging

configure_logging("myota-activity", component="api")

BoundedThreadingHTTPServer(("0.0.0.0", 8004), ActivityHandler).serve_forever()
