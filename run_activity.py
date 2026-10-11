from myota_logging import configure_logging

configure_logging("myota-activity", component="api")

from common import BoundedThreadingHTTPServer
from activity import ActivityHandler

BoundedThreadingHTTPServer(("0.0.0.0", 8004), ActivityHandler).serve_forever()
