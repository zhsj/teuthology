#!/usr/bin/env python

import os
import sys
import web

abspath = os.path.dirname(__file__)
if abspath not in sys.path:
    sys.path.append(abspath)

from api import Lock, MachineLock, MachineAdd

urls = (
    '/lock', 'Lock',
    '/add', 'MachineAdd',
    '/lock/(.*)', 'MachineLock',
    )

if __name__ == "__main__":
    app = web.application(urls, globals())
    application = app.wsgifunc()
    app.run()
