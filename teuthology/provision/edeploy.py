import logging
import os
import requests
import subprocess
import time

from ..contextutil import safe_while
from ..orchestra import remote

log = logging.getLogger(__name__)

edeploy_host = 'edeploy.front.sepia.ceph.com'


class Edeploy(object):
    nextboot_url_templ = \
        'http://{edeploy}:83/nextboot/{host}/{profile}'
    ok_msg_templ = "Next boot for {host} is set to {profile}"

    def __init__(self, name, os_type, os_version):
        self.name = name
        self.remote = remote.Remote(self.name)
        self.os_type = os_type
        self.os_version = os_version

    @property
    def profile(self):
        # FIXME: ugh
        os_type = self.os_type.lower()
        os_version = self.os_version.replace('.', '')
        return os_type + os_version

    def _wait_for_online(self):
        time.sleep(10)
        with safe_while(sleep=10, tries=60) as proceed:
            while proceed():
                if self.remote.is_online or self.remote.reconnect():
                    return True

    def create(self):
        url = self.nextboot_url_templ.format(
            edeploy=edeploy_host,
            host=self.name,
            profile=self.profile,
        )
        # Tell edeploy to reimage the machine
        resp = requests.get(url)
        resp.raise_for_status()
        assert resp.text == self.ok_msg_templ.format(
            host=self.name,
            profile=self.profile,
        )
        # Reboot the machine
        self.remote.console.power_cycle(wait=False)
        return self._wait_for_online()

    def destroy(self):
        pass
