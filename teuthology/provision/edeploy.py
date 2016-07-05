import logging
import os
import requests
import subprocess

log = logging.getLogger(__name__)

edeploy_host = 'edeploy.front.sepia.ceph.com'


class Edeploy(object):
    nextboot_url_templ = \
        'http://{edeploy}:83/nextboot/{host}/{profile}'
    ok_msg_templ = "Next boot for {host} is set to {profile}"

    def __init__(self, name, os_type, os_version):
        self.name = name
        self.os_type = os_type
        self.os_version = os_version

    @property
    def profile(self):
        # FIXME: ugh
        os_type = self.os_type.lower()
        os_version = self.os_version.replace('.', '')
        return os_type + os_version

    def create(self):
        url = self.nextboot_url_templ.format(
            edeploy=edeploy_host,
            host=self.name,
            profile=self.profile,
        )
        resp = requests.get(url)
        resp.raise_for_status()
        assert resp.text == self.ok_msg_templ.format(
            host=self.name,
            profile=self.profile,
        )
        return True

    def destroy(self):
        pass
