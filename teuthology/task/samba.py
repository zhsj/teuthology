import contextlib
import logging
import os

from teuthology import misc as teuthology
from ..orchestra import run
import install as package_installs

log = logging.getLogger(__name__)

def install(ctx, sambas):
    package_installs.install_debs(ctx, ["samba"], "samba", dict(branch="ceph"))

def get_sambas(ctx, roles):
    for role in roles:
        assert isinstance(role, basestring)
        PREFIX = 'samba.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        yield (id_, remote)

@contextlib.contextmanager
def task(ctx, config):
    """
    Setup samba smbd with ceph vfs module.

    The config is optional and defaults to starting samba on all nodes.
    If a config is given, it is expected to be a list of
    samba nodes to start smbd servers on.

    Example that starts smbd on all samba nodes::

        tasks:
        - samba:
        - interactive:

    Example that starts smbd on just one of the samba nodes and cifs on the other::

        tasks:
        - samba: [samba.0]
        - cifs: [samba.1]

    """
    log.info("Setting up smbd with ceph vfs...")
    assert config is None or isinstance(config, list), \
        "task samba got invalid config"

    if config is None:
        config = ['samba.{id}'.format(id=id_)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'samba')]
    sambas = [('samba.{id}'.format(id=id_), remote) for (id_, remote) in get_sambas(ctx=ctx, roles=config)]

    install(ctx, sambas)

    for id_, remote in sambas:
        # generate samba config
        teuthology.write_file(remote, "/usr/local/etc/smb.conf", """
[global]
  workgroup = WORKGROUP
  netbios name = DOMAIN

[ceph]
  path = /
  vfs objects = ceph
  ceph:config_file = /tmp/cephtest/ceph.conf
  writeable = yes
""")

        # generate upstart
        teuthology.write_file(remote, "/etc/init/smbd.conf", """
description "SMB/CIFS File Server"

start on runlevel 5
stop on runlevel [!2345]

respawn

pre-start script
    RUN_MODE="daemons"

    install -o root -g root -m 755 -d /var/run/samba
end script

exec smbd -F
""")

        # start smbd
        proc = remote.run(
            args=[
                'start smbd',
            ])

        # create ubuntu user
        remote.run(
            args=[
                'printf "ubuntu\nubuntu\n"',
                run.Raw('|'),
                'sudo',
                'smbpasswd -s -a ubuntu'
            ])
    try:
        yield
    finally:
        log.info('Stopping smbd processes...')
        for id_, remote in samba_servers:
            log.debug('Stopping smbd on samba.{id}...'.format(id=id_))
            remote.run(
                args=[
                    'stop smbd',
                ])
