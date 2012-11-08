import contextlib
import logging
import os

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

def download_deb(ctx, sambas):
    procs = {}
    for (role, remote) in sambas:
        log.info('role: {r}'.format(r=role))
        sha1, deb_url = teuthology.get_ceph_binary_url(
            package='samba',
            branch='ceph',
            format='deb',
            flavor='basic',
            arch='x86_64',
            dist='precise',
            )

        log.info('Downloading samba deb {sha1} on {role}...'.format(sha1=sha1, role=role))
        log.info('fetching samba deb from {url}'.format(url=deb_url))
        proc = remote.run(
            args=[
                'sudo', 'rm', '-f', '/tmp/samba.deb',
                run.Raw('&&'),
                'echo',
                'samba_4.1.0pre1-GIT-{sha1short}_amd64.deb'.format(sha1short=sha1[0:7]),
                run.Raw('|'),
                'wget',
                '-nv',
                '-O',
                '/tmp/samba.deb',
                '--base={url}'.format(url=deb_url),
                '--input-file=-',
                ],
            wait=False)
        procs[remote.name] = proc

    for name, proc in procs.iteritems():
        log.debug('Waiting for download/copy to %s to complete...', name)
        proc.exitstatus.get()

def install(ctx, sambas):
    procs = {}
    for (role, remote) in sambas:
        log.info('Installing samba on {role}...'.format(role=role))
        proc = remote.run(
            args=[
                'sudo',
                'dpkg',
                '-i',
                '/tmp/samba.deb',
                ],
            wait=False,
            )
        procs[remote.name] = proc

    for name, proc in procs.iteritems():
        log.debug('Waiting for install on %s to complete...', name)
        proc.exitstatus.get()

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

    download_deb(ctx, sambas)
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
