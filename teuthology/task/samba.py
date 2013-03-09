import contextlib
import logging
import os

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

def install(ctx, sambas):
    procs = {}
    for id_, remote in sambas:
        proc = remote.run(
            args = [
                'sudo', 'apt-get', 'install', 'samba', '-y', '--force-yes'
                ],
            wait=False
            )
        procs[remote.name] = proc

    for name, proc in procs.iteritems():
        log.debug("Waiting for samba install to finish on {name}".format(name=name))
        proc.exitstatus.get() # FIXME: this assumes success and doesn't check
            
def uninstall(ctx, sambas):
    procs = {}
    for id_, remote in sambas:
        proc = remote.run(
            args = [
                'sudo', 'apt-get', 'purge', 'samba', '-y', '--force-yes'
                ],
            wait=False
            )
        procs[remote.name] = proc

    for name, proc in procs.iteritems():
        log.debug("Waiting for samba uninstall to finish on {name}".format(name=name))
        proc.exitstatus.get() # FIXME: this assumes success and doesn't check

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

    The config is optional and defaults to starting samba on all nodes with the
    "samba" role listed in their node config.
    If a config is given, it is expected to be a list of
    samba roles to start smbd servers on.

    Example that starts smbd on all samba nodes::

        tasks:
        - samba:
        - interactive:

    Example that starts smbd on just one of the samba nodes and cifs on the other::

        tasks:
        - samba: [samba.0]
        - cifs: [samba.1]

    """
    log.info("Setting up smbd...")
    assert config is None or isinstance(config, list), \
        "task samba got invalid config"

    if config is None:
        config = ['samba.{id}'.format(id=id_)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'samba')]
    sambas = [('{id}'.format(id=id_), remote) for (id_, remote) in get_sambas(ctx=ctx, roles=config)]

    install(ctx, sambas)

    testdir = teuthology.get_testdir(ctx)

    for id_, remote in sambas:
        # generate samba config
        # FIXME: this assumes the id is the same for samba and client. We
        # should try and grep out the right mountpoint instead, or store
        # it somewhere for querying
        cephmnt = os.path.join(testdir, 'mnt.{id}'.format(id=id_))
        teuthology.sudo_write_file(remote, "/etc/samba/smb.conf", """
[global]
  workgroup = WORKGROUP
  netbios name = DOMAIN

[ceph]
  path = {cephmnt}
  browseable = yes
  writeable = yes
""".format(cephmnt=cephmnt))

        # start smbd
        proc = remote.run(
            args=[
                'sudo', 'restart', 'smbd',
                ])
        
        # create ubuntu user
        remote.run(
            args=[
                'printf', 'ubuntu\nubuntu\n',
                run.Raw('|'),
                'sudo',
                'smbpasswd', '-s', '-a', 'ubuntu'
                ])
    try:
        yield
    finally:
        log.info('Stopping smbd processes...')
        for id_, remote in sambas:
            log.debug('Stopping smbd on samba.{id}...'.format(id=id_))
            remote.run(
                args=[
                    'sudo', 'stop', 'smbd',
                    run.Raw('||'), 'true'
                    ])
        log.info('Uninstalling samba from all nodes...')
        uninstall(ctx, sambas)
