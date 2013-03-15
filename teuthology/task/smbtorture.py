import contextlib
import logging
import os

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

def install(ctx, sambas):
    log.info("installing samba-tools to get smbtorture")
    procs = {}
    for id_, remote in sambas:
        proc = remote.run(
            args = [
                'sudo', 'apt-get', 'install', 'samba-tools', '-y', '--force-yes'
                ],
            wait=False
            )
        procs[remote.name] = proc

    for name, proc in procs.iteritems():
        log.debug("Waiting for samba-tools install to finish on {name}".format(name=name))
        proc.exitstatus.get() # FIXME: this assumes success and doesn't check

def uninstall(ctx, sambas):
    procs = {}
    for id_, remote in sambas:
        proc = remote.run(
            args = [
                'sudo', 'apt-get', 'purge', 'samba-tools', '-y', '--force-yes'
                ],
            wait=False
            )
        procs[remote.name] = proc

    for name, proc in procs.iteritems():
        log.debug("Waiting for samba-tools uninstall to finish on {name}".format(name=name))
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
    Run smbtorture on the specified node, against the "ceph" share, using the
    ubuntu/ubuntu username/password. By default it runs on all nodes with the
    "samba" role. FIXME: This may not work out right if there are multiple sambas; I
    believe they'll collide over the naming. But if we run synchronously it might
    go okay. For the future, optionally set up each smbd in a different dir?
    """
    
    log.info("Setting up smbtorture...")
    assert config is None or isinstance(config, list), \
        "task smbtorture got invalid config"

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
        remote.run(
            args=[
                'smbtorture', '\\\\localhost\ceph', '-U', 'ubuntu%ubuntu'
                ]
            )
    try:
        yield
    finally:
        log.info("Uninstalling smbtorture...")
        uninstall(ctx, sambas)
