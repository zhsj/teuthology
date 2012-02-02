import contextlib
import logging
import sys

from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.task.ceph import CephState, ship_utilities
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run dd to loopback filesystems instead of ceph daemons. This is
    good for testing teuthology's daemon running infrastructure.
    To use this task, the loopback task must precede it, i.e.::

        tasks:
        - loopback:
        - fake_ceph:

    This task has no configuration.
    """
    assert config is None, 'fake_ceph takes no configuration'
    ctx.daemons = CephState(logger=log.getChild('CephState'))
    with contextutil.nested(
        lambda: ship_utilities(ctx=ctx, config=None),
        lambda: run_daemon(ctx=ctx, config=config, type_='mon'),
        lambda: run_daemon(ctx=ctx, config=config, type_='osd'),
        lambda: run_daemon(ctx=ctx, config=config, type_='mds'),
        ):
        yield

@contextlib.contextmanager
def run_daemon(ctx, config, type_):
    log.info('Starting %s daemons...', type_)
    daemons = ctx.cluster.only(teuthology.is_type(type_))
    for remote, roles_for_host in daemons.remotes.iteritems():
        for id_ in teuthology.roles_of_type(roles_for_host, type_):
            name = '%s.%s' % (type_, id_)
            ctx.daemons.add_daemon(
                remote, type_, id_,
                args=[
                    'sudo',
                    '/tmp/cephtest/daemon-helper', 'kill',
                    'dd',
                    'if=/dev/zero',
                    'of=/tmp/cephtest/mnt.{role}/out'.format(role=name),
                    ],
                logger=log.getChild('{type}.{id}'.format(type=type_, id=id_)),
                stdin=run.PIPE,
                wait=False,
                )
    try:
        yield
    finally:
        log.info('Shutting down %s daemons...', type_)
        exc_info = (None, None, None)
        for daemon in ctx.daemons.iter_daemons_of_role(type_):
            log.debug('shutting down daemon %s', daemon.proc)
            try:
                daemon.stop()
            except (run.CommandFailedError,
                    run.CommandCrashedError,
                    run.ConnectionLostError):
                exc_info = sys.exc_info()
                log.exception('Saw exception from %s.%s', daemon.role, daemon.id_)
        if type_ == 'mon':
            ctx.cluster.run(args=['sync'])

        if exc_info != (None, None, None):
            raise exc_info[0], exc_info[1], exc_info[2]
