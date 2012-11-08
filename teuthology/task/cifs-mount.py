import contextlib
import logging
import os

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a cifs client.

    The config is optional and defaults to mounting on all clients. If
    a config is given, it is expected to be a list of clients to do
    this operation on.

    Example that starts smbd and mounts cifs on all nodes::

        tasks:
        - ceph:
        - samba:
        - cifs-client:
        - interactive:

    Example that splits smbd and cifs:

        tasks:
        - ceph:
        - samba: [samba.0]
        - cifs-client: [client.0]
        - ceph-fuse: [client.1]
        - interactive:
    """
    log.info('Mounting cifs clients...')
    cifs_daemons = {}

    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    clients = list(teuthology.get_clients(ctx=ctx, roles=config.keys()))

    for id_, remote in clients:
        mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
        log.info('Mounting cifs client.{id} at {remote} {mnt}...'.format(
                id=id_, remote=remote,mnt=mnt))

        client_config = config.get("client.%s" % id_)
        if client_config is None:
            client_config = {}
        log.info("Cifs client client.%s config is %s" % (id_, client_config))

        deamon_signal = 'kill'

        remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
                ],
            )

        proc = run_cmd=[
            'mount',
            '-t',
            'cifs',
            '-o',
            'username=ubuntu,password=ubuntu',
            '//{sambaip}/ceph1'.format(sambaip=ip),
            mnt,
            ]

    try:
        yield
    finally:
        log.info('Unmounting cifs clients...')
        for id_, remote in clients:
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            remote.run(
                args=[
                    'umount',
                    mnt,
                    ],
                )

        for id_, remote in clients:
            mnt = os.path.join('/tmp/cephtest', 'mnt.{id}'.format(id=id_))
            remote.run(
                args=[
                    'rmdir',
                    '--',
                    mnt,
                    ],
                )
