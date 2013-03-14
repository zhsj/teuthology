import contextlib
import logging
import os

from teuthology import misc as teuthology
from ..orchestra import run

log = logging.getLogger(__name__)

def get_cifs(ctx, roles):
    for role in roles:
        assert isinstance(role, basestring)
        PREFIX = 'cifs-mount.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX)]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()
        yield(id_, remote)

@contextlib.contextmanager
def task(ctx, config):
    """
    Mount/unmount a cifs client. You need to already have smbd running somewhere.

    By default, the task will mount against the node with the 'samba.0' role,
    on all nodes which include a cifs-mount role. TODO: let users specify this stuff...

        tasks:
        - ceph:
        - ceph-fuse: [client.1]
        - samba: [samba.0]
        - cifs-mount:
        - interactive:
    """
    log.info('Mounting cifs clients...')
    cifs_daemons = {}

    if config is None:
        config = ['cifs-mount.{id}'.format(id=id_)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'cifs-mount')]

    clients = [('{id}'.format(id=id_), remote)
               for (id_, remote) in get_cifs(ctx=ctx, roles=config)]

    #get IP for Samba server
    (samba_remote,) = ctx.cluster.only('samba.0').remotes.iterkeys()
    (samba_ip,samba_port) = samba_remote.ssh.get_transport().getpeername()

    testdir = teuthology.get_testdir(ctx)

    for id_, remote in clients:
        mnt = os.path.join(testdir, 'cifs_mnt.{id}'.format(id=id_))
        log.info('Mounting cifs-mount.{id} at {remote} {mnt}...'.format(
                id=id_, remote=remote, mnt=mnt))
        
        remote.run(
            args=[
                'sudo', 'apt-get', 'install', 'cifs-utils', '-y', '--force-yes'
                ]
            )

        deamon_signal = 'kill'

        remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
                ],
            )

        remote.run(
            args=[
                'sudo',
                'mount',
                '-t',
                'cifs',
                '-o',
                'username=ubuntu,password=ubuntu',
                '//{sambaip}/ceph'.format(sambaip=samba_ip),
                mnt,
                ]
            )

    try:
        yield
    finally:
        log.info('Unmounting cifs clients...')
        for id_, remote in clients:
            mnt = os.path.join(testdir, 'cifs_mnt.{id}'.format(id=id_))
            remote.run(
                args=[
                    'sudo',
                    'umount',
                    mnt,
                    ],
                )
            remote.run(
                args=[
                    'sudo', 'apt-get', 'remove', 'cifs-utils', '-y', '--force-yes'
                    ]
                )


        for id_, remote in clients:
            mnt = os.path.join(testdir, 'cifs_mnt.{id}'.format(id=id_))
            remote.run(
                args=[
                    'rmdir',
                    '--',
                    mnt,
                    ],
                )
