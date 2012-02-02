from cStringIO import StringIO

import contextlib
import logging

from ..orchestra import run
from teuthology import misc as teuthology
from teuthology import contextutil
from teuthology.parallel import parallel

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Create a file, make a loopback device for it, create a filesystem
    on the device, and mount the filesystem. This was written to be
    used by the fake_ceph task.

    To use default settings, and run on all roles, use no
    configuration::

        tasks:
        - loopback:
        - fake_ceph:

    Or you can change the default options on all roles::

        tasks:
        - loopback:
            all:
              size: 4096 # in megabytes
              fs_type: xfs
        - fake_ceph:

    Or have different options for particular roles:

        tasks:
        - loopback:
            osd.0:
              size: 20480
            osd.1:
              fs_type: btrfs
            osd.2:
              fs_type: ext4
              size: 40960
            mon.0:
              size: 1024
            mon.1:
              size: 1024
            mon.2:
              size: 1024
        - fake_ceph:

    Note that each ceph role must be listed in the config,
    or the fake_ceph task will not work.
    """
    if config is None:
        config = { 'all': None }
    norm_config = teuthology.replace_all_with_roles(ctx.cluster,
                                                    config,
                                                    lambda role: True)
    log.debug('config is: %s', repr(norm_config))
    for role, option in norm_config.items():
        if option is None:
            norm_config[role] = {}
    log.debug('config is: %s', repr(norm_config))

    with contextutil.nested(
        lambda: create_file(ctx=ctx, config=norm_config),
        lambda: attach_dev(ctx=ctx, config=norm_config),
        lambda: mkfs(ctx=ctx, config=norm_config),
        lambda: mount(ctx=ctx, config=norm_config),
        ):
        yield

def filename_for_role(role):
    return '/tmp/cephtest/loopback_file.{role}'.format(role=role)

@contextlib.contextmanager
def create_file(ctx, config):
    for role, options in config.iteritems():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        remote.run(
            args=[
                'truncate',
                '-s',
                '{megs}M'.format(megs=options.get('size', 4096)),
                filename_for_role(role),
                ],
            )
    try:
        yield
    finally:
        for role in config.iterkeys():
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'rm',
                    '-f',
                    '--',
                    filename_for_role(role),
                    ],
                )

def dev_for_file(remote, filename):
    dev_fp = StringIO()
    remote.run(
        args=[
            'sudo',
            'losetup',
            '-j',
            filename,
            ],
        stdout=dev_fp,
        )
    out = dev_fp.getvalue()
    log.debug("mapped dev is %s", out.rstrip('\n'))
    return out[:out.find(':')]

@contextlib.contextmanager
def attach_dev(ctx, config):
    for role in config.iterkeys():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        remote.run(
            args=[
                'sudo',
                'losetup',
                '-f',
                filename_for_role(role),
                ],
            )
    try:
        yield
    finally:
        for role in config.iterkeys():
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'sudo',
                    'losetup',
                    '-d',
                    dev_for_file(remote, filename_for_role(role)),
                    ],
                )

@contextlib.contextmanager
def mkfs(ctx, config):
    with parallel() as p:
        for role, options in config.iteritems():
            (remote,) = ctx.cluster.only(role).remotes.keys()
            p.spawn(
                remote.run, 
                args=[
                    'sudo',
                    'mkfs',
                    '-t',
                    options.get('fs_type', 'ext3'),
                    dev_for_file(remote, filename_for_role(role)),
                    ],
                )
    yield

@contextlib.contextmanager
def mount(ctx, config):
    for role in config.iterkeys():
        (remote,) = ctx.cluster.only(role).remotes.keys()
        mnt = '/tmp/cephtest/mnt.{role}'.format(role=role)
        remote.run(
            args=[
                'mkdir',
                '--',
                mnt,
                run.Raw('&&'),
                'sudo',
                'mount',
                dev_for_file(remote, filename_for_role(role)),
                mnt,
                ],
            )
    try:
        yield
    finally:
        for role in config.iterkeys():
            (remote,) = ctx.cluster.only(role).remotes.keys()
            remote.run(
                args=[
                    'sudo',
                    'umount',
                    dev_for_file(remote, filename_for_role(role)),
                    run.Raw('&&'),
                    'rm',
                    '-rf',
                    '--',
                    '/tmp/cephtest/mnt.{role}'.format(role=role),
                    ],
                )
