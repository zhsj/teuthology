from cStringIO import StringIO

import contextlib
import logging
import os
import re
import yaml

from teuthology import contextutil
from ..orchestra import run 

log = logging.getLogger(__name__)
directory = '/tmp/cephtest/collectl'
bin_dir = os.path.join(directory, 'collectl_latest')
log_dir = '/tmp/cephtest/archive/performance/collectl'

@contextlib.contextmanager
def setup(ctx, config):
    log.info('Setting up collectl')
    for client in config.iterkeys():
        cluster = ctx.cluster.only(client)

        for remote in cluster.remotes.iterkeys():
           proc = remote.run(
                args=[
                    'mkdir',
                    '-p',
                    directory,
                    run.Raw('&&'),
                    'mkdir',
                    '-p',
                    log_dir,
                    run.Raw('&&'),
                    'wget',
                    'http://sourceforge.net/projects/collectl/files/latest/download?source=files',
                    '-O',
                    os.path.join(directory, 'collectl_latest.tgz'),
                    run.Raw('&&'),
                    'tar',
                    'xvfz',
                    os.path.join(directory, 'collectl_latest.tgz'),
                    '-C',
                    directory,
                    ],
                    stdout=StringIO(),
                )
           log.info(proc.stdout.getvalue().split('\n', 1)[0])
           extract_dir = os.path.join(directory, proc.stdout.getvalue().split('\n', 1)[0])
           for remote in cluster.remotes.iterkeys():
               proc = remote.run(args=['mv', extract_dir, bin_dir])
           
    try:
        yield
    finally:
        log.info('removing collectl')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(args=['rm', '-rf', directory]) 

@contextlib.contextmanager
def execute(ctx, config):
    nodes = {}
    for client, properties in config.iteritems():
        if properties is None:
            properties = {}
        iteration = properties.get('iteration', 1)
        piteration = properties.get('piteration', 10)

        cluster = ctx.cluster.only(client)
        for remote in cluster.remotes.iterkeys():
            proc = remote.run(
                args=[
                    os.path.join(bin_dir, 'collectl.pl'),
                    '-f',
                    log_dir,
                    '-s+YZ',
                    '-i',
                    '%s:%s' % (iteration, piteration),
                    '-F10',
                    ],
                wait=False,        
                )
            nodes[remote] = proc
    try:
        yield
    finally:
        for remote in cluster.remotes.iterkeys():
            log.info('stopping collectl process on %s' % (remote.name))
            remote.run(args=['pkill', '-f', 'collectl.pl'])

@contextlib.contextmanager
def task(ctx, config):
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict.fromkeys(config)

    with contextutil.nested(
        lambda: setup(ctx=ctx, config=config),
        lambda: execute(ctx=ctx, config=config),
        ):
        yield

