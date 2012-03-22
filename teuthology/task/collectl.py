import contextlib
import logging
import os
import yaml

from teuthology import contextutil
from ..orchestra import run 

log = logging.getLogger(__name__)
directory = '/tmp/cephtest/collectl'

@contextlib.contextmanager
def setup(ctx, config):
    log.info('Setting up collectl')
    for client in config.iterkeys():
        ctx.cluster.only(client).run(
            args=[
                'mkdir',
                '-p',
                directory,
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
                ]
            )
    try:
        yield
    finally:
        log.info('removing collectl')
        for client in config.iterkeys():
            ctx.cluster.only(client).run(args=['rm', '-rf', directory]) 

#@contextlib.contextmanager
#def run(ctx, config):
    

@contextlib.contextmanager
def task(ctx, config):
    if config is None:
        config = dict(('client.{id}'.format(id=id_), None)
                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client'))
    elif isinstance(config, list):
        config = dict((name, None) for name in config)

    with contextutil.nested(
        lambda: setup(ctx=ctx, config=config),
        ):
        yield

