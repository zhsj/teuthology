import logging
import contextlib
import ceph_manager
import random
import time
import gevent
import json
import math
from teuthology import misc as teuthology
from teuthology.task import ceph as ceph_task

log = logging.getLogger(__name__)

class CephUpgrade:

  def __init__(self, ctx, manager, config, logger):
    self.ctx = ctx
    self.manager = manager
    self.manager.wait_for_clean()

    self.stopping = False
    self.logger = logger
    self.config = config

    assert self.config is not None, \
        'ceph_upgrade requires a config'

    self.upgrade_to = self.config.get('upgrade-to', None)
    self.upgrade_after = self.config.get('upgrade-after', None)

    assert self.upgrade_to is not None, \
        'must specify \'upgrade-to\''
    assert isinstance(self.upgrade_to, list), \
        '\'upgrade-to\' must be a list of alternative names'
    assert self.upgrade_after is not None, \
        'must specify \'upgrade-after\''
    self.upgrade_after = float(self.upgrade_after)
    assert self.upgrade_after > 0.0, \
        'upgrade-after must be > 0.0'

    self.thread = gevent.spawn(self.do_upgrade)

  def do_upgrade(self):
    i = 0
    for name in self.upgrade_to:
      time.sleep(self.upgrade_after)
      self.logger.info('upgrading to {n}'.format(n=name))
      ceph_task.set_alternative(self.ctx, name)
      i += 1
      if self.stopping:
        break
    self.logger.info('finished; upgraded {n}'.format(n=i))

  def do_join(self):
    self.logger.info('Stopping ceph_upgrade')
    self.stopping = True
    self.thread.get()



@contextlib.contextmanager
def task(ctx, config):
  """
  Upgrade the cluster by changing between alternatives.
  """
  if config is None:
    config = {}
  assert isinstance(config, dict), \
      'ceph_upgrade task only accepts a dict for configuration'
  log.info('Beginning ceph_upgrade...')
  first_mon = teuthology.get_first_mon(ctx, config)
  (mon,) = ctx.cluster.only(first_mon).remotes.iterkeys()
  manager = ceph_manager.CephManager(
      mon,
      ctx=ctx,
      logger=log.getChild('ceph_manager'),
      )
  upgrade_proc = CephUpgrade(ctx, manager,
      config, logger=log.getChild('ceph_upgrade'))
  try:
    yield
  finally:
    log.info('finishing ceph_upgrade')
    upgrade_proc.do_join()
