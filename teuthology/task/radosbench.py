import contextlib
import logging

from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def task(ctx, config):
    """
    Run radosbench

    The config should be as follows:

    radosbench:
        clients: [client list]
        time: <seconds to run>

    example:

    tasks:
    - ceph:
    - radosbench:
        clients: [client.0]
        time: 360
    - interactive:
    """
    log.info('Beginning radosbench...')
    assert isinstance(config, dict), \
        "please list clients to run on"
    radosbench = {}

    for role in config.get('clients', ['client.0']):
        assert isinstance(role, basestring)
        out_path = '/tmp/cephtest/archive/performance/radosbench'
        PREFIX = 'client.'
        assert role.startswith(PREFIX)
        id_ = role[len(PREFIX):]
        (remote,) = ctx.cluster.only(role).remotes.iterkeys()

        op_size = str(config.get('op_size', ''))
        op_size_opt = '-b' if op_size else ''
        concurrent_ops = str(config.get('concurrent_ops', ''))
        concurrent_ops_opt = '-t' if concurrent_ops else ''
        args = []
        args.extend(['mkdir', '-p', '-m0755', '--', out_path])
        args.extend([run.Raw('&&')])
        args.extend([
            'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
            '/tmp/cephtest/enable-coredump',
            '/tmp/cephtest/binary/usr/local/bin/ceph-coverage',
            '/tmp/cephtest/archive/coverage',
            '/tmp/cephtest/binary/usr/local/bin/rados',
            '-c', '/tmp/cephtest/ceph.conf',
            '-k', '/tmp/cephtest/data/{role}.keyring'.format(role=role),
            '--name', role,
            '-p', str(config.get('pool', 'data')),
            op_size_opt, op_size, 
            'bench', 
            str(config.get('time', 360)),
            str(config.get('mode', 'write')),
            concurrent_ops_opt, concurrent_ops,
            run.Raw('|'), 'tee', '%s/%s.out' % (out_path, role)
            ]),
        proc = remote.run(
            args=args,
            logger=log.getChild('radosbench.{id}'.format(id=id_)),
            stdin=run.PIPE,
            wait=False,
            )
        radosbench[id_] = proc

    try:
        yield
    finally:
        log.info('joining radosbench')
        run.wait(radosbench.itervalues())
