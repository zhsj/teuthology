import contextlib
import logging
import yaml
import os
import errno
import re

from email.Utils import formatdate
from teuthology import teuthology_email
from teuthology import teuthology_report
from teuthology import misc as teuthology
from teuthology import contextutil
from ..orchestra import run

log = logging.getLogger(__name__)

@contextlib.contextmanager
def parallel_test(ctx, config):
    """Executing parallel radosbench tests..."""
    out_path = '/tmp/cephtest/archive/perf/radosbench'
    report = teuthology_report.TeuthologyReport('Radosbench Task Results Summary')
    roles = config.get('roles', {})

    attachments = {};
    for role, options in roles.iteritems():
        cluster = ctx.cluster.only(role)
        bench_options = options.get('bench', {})

        pool = str(options.get('pool', 'data'))
        op_size = str(bench_options.get('op_size', 1000000))
        duration = str(bench_options.get('duration', 60))
        mode = str(bench_options.get('mode', 'write'))
        concurrent_ops = str(bench_options.get('concurrent_operations', 16))

        header = 'Running test on hosts with role "%s"' % role
        log.info(header)
        section = report.add_section(header)
        section.header.append('')
        section.header.append('mode: %s' % mode)
        section.header.append('duration: %s' % duration)
        section.header.append('concurrent instances: %s' % len(cluster.remotes))
        section.header.append('concurrent operations per instance: %s' % concurrent_ops)
        section.header.append('pool: %s' % pool)

        args = [
            'mkdir', '-p', '-m0755', '--', out_path, run.Raw('&&'),
            'CEPH_CONF=/tmp/cephtest/ceph.conf',
            'LD_LIBRARY_PATH=/tmp/cephtest/binary/usr/local/lib',
            '/tmp/cephtest/binary/usr/local/bin/rados',
            '-p', pool, '-b', op_size, 'bench', duration, mode, '-t', concurrent_ops
            ]

        nodes = {}
        for remote in cluster.remotes.iterkeys():
            """Call remote.run with 'wait=False' so it returns immediately."""
            proc = remote.run(
                args = args + [run.Raw('|'), 'tee', '%s/%s.out' % (out_path, role)], 
                stdout=run.PIPE,
                wait=False,
                )
            nodes[remote.name] = proc
            
        for name, proc in nodes.iteritems():
            """Wait for each process to finish before moving on."""
            proc.exitstatus.get()

        total_throughput = 0
        tgz_files = {}
        for name, proc in nodes.iteritems():
            lines = []
            for line in proc.stdout:
                lines.append(line) 
                if 'Bandwidth' in line:
                    line = line.rstrip('\r\n')
                    log.info(line)
                    section.lines.append(line)
                    try:
                        current_throughput = float(re.findall(r'[\w.]+', line)[-1])
                    except ValueError:
                        log.error("Not a numeric string.")
                    total_throughput += current_throughput
#            file_name = '%s_%s.out' % (role, name)
            tgz_files['%s_%s.out' % (role, name)] = os.linesep.join(lines)
#            log.info(tgz_files)
        tgz = teuthology_email.create_tgz(tgz_files)
#        log.info(len(tgz.read()))
        attachments['%s.tgz' % role] = tgz 
             
        line = 'Aggregate (MB/sec):    %s' % total_throughput
        log.info(line)
        section.lines.append('-' * 32)
        section.lines.append(line)

    send_report(
        message = str(report),
        attachments = attachments,
        email_config = config.get('email', {})
        )
    yield

def send_report(message = '', attachments = {}, email_config = {}):
    FROM = str(email_config.get('from', None))
    TO = str(email_config.get('to', None))
    SERVER = str(email_config.get('server', None))
    USER = str(email_config.get('user', None))
    PASSWORD = str(email_config.get('password', None))
    SUBJECT = 'Teuthology Rados Bench Report - %s' % formatdate(localtime=True)
    teuthology_email.send_email(FROM=FROM, 
                                TO=TO, 
                                SERVER=SERVER, 
                                USER=USER, 
                                PASSWORD=PASSWORD, 
                                TEXT=message,
                                SUBJECT=SUBJECT,
                                ATTACHMENTS=attachments,
        )



@contextlib.contextmanager
def task(ctx, config):
    """This is the main body of the task that gets run."""

    """Take car of some yaml parsing here"""
#    if config is not None and not isinstance(config, list) and not isinstance(config, dict):
#        assert(false), "task parallel_example only supports a list or dictionary for configuration"
#    if config is None:
#        config = ['client.{id}'.format(id=id_)
#                  for id_ in teuthology.all_roles_of_type(ctx.cluster, 'client')] 
#    if isinstance(config, list):
#        config = dict.fromkeys(config)
#    clients = config.keys()

    """Run Multiple contextmanagers sequentially by nesting them."""
    with contextutil.nested(
        lambda: parallel_test(ctx=ctx, config=config),
        ):
        yield
