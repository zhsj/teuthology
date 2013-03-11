from cStringIO import StringIO
import logging
import time
import pipes
import ConfigParser

from teuthology import misc as teuthology

from ..orchestra import run
log = logging.getLogger(__name__)

class _TrimConf(object):
    def __init__(self, s):
        self.conf = StringIO(s)

    def readline(self):
        line = self.conf.readline()
        return line.lstrip(' \t')

def _optionxform(s):
    s = s.replace('_', ' ')
    s = '_'.join(s.split())
    return s

def set_config(ctx, config, role, id_, *args):
    log.info('Setting one time config for {r}.{i} daemon: {a}'.format(r=role, i=id_, a=args))
    daemon = ctx.daemons.get_daemon(role, id_)

    conf_path = config.get('conf_path', '/etc/ceph/ceph.conf')
    oldconf = teuthology.get_file(
        remote=daemon.remote,
        path=conf_path,
        )

    confsect = '{r}.{i}'.format(r=role, i=id_)

    confp = ConfigParser.RawConfigParser()
    confp.optionxform = _optionxform
    ifp = _TrimConf(oldconf)
    confp.readfp(ifp)
    conf = dict(zip(args[0::2], args[1::2]))
    for k, v in conf:
        confp.set(confsect, k, v)

    newconf = StringIO()
    confp.write(newconf)
    teuthology.write_file(
        remote=daemon.remote,
        path=conf_path,
        data=newconf.getvalue(),
        perms='0644')
    newconf.close()

    return oldconf

def reset_config(ctx, config, role, id_, conf):
    log.info('Resetting config for {r}.{i} daemon'.format(r=role, i=id_))
    daemon = ctx.daemons.get_daemon(role, id_)
    conf_path = config.get('conf_path', '/etc/ceph/ceph.conf')

    teuthology.write_file(
        remote=daemon.remote,
        path=conf_path,
        data=conf,
        perms='0644')

def restart_daemon(ctx, config, role, id_):
    log.info('Restarting {r}.{i} daemon...'.format(r=role, i=id_))
    daemon = ctx.daemons.get_daemon(role, id_)
    while daemon.running():
        time.sleep(1)
    daemon.restart()

    # wait to see that it was restarted
    while not daemon.running():
        time.sleep(1)

def get_tests(ctx, config, role, remote, testdir):
    srcdir = '{tdir}/restart.{role}'.format(tdir=testdir, role=role)

    refspec = config.get('branch')
    if refspec is None:
        refspec = config.get('sha1')
    if refspec is None:
        refspec = config.get('tag')
    if refspec is None:
        refspec = 'HEAD'
    log.info('Pulling restart qa/workunits from ref %s', refspec)

    remote.run(
        logger=log.getChild(role),
        args=[
            'mkdir', '--', srcdir,
            run.Raw('&&'),
            'git',
            'archive',
            '--remote=git://ceph.newdream.net/git/ceph.git',
            '%s:qa/workunits' % refspec,
            run.Raw('|'),
            'tar',
            '-C', srcdir,
            '-x',
            '-f-',
            run.Raw('&&'),
            'cd', '--', srcdir,
            run.Raw('&&'),
            'if', 'test', '-e', 'Makefile', run.Raw(';'), 'then', 'make', run.Raw(';'), 'fi',
            run.Raw('&&'),
            'find', '-executable', '-type', 'f', '-printf', r'%P\0'.format(srcdir=srcdir),
            run.Raw('>{tdir}/restarts.list'.format(tdir=testdir)),
            ],
        )
    restarts = sorted(teuthology.get_file(
                        remote,
                        '{tdir}/restarts.list'.format(tdir=testdir)).split('\0'))
    return (srcdir, restarts)

def task(ctx, config):
    """
    Execute commands and allow daemon restart with config options.
    Each process executed can output to stdout restart commands of the form:
        restart <role> <id> <conf_key1> <conf_value1> <conf_key2> <conf_value2>
    This will restart the daemon <role>.<id> with the specified config values once
    by modifying the conf file with those values, and then replacing the old conf file
    once the daemon is restarted.
    This task does not kill a running daemon, it assumes the daemon will abort on an
    assert specified in the config.

        tasks:
        - install:
        - ceph:
        - restart:
            client.0:
              - test_backtraces.py

    """
    assert isinstance(config, dict), "task kill got invalid config"

    testdir = teuthology.get_testdir(ctx)

    try:
        for role, task in config.iteritems():
            (remote,) = ctx.cluster.only(role).remotes.iterkeys()
            srcdir, restarts = get_tests(ctx, config, role, remote, testdir)
            log.info('Running command on role %s host %s', role, remote.name)
            prefix = '{spec}/'.format(spec=task)
            to_run = [w for w in restarts if w == task or w.startswith(prefix)]
            for c in to_run:
                log.info('Running restart script %s...', c)
                args = [
                    run.Raw('TESTDIR="{tdir}"'.format(tdir=testdir)),
                    run.Raw('PYTHONPATH="$PYTHONPATH:{tdir}/binary/usr/local/lib/python2.7/dist-packages:{tdir}/binary/usr/local/lib/python2.6/dist-packages"'.format(tdir=testdir)),
                    ]
                env = config.get('env')
                if env is not None:
                    for var, val in env.iteritems():
                        quoted_val = pipes.quote(val)
                        env_arg = '{var}={val}'.format(var=var, val=quoted_val)
                        args.append(run.Raw(env_arg))
                        args.extend([
                            '{tdir}/enable-coredump'.format(tdir=testdir),
                            'ceph-coverage',
                            '{tdir}/archive/coverage'.format(tdir=testdir),
                            '{srcdir}/{c}'.format(
                                srcdir=srcdir,
                                c=c,
                                ),
                            ])
                proc = remote.run(
                    logger=log.getChild(role),
                    args=args,
                    )
                for l in proc.stdout.readline():
                    cmd = l.split(' ')
                    if cmd == "done":
                        break
                    assert cmd[0] == 'restart', "script sent invalid command request to kill task"
                    # cmd should be: restart <role> <id> <conf_key1> <conf_value1> <conf_key2> <conf_value2>
                    old_conf = set_config(ctx, config, cmd[1], cmd[2], cmd[3:])
                    restart_daemon(ctx, config, cmd[1], cmd[2])
                    reset_config(ctx, config, cmd[1], cmd[2], old_conf)
                    proc.stdin.writelines(['restarted\n'])
                    proc.stdin.flush()
    finally:


