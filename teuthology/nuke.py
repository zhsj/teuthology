import argparse
import yaml

def parse_args():
    from teuthology.run import config_file
    from teuthology.run import MergeConfig

    parser = argparse.ArgumentParser(description='Reset test machines')
    parser.add_argument(
        '-v', '--verbose',
        action='store_true', default=None,
        help='be more verbose'
        )
    parser.add_argument(
        '-t', '--targets',
        nargs='+',
        type=config_file,
        action=MergeConfig,
        default={},
        dest='config',
        help='yaml config containing machines to nuke',
        )
    parser.add_argument(
        '-a', '--archive',
        metavar='DIR',
        help='archive path for a job to kill and nuke',
        )
    parser.add_argument(
        '--owner',
        help='job owner',
        )
    parser.add_argument(
        '-p','--pid',
	type=int,
	default=False,
        help='pid of the process to be killed',
        )
    parser.add_argument(
        '-r', '--reboot-all',
        action='store_true',
        default=False,
        help='reboot all machines',
        )
    parser.add_argument(
        '-s', '--synch-clocks',
        action='store_true',
        default=False,
        help='synchronize clocks on all machines',
        )
    parser.add_argument(
        '-u', '--unlock',
        action='store_true',
        default=False,
        help='Unlock each successfully nuked machine, and output targets that'
        'could not be nuked.'
        )
    parser.add_argument(
        '-n', '--name',
        metavar='NAME',
        help='Name of run to cleanup'
        )
    args = parser.parse_args()
    return args

def shutdown_daemons(ctx, log):
    from .orchestra import run
    nodes = {}
    reboot = []
    unmount_args = [
                'if', 'grep', '-q', 'ceph-fuse', '/etc/mtab', run.Raw(';'),
                'then',
                'grep', 'ceph-fuse', '/etc/mtab', run.Raw('|'),
                'grep', '-o', " /.* fuse", run.Raw('|'),
                'grep', '-o', "/.* ", run.Raw('|'),
                'xargs', 'sudo', 'fusermount', '-u', run.Raw(';'),
                'fi',
                ]

    killall_args = [
                'killall',
                '--quiet',
                'ceph-mon',
                'ceph-osd',
                'ceph-mds',
                'ceph-fuse',
                'radosgw',
                'testrados',
                'rados',
                'apache2',
                'testrados',
                run.Raw('||'),
                'true', # ignore errors from ceph binaries not being found
                ]

    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=unmount_args,
            wait=False,
        )
        try:
            proc.exitstatus.get(timeout=20)
        except gevent.Timeout:
            # blow away the mount with the abort sysctl
            # basically:  echo 1 > /sys/fs/fuse/connections/<last>/abort
            # we need some cruft to make that work here though
            remote.run(
                args=[
                    'echo', '1', run.Raw('>'), '/tmp/abortfile',
                    ],
                    )
            remote.run(
                args=[
                    'sudo', 'ls', '/sys/fs/fuse/connections/', run.Raw('|'),
                    'tail', '-n', '1', run.Raw('|'),
                    'sudo', 'xargs', '-I', 'XX', 'cp', '/tmp/abortfile', '/sys/fs/fuse/connections/XX/abort',
                    ],
                    )
            proc = remote.run(
                args=unmount_args,
                wait=False,
                )
            try:
                proc.exitstatus.get(timeout=20)
            except gevent.Timeout:
                # give up and request reboot
                reboot[remote] = remote

        proc = remote.run(
                args=killall_args,
                wait=False,
                )
        nodes[remote] = proc

    for r, proc in nodes.iteritems():
        log.info('Waiting for %s to finish shutdowns...', r.name)
        try:
            proc.exitstatus.get(timeout=20)
        except:
            # request reboot
            reboot[r] = r

    return reboot

def find_kernel_mounts(ctx, log):
    from .orchestra import run
    nodes = {}
    log.info('Looking for kernel mounts to handle...')
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'grep', '-q', ' ceph ' , '/etc/mtab',
                run.Raw('||'),
                'grep', '-q', '^/dev/rbd' , '/etc/mtab',
                ],
            wait=False,
            )
        nodes[remote] = proc
    kernel_mounts = {}
    for remote, proc in nodes.iteritems():
        try:
            proc.exitstatus.get()
            log.debug('kernel mount exists on %s', remote.name)
            kernel_mounts[remote] = remote
        except run.CommandFailedError: # no mounts!
            log.debug('no kernel mount on %s', remote.name)
    
    return kernel_mounts

def remove_kernel_mounts(ctx, kernel_mounts, log):
    """
    properly we should be able to just do a forced unmount,
    but that doesn't seem to be working, so you should reboot instead 
    """
    from .orchestra import run
    nodes = {}
    reboot = {}
    for remote in kernel_mounts.iterkeys():
        log.info('clearing kernel mount from %s', remote.name)
        proc = remote.run(
            args=[
                'grep', 'ceph', '/etc/mtab', run.Raw('|'),
                'grep', '-o', "on /.* type", run.Raw('|'),
                'grep', '-o', "/.* ", run.Raw('|'),
                'xargs', '-r',
                'sudo', 'umount', run.Raw(';'),
                'fi'
                ],
            wait=False
            )
        nodes[remote] = proc

    for remote, proc in nodes:
        try:
            proc.exitstatus.get(timeout=20)
        except gevent.Timeout:
            # mount hung, add to reboot list
            reboot[remote] = remote

    return reboot

def remove_osd_mounts(ctx, log):
    """
    unmount any osd data mounts (scratch disks)
    """
    from .orchestra import run
    from teuthology.misc import get_testdir
    nodes = {}
    reboot = {}
    for remote in ctx.cluster.remotes.iterkeys():
        proc = ctx.cluster.run(
            args=[
                'grep',
                '{tdir}/data/'.format(tdir=get_testdir(ctx)),
                '/etc/mtab',
                run.Raw('|'),
                'awk', '{print $2}', run.Raw('|'),
                'xargs', '-r',
                'sudo', 'umount', run.Raw(';'),
                'true'
                ],
            wait=False,
            )
        nodes[remote] = proc

    for r, p in nodes.iteritems():
        try:
            p.exitstatus.get(timeout=20)
        except gevent.Timeout:
            reboot[r] = r

    return reboot

def remove_osd_tmpfs(ctx, log):
    """
    unmount tmpfs mounts
    """
    from .orchestra import run
    nodes = {}
    reboot = {}
    for remote in ctx.cluster.remotes.iterkeys():
        proc = ctx.cluster.run(
                args=[
                    'egrep', 'tmpfs\s+/mnt', '/etc/mtab', run.Raw('|'),
                    'awk', '{print $2}', run.Raw('|'),
                    'xargs', '-r',
                    'sudo', 'umount', run.Raw(';'),
                    'true'
                    ],
                wait=False,
                )
        nodes[remote] = proc

    for r, p in nodes.iteritems():
        try:
            p.exitstatus.get(timeout=20)
        except gevent.Timeout:
            reboot[r] = r

    return reboot


def reboot(ctx, remotes, log):
    import time
    nodes = {}
    for remote in remotes:
        log.info('rebooting %s', remote.name)
        proc = remote.run( # note use of -n to force a no-sync reboot
            args=['sudo', 'reboot', '-f', '-n'],
            wait=False
            )
        nodes[remote] = proc
        # we just ignore these procs because reboot -f doesn't actually
        # send anything back to the ssh client!
        #for remote, proc in nodes.iteritems():
        #proc.exitstatus.get()
    from teuthology.misc import reconnect
    if remotes:
        log.info('waiting for nodes to reboot')
        time.sleep(5) #if we try and reconnect too quickly, it succeeds!
        reconnect(ctx, 480)     #allow 8 minutes for the reboots

def reset_syslog_dir(ctx, log):
    from .orchestra import run
    nodes = {}
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'if', 'test', '-e', '/etc/rsyslog.d/80-cephtest.conf',
                run.Raw(';'),
                'then',
                'sudo', 'rm', '-f', '--', '/etc/rsyslog.d/80-cephtest.conf',
                run.Raw('&&'),
                'sudo', 'initctl', 'restart', 'rsyslog',
                run.Raw(';'),
                'fi',
                run.Raw(';'),
                ],
            wait=False,
            )
        nodes[remote.name] = proc

    for name, proc in nodes.iteritems():
        log.info('Waiting for %s to restart syslog...', name)
        proc.exitstatus.get()

def remove_testing_tree(ctx, log):
    from teuthology.misc import get_testdir_base
    nodes = {}
    for remote in ctx.cluster.remotes.iterkeys():
        proc = remote.run(
            args=[
                'sudo', 'rm', '-rf',
                get_testdir_base(ctx),
                ],
            wait=False,
            )
        nodes[remote.name] = proc

    for name, proc in nodes.iteritems():
        log.info('Waiting for %s to clear filesystem...', name)
        proc.exitstatus.get()

def synch_clocks(remotes, log):
    from .orchestra import run
    nodes = {}
    for remote in remotes:
        proc = remote.run(
            args=[
                'sudo', 'service', 'ntp', 'stop',
                run.Raw('&&'),
                'sudo', 'ntpdate-debian',
                run.Raw('&&'),
                'sudo', 'hwclock', '--systohc', '--utc',
                run.Raw('&&'),
                'sudo', 'service', 'ntp', 'start',
                run.Raw('||'),
                'true',    # ignore errors; we may be racing with ntpd startup
                ],
            wait=False,
            )
        nodes[remote.name] = proc
    for name, proc in nodes.iteritems():
        log.info('Waiting for clock to synchronize on %s...', name)
        proc.exitstatus.get()

def main():
    from gevent import monkey; monkey.patch_all(dns=False)
    from .orchestra import monkey; monkey.patch_all()
    from teuthology.run import config_file

    import logging

    log = logging.getLogger(__name__)

    ctx = parse_args()

    loglevel = logging.INFO
    if ctx.verbose:
        loglevel = logging.DEBUG

    logging.basicConfig(
        level=loglevel,
        )

    if ctx.archive:
        ctx.config = config_file(ctx.archive + '/config.yaml')
        if not ctx.pid:
            ctx.pid = int(open(ctx.archive + '/pid').read().rstrip('\n'))
        if not ctx.owner:
            ctx.owner = open(ctx.archive + '/owner').read().rstrip('\n')

    from teuthology.misc import read_config
    read_config(ctx)

    log.info('\n  '.join(['targets:', ] + yaml.safe_dump(ctx.config['targets'], default_flow_style=False).splitlines()))

    if ctx.owner is None:
        from teuthology.misc import get_user
        ctx.owner = get_user()

    if ctx.pid:
        if ctx.archive:
            import os
            log.info('Killing teuthology process at pid %d', ctx.pid)
            os.system('grep -q %s /proc/%d/cmdline && sudo kill %d' % (
                    ctx.archive,
                    ctx.pid,
                    ctx.pid))
        else:
            import subprocess
            subprocess.check_call(["kill", "-9", str(ctx.pid)]);

    nuke(ctx, log, ctx.unlock, ctx.synch_clocks, ctx.reboot_all)

def nuke(ctx, log, should_unlock, sync_clocks=True, reboot_all=True):
    from teuthology.parallel import parallel
    total_unnuked = {}
    with parallel() as p:
        for target, hostkey in ctx.config['targets'].iteritems():
            p.spawn(
                nuke_one,
                ctx,
                {target: hostkey},
                log,
                should_unlock,
                sync_clocks,
                reboot_all,
                ctx.config.get('check-locks', True),
                )
        for unnuked in p:
            if unnuked:
                total_unnuked.update(unnuked)
    if total_unnuked:
        log.error('Could not nuke the following targets:\n' + '\n  '.join(['targets:', ] + yaml.safe_dump(total_unnuked, default_flow_style=False).splitlines()))

def nuke_one(ctx, targets, log, should_unlock, synch_clocks, reboot_all, check_locks):
    from teuthology.lock import unlock
    ret = None
    ctx = argparse.Namespace(
        config=dict(targets=targets),
        owner=ctx.owner,
        check_locks=check_locks,
        synch_clocks=synch_clocks,
        reboot_all=reboot_all,
        teuthology_config=ctx.teuthology_config,
        name=ctx.name,
        )
    try:
        nuke_helper(ctx, log)
    except:
        log.exception('Could not nuke all targets in %s', targets)
        # not re-raising the so that parallel calls aren't killed
        ret = targets
    else:
        if should_unlock:
            for target in targets.keys():
                unlock(ctx, target, ctx.owner)
    return ret

def nuke_helper(ctx, log):
    # ensure node is up with ipmi
    from teuthology.orchestra import remote

    (target,) = ctx.config['targets'].keys()
    host = target.split('@')[-1]
    shortname = host.split('.')[0]
    log.debug('shortname: %s' % shortname)
    if 'ipmi_user' in ctx.teuthology_config:
        console = remote.RemoteConsole(name=host,
                                       ipmiuser=ctx.teuthology_config['ipmi_user'],
                                       ipmipass=ctx.teuthology_config['ipmi_password'],
                                       ipmidomain=ctx.teuthology_config['ipmi_domain'])
        cname = '{host}.{domain}'.format(host=shortname, domain=ctx.teuthology_config['ipmi_domain'])
        log.info('checking console status of %s' % cname)
        if not console.check_status():
            # not powered on or can't get IPMI status.  Try to power on
            console.power_on()
            # try to get status again, waiting for login prompt this time
            log.info('checking console status of %s' % cname)
            if not console.check_status(100):
                log.error('Failed to get console status for %s, disabling console...' % cname)
            log.info('console ready on %s' % cname)
        else:
            log.info('console ready on %s' % cname)

    from teuthology.task.internal import check_lock, connect
    if ctx.check_locks:
        check_lock(ctx, None)
    connect(ctx, None)

    log.info('Unmount ceph-fuse and killing daemons...')
    daemon_reboot = shutdown_daemons(ctx, log)
    log.info('All daemons killed.')

    log.info('Unmount any osd data directories...')
    osd_reboot = remove_osd_mounts(ctx, log)

    log.info('Unmount any osd tmpfs dirs...')
    tmpfs_reboot = remove_osd_tmpfs(ctx, log)


    log.info('Dealing with any kernel mounts...')
    kernel_mounts = find_kernel_mounts(ctx, log)
    kmount_reboot = remove_kernel_mounts(ctx, kernel_mounts, log)

    need_reboot = list(set(daemon_reboot.items()+osd_reboot.items()+tmpfs_reboot.items()))

    if ctx.reboot_all:
        need_reboot = ctx.cluster.remotes.keys()
    reboot(ctx, need_reboot, log)
    log.info('All kernel mounts gone.')

    log.info('Synchronizing clocks...')
    # all rebooted nodes need clocks resynced
    sync_clocks = need_reboot
    if ctx.synch_clocks:
        # synch all
        sync_clocks = ctx.cluster.remotes.keys()
    synch_clocks(sync_clocks, log)

    log.info('Reseting syslog output locations...')
    reset_syslog_dir(ctx, log)
    log.info('Clearing filesystem of test data...')
    remove_testing_tree(ctx, log)
    log.info('Filesystem Cleared.')
