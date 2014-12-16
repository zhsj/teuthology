import logging
import os
import subprocess
import tempfile
import yaml

from .config import config
from .misc import decanonicalize_hostname
from .lockstatus import get_status

log = logging.getLogger(__name__)


def _get_downburst_exec():
    """
    First check for downburst in the user's path.
    Then check in ~/src, ~ubuntu/src, and ~teuthology/src.
    Return '' if no executable downburst is found.
    """
    if config.downburst:
        return config.downburst
    path = os.environ.get('PATH', None)
    if path:
        for p in os.environ.get('PATH', '').split(os.pathsep):
            pth = os.path.join(p, 'downburst')
            if os.access(pth, os.X_OK):
                return pth
    import pwd
    little_old_me = pwd.getpwuid(os.getuid()).pw_name
    for user in [little_old_me, 'ubuntu', 'teuthology']:
        pth = os.path.expanduser("~%s/src/downburst/virtualenv/bin/downburst"
                                 % user)
        if os.access(pth, os.X_OK):
            return pth
    return ''


def get_distro(ctx):
    """
    Get the name of the distro that we are using (usually the os_type).
    """
    os_type = None
    # first, try to get the os_type from the config or --os-type
    try:
        os_type = ctx.config.get('os_type', ctx.os_type)
    except AttributeError:
        pass
    # next, look for an override in the downburst config for os_type
    try:
        os_type = ctx.config['downburst'].get('distro', os_type)
    except (KeyError, AttributeError):
        pass
    if os_type is None:
        # default to ubuntu if we can't find the os_type anywhere else
        return "ubuntu"
    return os_type


def get_distro_version(ctx):
    """
    Get the version of the distro that we are using (release number).
    """
    default_os_version = dict(
        ubuntu="12.04",
        fedora="18",
        centos="6.4",
        opensuse="12.2",
        sles="11-sp2",
        rhel="6.4",
        debian='7.0'
    )
    distro = get_distro(ctx)
    if ctx.os_version is not None:
        return ctx.os_version
    try:
        os_version = ctx.config.get('os_version', default_os_version[distro])
    except AttributeError:
        os_version = default_os_version[distro]
    try:
        return ctx.config['downburst'].get('distroversion', os_version)
    except (KeyError, AttributeError):
        return os_version


def create_if_vm(ctx, machine_name):
    """
    Use downburst to create a virtual machine
    """
    status_info = get_status(machine_name)
    if not status_info.get('is_vm', False):
        return False
    phys_host = decanonicalize_hostname(status_info['vm_host']['name'])
    os_type = get_distro(ctx)
    os_version = get_distro_version(ctx)

    createMe = decanonicalize_hostname(machine_name)
    with tempfile.NamedTemporaryFile() as tmp:
        if hasattr(ctx, 'config') and ctx.config is not None:
            lcnfg = ctx.config.get('downburst', dict())
        else:
            lcnfg = {}
        distro = lcnfg.get('distro', os_type.lower())
        distroversion = lcnfg.get('distroversion', os_version)

        file_info = {}
        file_info['disk-size'] = lcnfg.get('disk-size', '100G')
        file_info['ram'] = lcnfg.get('ram', '1.9G')
        file_info['cpus'] = lcnfg.get('cpus', 1)
        file_info['networks'] = lcnfg.get(
            'networks',
            [{'source': 'front', 'mac': status_info['mac_address']}])
        file_info['distro'] = distro
        file_info['distroversion'] = distroversion
        file_info['additional-disks'] = lcnfg.get(
            'additional-disks', 3)
        file_info['additional-disks-size'] = lcnfg.get(
            'additional-disks-size', '200G')
        file_info['arch'] = lcnfg.get('arch', 'x86_64')
        fqdn = machine_name.split('@')[1]
        file_out = {'downburst': file_info, 'local-hostname': fqdn}
        yaml.safe_dump(file_out, tmp)
        metadata = "--meta-data=%s" % tmp.name
        dbrst = _get_downburst_exec()
        if not dbrst:
            log.error("No downburst executable found.")
            return False
        p = subprocess.Popen([dbrst, '-c', phys_host,
                              'create', metadata, createMe],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
        owt, err = p.communicate()
        if err:
            log.info("Downburst completed on %s: %s" %
                    (machine_name, err))
        else:
            log.info("%s created: %s" % (machine_name, owt))
        # If the guest already exists first destroy then re-create:
        if 'exists' in err:
            log.info("Guest files exist. Re-creating guest: %s" %
                    (machine_name))
            destroy_if_vm(ctx, machine_name)
            create_if_vm(ctx, machine_name)
    return True


def destroy_if_vm(ctx, machine_name):
    """
    Use downburst to destroy a virtual machine

    Return False only on vm downburst failures.
    """
    status_info = get_status(machine_name)
    if not status_info or not status_info.get('is_vm', False):
        return True
    phys_host = decanonicalize_hostname(status_info['vm_host']['name'])
    destroyMe = decanonicalize_hostname(machine_name)
    dbrst = _get_downburst_exec()
    if not dbrst:
        log.error("No downburst executable found.")
        return False
    p = subprocess.Popen([dbrst, '-c', phys_host,
                          'destroy', destroyMe],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE,)
    owt, err = p.communicate()
    if err:
        log.error(err)
        return False
    else:
        log.info("%s destroyed: %s" % (machine_name, owt))
    return True
