import pprint
import yaml

from teuthology.misc import get_user, merge_configs
from teuthology import report


def main(args):
    if not args['--last-in-suite']:
        if args['--email']:
            raise ValueError(
                '--email is only applicable to the last job in a suite')
        if args['--timeout']:
            raise ValueError(
                '--timeout is only applicable to the last job in a suite')

    name = args['--name']
    if not name or name.isdigit():
        raise ValueError("Please use a more descriptive value for --name")
    job_config = build_config(args)
    if args['--dry-run']:
        pprint.pprint(job_config)
    else:
        schedule_job(job_config, args['--num'])


def build_config(args):
    """
    Given a dict of arguments, build a job config
    """
    config_paths = args.get('<conf_file>', list())
    conf_dict = merge_configs(config_paths)
    # strip out targets; the worker will allocate new ones when we run
    # the job with --lock.
    if 'targets' in conf_dict:
        del conf_dict['targets']
    args['config'] = conf_dict

    owner = args['--owner']
    if owner is None:
        owner = 'scheduled_{user}'.format(user=get_user())

    job_config = dict(
        name=args['--name'],
        last_in_suite=args['--last-in-suite'],
        email=args['--email'],
        description=args['--description'],
        owner=owner,
        verbose=args['--verbose'],
        machine_type=args['--worker'],
        tube=args['--worker'],
        priority=int(args['--priority']),
    )
    # Update the dict we just created, and not the other way around, to let
    # settings in the yaml override what's passed on the command line. This is
    # primarily to accommodate jobs with multiple machine types.
    job_config.update(conf_dict)
    if args['--timeout'] is not None:
        job_config['results_timeout'] = args['--timeout']
    return job_config


def schedule_job(job_config, num=1):
    """
    Schedule a job.

    :param job_config: The complete job dict
    :param num:      The number of times to schedule the job
    """
    num = int(num)
    while num > 0:
        job_id = report.try_push_job_info(job_config, dict(status='queued'))
        print 'Job scheduled with name {0} and ID {1}'.format(
            job_config['name'], job_id)
        num -= 1
