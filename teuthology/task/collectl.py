import logging
import os

from cStringIO import StringIO

from teuthology.exceptions import SELinuxError
from teuthology.misc import get_archive_dir
from teuthology.orchestra.cluster import Cluster
from teuthology.orchestra import run
from teuthology.lockstatus import get_status

from . import Task

log = logging.getLogger(__name__)


class Collectl(Task):
    def __init__(self, ctx, config):
        super(Collectl, self).__init__(ctx, config)
        self.log = log
        self.mode = self.config.get('mode', 'permissive')
        archive_dir = get_archive_dir(ctx)
        self.log_dir = '{}/collectl'.format(archive_dir)

    def setup(self):
        """
        Perform any setup that is needed by the task before it executes
        """
        self.cluster.run(args=[
            'mkdir', self.log_dir,
        ])

    def begin(self):
        """
        Execute the main functionality of the task
        """
        base_args = self._build_args()
        self.procs = self.cluster.run(
            args=base_args,
            wait=False,
        )

    def _build_args(self):
        """
        Assemble the list of args to be executed
        """
        args = [
            # Run collectl with sudo
            'sudo', 'collectl',
            # Write files to self.log_dir
            '-f', self.log_dir,
            # Write a daemon logfile in addition to data files
            '-m',
            # Write data to disk as soon as it is collected
            '-F0',
            # In addition to any defaults, collect:
            #  CPU, Disk, Memory, Network, Process and Slabs data
            '-s+cdmnYZ',
            # Sample most data types every second; processes/slabs every 30s,
            # and environmentals every 300s
            '-i1:60:300',
            # Compress all data files
            '-oz',
            # Write plot files
            '-P',
            # ...And also write raw files
            '--rawtoo',
        ]
        return args

    def end(self):
        """
        Perform any work needed to stop processes started in begin()
        """
        pass

    def teardown(self):
        """
        Perform any work needed to restore configuration to a previous state.

        Can be skipped by setting 'skip_teardown' to True in self.config
        """
        pass
