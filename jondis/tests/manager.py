import getpass
import logging
import os
import subprocess
from time import sleep

import redis

logger = logging.getLogger(__name__)
port = 25530
DEVNULL = open(os.devnull, 'wb')
CURRENT_USER = getpass.getuser()

if CURRENT_USER == 'travis':
    SLEEP_TIME = 3
else:
    SLEEP_TIME = .5


class Manager(object):

    def __init__(self):
        # procs is a dict of tuples (proc, port)
        self.procs = {}

    def start(self, name, master=None):
        global port
        slave_of = "--slaveof 127.0.0.1 {0}".format(master) if master else ""
        if CURRENT_USER == 'travis':
            start_command = "sudo redis-server --port {0} {1}".format(port,
                                                                      slave_of)
        else:
            start_command = "redis-server --port {0} {1}".format(port,
                                                                 slave_of)

        proc = subprocess.Popen(start_command, shell=True, stdout=DEVNULL,
                                stderr=DEVNULL)
        self.procs[name] = (proc, port)
        port += 1
        # ghetto hack but necessary to find the right slaves
        sleep(SLEEP_TIME)
        return self.procs[name][1]

    def stop(self, name):
        proc, port = self.procs[name]
        if CURRENT_USER == 'travis':
            logging.debug(
                subprocess.check_output('ps axu | grep redis-server',
                                        shell=True))
            pid = proc.pid
            logging.debug('Kill process #{0}'.format(pid))
            # some hack to kill child process
            subprocess.call("sudo kill {parent} {child}".format(parent=pid,
                                                                child=pid + 1),
                            shell=True)
            logging.debug(
                subprocess.check_output('ps axu | grep redis-server',
                                        shell=True))
        else:
            proc.terminate()
        # same hack as above to make sure failure actually happens
        sleep(SLEEP_TIME)

    def promote(self, port):
        admin_conn = redis.StrictRedis('localhost', port)
        logger.debug("Promoting {0}".format(port))
        admin_conn.slaveof()  # makes it the master
        sleep(SLEEP_TIME)

    def shutdown(self):
        for (proc, port) in self.procs.itervalues():
            proc.terminate()

    def __getitem__(self, item):
        return self.procs[item]
