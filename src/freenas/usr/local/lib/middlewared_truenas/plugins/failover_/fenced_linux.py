import subprocess

from middlewared.service import private, Service


class FencedForceService(Service):

    class Config:
        namespace = 'failover.fenced'

    @private
    def start(self, force=False):

        # TODO
        # Return False always until fenced daemon
        # can be written to work on Linux.
        return False

    @private
    def stop(self):

        subprocess.run(['pkill', '-9', '-f', 'fenced'])
