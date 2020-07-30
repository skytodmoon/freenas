# Copyright (c) 2020 iXsystems, Inc.
# All rights reserved.
# This file is a part of TrueNAS
# and may not be copied and/or distributed
# without the express permission of iXsystems.

from lockfile import LockFile, AlreadyLocked
from collections import defaultdict
from threading import Lock
import os
import subprocess
import time
import struct
import contextlib
import shutil

from middlewared.utils import filter_list
from middlewared.service import Service, private


# This is the primitive lock used to protect a failover "event".
# This means that we will grab an exclusive lock before
# we call any of the code that does any of the work.
# This does a few things:
#
#    1. protects us if we have an interface that has a
#        rapid succession of state changes
#
#    2. if we have a near simultaneous amount of
#        events get triggered for all interfaces
#        --this can happen on external network failure
#        --this happens when one node reboots
#        --this happens when keepalived service is restarted
#
# If any of the above scenarios occur, we want to ensure
# that only one thread is trying to run fenced or import the
# zpools.
EVENT_LOCK = Lock()

# file created by the pool plugin during certain
# scenarios when importing zpools on boot
ZPOOL_KILLCACHE = '/data/zfs/killcache'

# zpool cache file managed by ZFS
ZPOOL_CACHE_FILE = '/data/zfs/zpool.cache'

# zpool cache file that's been saved by pool plugin
# during certain scenarios importing zpools on boot
ZPOOL_CACHE_FILE_SAVED = f'{ZPOOL_CACHE_FILE}.saved'

# Samba sentinel file
SAMBA_USER_IMPORT_FILE = "/root/samba/.usersimported"

# This sentinel is created by the pool decryption
# script to let us know we need to do something
FAILOVER_NEEDOP = '/tmp/.failover_needop'

# This file is managed in unscheduled_reboot_alert.py
# Ticket 39114
WATCHDOG_ALERT_FILE = "/data/sentinels/.watchdog-alert"

# TODO: REMOVE ME ONCE vrrp_backup has been fixed
FENCED_EVENT = '/tmp/.failover_override'
FAILOVER_ASSUMED_MASTER = '/tmp/.failover_master'
FAILOVER_EVENT = '/tmp/.failover_event'


def run(cmd, stderr=False):
    proc = subprocess.Popen(
        cmd,
        stderr=subprocess.PIPE if not stderr else subprocess.STDOUT,
        stdout=subprocess.PIPE,
        shell=True,
        encoding='utf8',
    )
    output = proc.communicate()[0]
    return (proc.returncode, output.strip('\n'))


def run_async(cmd):
    subprocess.Popen(
        cmd,
        shell=True,
    )
    return


class FailoverService(Service):

    @private
    def run_call(self, method, *args, **kwargs):
        try:
            return self.middleware.call_sync(method, *args, **kwargs)
        except Exception as e:
            self.logger.error('Failed to run %s:%r:%r %s', method, args, kwargs, e)

    @private
    def event(self, ifname, event):
        try:
            return self._event(ifname, event)
        finally:
            self.run_call('failover.status_refresh')

    @private
    def generate_failover_data(self):

        # only care about name and guid
        volumes = self.run_call('pool.query', [], {'select': ['name', 'guid']})

        failovercfg = self.run_call('failover.config')
        interfaces = self.run_call('interface.query')
        internal_ints = self.run_call('failover.internal_interfaces')

        data = {
            'disabled': failovercfg['disabled'],
            'master': failovercfg['master'],
            'timeout': failovercfg['timeout'],
            'groups': defaultdict(list),
            'volumes': volumes,
            'non_crit_interfaces': [
                i['id'] for i in filter_list(interfaces, [
                    ('failover_critical', '!=', True),
                ])
            ],
            'internal_interfaces': internal_ints,
        }

        for i in filter_list(interfaces, [('failover_critical', '=', True)]):
            data['groups'][i['failover_group']].append(i['id'])

        return data

    @private
    def _event(self, ifname, event):

        # first thing to check is if there is an ongoing event
        # if there is, ignore it
        if EVENT_LOCK.locked():
            self.logger.warning('Failover event is already being processed, ignoring.')
            return

        forcetakeover = False
        if event == 'forcetakeover':
            forcetakeover = True

        # generate data to be used during the failover event
        fobj = self.generate_failover_data()

        # grab the primitive lock
        with EVENT_LOCK:
            if not forcetakeover:
                if fobj['disabled'] and not fobj['master']:
                    # if forcetakeover is false, and failover is disabled
                    # and we're not set as the master controller, then
                    # there is nothing we need to do.
                    self.logger.warning('Failover is disabled, assuming backup.')
                    self.run_call('service.restart', 'keepalived')
                    return

                # any logic below here means failover is disabled and we are
                # designated as the master controller so act accordingly

                # If there is a state change on a non-critical interface then
                # ignore the event and return
                ignore = [i for i in fobj['non_crit_interfaces'] if i in ifname]
                if ignore:
                    self.logger.warning(f'Ignoring state change on non-critical interface:{ifname}.')
                    return

                # if the other controller is already master, then assume backup
                try:
                    if self.call_sync('failover.call_remote', 'failover.status') == 'MASTER':
                        self.logger.warning('Other node is already active, assuming backup.')
                        self.run_call('service.restart', 'keepalived')
                        return
                except Exception:
                    self.logger.error('Failed to contact the other node', exc_info=True)

                # ensure the zpools are imported
                needs_imported = False
                for vol in fobj['volumes']:
                    zpool = self.run_call('pool.query', [('name', '=', vol['name'])], {'get': True})
                    if zpool['status'] != 'ONLINE':
                        needs_imported = True
                        # try to restart the vrrp service on standby controller to ensure all interfaces
                        # on this controller are in the MASTER state
                        try:
                            self.run_call('failover.call_remote', 'service.restart', ['keepalived'])
                        except Exception:
                            self.logger.error('Failed contacting standby controller when restarting vrrp.', exc_info=True)
                        break

                # means all zpools are already imported so nothing else to do
                if not needs_imported:
                    self.logger.warning('Failover disabled but zpool(s) are imported. Assuming active.')
                    return
                # means at least 1 of the zpools are not imported so act accordingly
                else:
                    # set the event to MASTER
                    event = 'MASTER'
                    # set force_fenced to True so that it's
                    # called with the --force option which
                    # guarantees the disks will be reserved
                    # by this controller
                    force_fenced = needs_imported

            if event == 'MASTER' or event == 'forcetakeover':
                return self.vrrp_master(fobj, ifname, event, force_fenced, forcetakeover)
            elif event == 'BACKUP':
                return self.vrrp_backup(fobj, ifname, event, force_fenced)

    @private
    def vrrp_master(self, fobj, ifname, vhid, event, force_fenced, forcetakeover):

        # this is the boolean we return to our caller
        failover_successful = False

        # first thing to do is stop fenced process just in case it's running already
        # we will restart it based on args passed to us
        # NOTE: this does not cause concern because:
        #
        #    1. if fenced was already running then the disks have been reserved
        #       and stopping the process does not clear the reservations
        #
        #    2. if fenced was not already running then the disks were probably
        #       not reserved by us so we will reserve them eventually (or error)
        #       based on the args passed to this method
        #
        self.run_call('failover.fenced.stop')

        fenced_error = None
        if forcetakeover or force_fenced:
            # reserve the disks forcefully ignoring if the other node has the disks
            self.logger.warning('Forcefully taking over as the MASTER node.')
            fenced_error = self.run_call('failover.fenced.start', force=True)
        else:
            self.logger.warning(f'Entering MASTER on {ifname}.')
            fenced_error = self.run_call('failover.fenced.start')

        # starting fenced daemon failed....which is bad
        # emit an error and exit
        if fenced_error:
            if fenced_error == 1:
                self.logger.error('Failed to register keys on disks, exiting!')
            elif fenced_error == 2:
                self.logger.error('Fenced is running on the remote node, exiting!')
            elif fenced_error == 3:
                self.logger.error('10% or more of the disks failed to be reserved, exiting!')
            elif fenced_error == 5:
                self.logger.warn('Fencing daemon encountered an unexpected fatal error, exiting!')
            else:
                self.logger.warn(f'Fenced exited with code:{fenced_error} which should never happen, exiting!')

            return failover_successful

        # At this point, fenced is daemonized and drives have been reserved.
        # Bring up all carps we own.

        # remove the zpool cache files if necessary
        if os.path.exists(ZPOOL_KILLCACHE):
            for i in (ZPOOL_CACHE_FILE, ZPOOL_CACHE_FILE_SAVED):
                with contextlib.suppress(Exception):
                    os.unlink(i)

        # create the ZPOOL_KILLCACHE file
        else:
            with contextlib.suppress(Exception):
                with open(ZPOOL_KILLCACHE, 'w') as f:
                    f.flush()  # be sure it goes straight to disk
                    os.fsync(f.fileno())  # be EXTRA sure it goes straight to disk

        # if we're here and the zpool "saved" cache file exists we need to check
        # if it's modify time is < the standard zpool cache file and if it is
        # we overwrite the zpool "saved" cache file with the standard one
        if os.path.exists(ZPOOL_CACHE_FILE_SAVED) and os.path.exists(ZPOOL_CACHE_FILE):
            zpool_cache_mtime = os.stat(ZPOOL_CACHE_FILE).st_mtime
            zpool_cache_saved_mtime = os.stat(ZPOOL_CACHE_FILE_SAVED).st_mtime
            if zpool_cache_mtime > zpool_cache_saved_mtime:
                with contextlib.suppress(Exception):
                    shutil.copy2(ZPOOL_CACHE_FILE, ZPOOL_CACHE_FILE_SAVED)

        failed = []
        for vol in fobj['volumes']:
            self.logger.info(f'Importing {vol["name"]}')

            # try to import the zpool(s)
            try:
                self.run_call(
                    'zfs.pool.import_pool',
                    vol['guid'],
                    {
                        'altroot': '/mnt',
                        'cachefile': ZPOOL_CACHE_FILE,
                    }
                )
            except Exception as e:
                vol['error'] = str(e)
                failed.append(vol)
                continue

            # try to unlock the zfs datasets (if any)
            unlock_job = self.run_call('failover.unlock_zfs_datasets', vol["name"])
            unlock_job.wait_sync()
            if unlock_job.error:
                self.logger.error(f'Error unlocking ZFS encrypted datasets: {unlock_job.error}')
            elif unlock_job.result['failed']:
                self.logger.error('Failed to unlock %s ZFS encrypted dataset(s)', ','.join(unlock_job.result['failed']))

        # if we fail to import all zpools then alert the user because nothing
        # is going to work at this point
        if len(failed) == len(fobj['volumes']):
            for i in failed:
                self.logger.error(
                    f'Failed to import volume with name:{failed["name"]} with guid:{failed["guid"]} '
                    'with error:{failed["error"]}'
                )

            self.logger.error('All volumes failed to import!')
            return failover_successful

        # if we fail to import any of the zpools then alert the user but continue the process
        for i in failed:
            self.logger.error(
                f'Failed to import volume with name:{failed["name"]} with guid:{failed["guid"]} '
                'with error:{failed["error"]}. '
                'However, other zpools imported so we continued the failover process.'
            )

        self.logger.info('Volume imports complete.')

        # need to make sure failover status is updated in the middleware cache
        self.logger.info('Refreshing failover status')
        self.run_call('failover.status_refresh')

        # calling this actually generates a file
        self.logger.info('Enabling necessary services.')
        self.run_call('etc.generate', 'rc')

        self.logger.info('Configuring system dataset')
        self.run_call('etc.generate', 'system_dataset')

        # Write the certs to disk based on what is written in db.
        self.run_call('etc.generate', 'ssl')
        # Now we restart the appropriate services to ensure it's using correct certs.
        self.run_call('service.restart', 'http')

        # classify list of "critical" services
        critical_services = ['iscsitarget', 'nfs', 'cifs', 'afp']
        ha_propagate = {'ha_propagate': False}

        # get list of all services on system
        # we query db directly since on SCALE calling `service.query`
        # actually builds a list of all services and includes if they're
        # running or not. Probing all services on the system to see if
        # they're running takes longer than what we need since failover
        # needs to be as fast as possible.
        services = self.run_call('datastore.query', 'services_services')

        # now we restart the services, prioritizing the "critical" services
        self.logger.info('Restarting critical services.')
        for i in critical_services:
            for j in services:
                if i == j['srv_service'] and j['srv_enable']:
                    self.logger.info(f'Restarting critical service:{i}')
                    self.run_call('service.restart', i, ha_propagate)

        # TODO: look at nftables
        # self.logger.info('Allowing network traffic.')
        # run('/sbin/pfctl -d')

        self.logger.info('Critical portion of failover is now complete.')

        # regenerate cron
        self.logger.info('Regenerating cron')
        self.run_call('etc.generate', 'cron')

        # sync disks is disabled on passive node
        self.logger.info('Syncing disks')
        self.run_call('disk.sync_all')

        self.logger.info('Syncing enclosure')
        self.run_call('enclosure.sync_zpool')

        # restart the remaining "non-critical" services
        self.logger.info('Restarting remaining services')

        self.logger.info('Restarting collected')
        self.run_call('service.restart', 'collectd', ha_propagate)

        self.logger.info('Restarting syslog-ng')
        self.run_call('service.restart', 'syslogd', ha_propagate)

        for i in services:
            if i['srv_service'] not in critical_services and i['srv_enable']:
                self.logger.info('Restarting service:{i["srv_service"]}')
                self.run_call('service.restart', i['srv_service'], ha_propagate)

        # TODO: jails don't exist on SCALE (yet)
        # TODO: vms don't exist on SCALE (yet)
        # self.run_call('jail.start_on_boot')
        # self.run_call('vm.start_on_boot')

        self.logger.info('Initializing alert system')
        self.run_call('alert.block_failover_alerts')
        self.run_call('alert.initialize', False)

        kmip_config = self.run_call('kmip.config')
        if kmip_config and kmip_config['enabled']:
            self.logger.info('Initializing KMIP sync')

            # Even though we keep keys in sync, it's best that we do this as well
            # to ensure that the system is up to date with the latest keys available
            # from KMIP. If it's unaccessible, the already synced memory keys are used
            # meanwhile.
            self.run_call('kmip.initialize_keys')

        self.logger.warn('Failover event complete.')

        failover_succesful = True
        return failover_succesful

    @private
    def carp_backup(self, fobj, ifname, vhid, event, user_override):
        self.logger.warn('Entering BACKUP on %s', ifname)

        if not user_override:
            sleeper = fobj['timeout']
            # The specs for lagg require that if a subinterface of the lagg interface
            # changes state, all traffic on the entire logical interface will be halted
            # for two seconds while the bundle reconverges.  This means if there's a
            # toplogy change on the active node, the standby node will get a link_up
            # event on the lagg.  To  prevent the standby node from immediately pre-empting
            # we wait 2 seconds to see if the evbent was transient.
            if ifname.startswith('lagg'):
                if sleeper < 2:
                    sleeper = 2
            else:
                # Check interlink - if it's down there is no need to wait.
                for iface in fobj['internal_interfaces']:
                    error, output = run(
                        f"ifconfig {iface} | grep 'status:' | awk '{{print $2}}'"
                    )
                    if output != 'active':
                        break
                else:
                    if sleeper < 2:
                        sleeper = 2

            if sleeper != 0:
                self.logger.warn('Sleeping %s seconds and rechecking %s', sleeper, ifname)
                time.sleep(sleeper)
                error, output = run(
                    f"ifconfig {ifname} | grep 'carp:' | awk '{{print $2}}'"
                )
                if output == 'MASTER':
                    self.logger.warn(
                        'Ignoring state on %s because it changed back to MASTER after '
                        '%s seconds.', ifname, sleeper,
                    )
                    return True

        """
        We check if we have at least one MASTER interface per group.
        If that turns out to be true we ignore the BACKUP state in one of the
        interfaces, otherwise we assume backup demoting carps.
        """
        ignoreall = True
        for group, carpint in list(fobj['groups'].items()):
            totoutput = 0
            ignore = False
            for i in carpint:
                error, output = run(f"ifconfig {i} | grep -c 'carp: MASTER'")
                totoutput += int(output)

                if not error and totoutput > 0:
                    ignore = True
                    break
            ignoreall &= ignore

        if ignoreall:
            self.logger.warn(
                'Ignoring DOWN state on %s because we still have interfaces that '
                'are UP.', ifname)
            return False

        # Start the critical section
        try:
            with LockFile(FAILOVER_EVENT, timeout=0):
                # The lockfile modules cleans up lockfiles if this script exits on it's own accord.
                # For reboots, /tmp is cleared by virtue of being a memory device.
                # If someone does a kill -9 on the script while it's running the lockfile
                # will get left dangling.
                self.logger.warn('Acquired failover backup lock')
                run('pkill -9 -f fenced')

                for iface in fobj['non_crit_interfaces']:
                    error, output = run(f"ifconfig {iface} | grep 'carp:' | awk '{{print $4}}'")
                    for vhid in output.split():
                        self.logger.warn('Setting advskew to 100 on non-critical interface %s', iface)
                        run(f'ifconfig {iface} vhid {vhid} advskew 100')

                for group in fobj['groups']:
                    for interface in fobj['groups'][group]:
                        error, output = run(f"ifconfig {interface} | grep 'carp:' | awk '{{print $4}}'")
                        for vhid in output.split():
                            self.logger.warn('Setting advskew to 100 on critical interface %s', interface)
                            run(f'ifconfig {interface} vhid {vhid} advskew 100')

                run('/sbin/pfctl -ef /etc/pf.conf.block')

                run('/usr/sbin/service watchdogd quietstop')

                # ticket 23361 enabled a feature to send email alerts when an unclean reboot occurrs.
                # TrueNAS HA, by design, has a triggered unclean shutdown.
                # If a controller is demoted to standby, we set a 4 sec countdown using watchdog.
                # If the zpool(s) can't export within that timeframe, we use watchdog to violently reboot the controller.
                # When this occurrs, the customer gets an email about an "Unauthorized system reboot".
                # The idea for creating a new sentinel file for watchdog related panics,
                # is so that we can send an appropriate email alert.

                # If we panic here, middleware will check for this file and send an appropriate email.
                # Ticket 39114
                try:
                    fd = os.open(WATCHDOG_ALERT_FILE, os.O_RDWR | os.O_CREAT | os.O_TRUNC)
                    epoch = int(time.time())
                    b = struct.pack("@i", epoch)
                    os.write(fd, b)
                    os.fsync(fd)
                    os.close(fd)
                except EnvironmentError as err:
                    self.logger.warn(err)

                run('watchdog -t 4')

                # If the network is flapping, a backup node could get a master
                # event followed by an immediate backup event.  If the other node
                # is master and shoots down our master event we will immediately
                # run the code for the backup event, even though we are already backup.
                # So we use volumes as a sentinel to tell us if we did anything with
                # regards to exporting volumes.  If we don't export any volumes it's
                # ok to assume we don't need to do anything else associated with
                # transitioning to the backup state. (because we are already there)

                # Note this wouldn't be needed with a proper state engine.
                volumes = False
                for volume in fobj['volumes'] + fobj['phrasedvolumes']:
                    error, output = run(f'zpool list {volume}')
                    if not error:
                        volumes = True
                        self.logger.warn('Exporting %s', volume)
                        error, output = run(f'zpool export -f {volume}')
                        if error:
                            # the zpool status here is extranious.  The sleep
                            # is going to run off the watchdog and the system will reboot.
                            run(f'zpool status {volume}')
                            time.sleep(5)
                        self.logger.warn('Exported %s', volume)

                run('watchdog -t 0')
                try:
                    os.unlink(FAILOVER_ASSUMED_MASTER)
                except Exception:
                    pass

                # We also remove this file here, because this code path is executed on boot.
                # The middlewared process is removing the file and then sending an email as expected.
                # However, this python file is being called about 1min after middlewared and recreating the file on line 651.
                try:
                    os.unlink(WATCHDOG_ALERT_FILE)
                except EnvironmentError:
                    pass

                self.run_call('failover.status_refresh')
                self.run_call('service.restart', 'syslogd', {'ha_propagate': False})

                self.run_call('etc.generate', 'cron')

                if volumes:
                    run('/usr/sbin/service watchdogd quietstart')
                    self.run_call('service.stop', 'smartd', {'ha_propagate': False})
                    self.run_call('service.stop', 'collectd', {'ha_propagate': False})
                    self.run_call('jail.stop_on_shutdown')
                    for vm in (self.run_call('vm.query', [['status.state', '=', 'RUNNING']]) or []):
                        self.run_call('vm.poweroff', vm['id'], True)
                    run_async('echo "$(date), $(hostname), assume backup" | mail -s "Failover" root')

                for i in (
                    'ssh', 'iscsitarget',
                ):
                    verb = 'restart'
                    if i == 'iscsitarget':
                        if not self.run_call('iscsi.global.alua_enabled'):
                            verb = 'stop'

                    ret = self.run_call('datastore.query', 'services.services', [('srv_service', '=', i)])
                    if ret and ret[0]['srv_enable']:
                        self.run_call(f'service.{verb}', i, {'ha_propagate': False})

                detach_all_job = self.run_call('failover.encryption_detachall')
                detach_all_job.wait_sync()
                if detach_all_job.error:
                    self.logger.error('Failed to detach geli providers: %s', detach_all_job.error)

                rem_version = self.run_call('failover.call_remote', 'system.info')['version'].split('-')
                if len(rem_version) > 1 and rem_version[1].startswith('11'):
                    passphrase = self.run_call('failover.call_remote', 'failover.encryption_getkey')
                    self.run_call(
                        'failover.update_encryption_keys', {
                            'pools': [
                                {'name': p['name'], 'passphrase': passphrase or ''}
                                for p in self.middleware.call_sync('pool.query', [('encrypt', '=', 2)])
                            ],
                            'sync_keys': False,
                        }
                    )
                else:
                    self.run_call('failover.call_remote', 'failover.sync_keys_to_remote_node')

        except AlreadyLocked:
            self.logger.warn('Failover event handler failed to acquire backup lockfile')


async def vrrp_fifo_hook(middleware, data):

    # `data` is a single line separated by whitespace for a total of 4 words.
    # we ignore the 1st word (vrrp instance) and 4th word (priority)
    # since both of them are static
    data = data.split()

    iface = data[1].strip('"')  # interface
    state = data[2]  # the state that is being transititoned to

    middleware.send_event(
        'failover.vrrp_event',
        'CHANGED',
        fields={
            'iface': iface,
            'state': state,
        }
    )

    await middleware.call('failover.event', iface, state)


def setup(middleware):
    middleware.event_register('failover.vrrp_event', 'Sent when a VRRP state changes.')
    middleware.register_hook('vrrp.fifo', vrrp_fifo_hook)
