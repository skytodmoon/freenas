# Copyright (c) 2020 iXsystems, Inc.
# All rights reserved.
# This file is a part of TrueNAS
# and may not be copied and/or distributed
# without the express permission of iXsystems.

from collections import defaultdict
from threading import Lock
import os
import time
import contextlib
import shutil
import signal
import logging

from middlewared.utils import filter_list
from middlewared.service import Service, private

logger = logging.getLogger('failover')


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

# This file is managed in unscheduled_reboot_alert.py
# Ticket 39114
WATCHDOG_ALERT_FILE = "/data/sentinels/.watchdog-alert"

# this is the time limit we place on exporting the
# zpool(s) when becoming the BACKUP node
ZPOOL_EXPORT_TIMEOUT = 4  # seconds


class ZpoolExportTimeout(Exception):

    """
    This is raised if we can't export the
    zpool(s) from the system when becoming
    BACKUP
    """
    pass


class IgnoreFailoverEvent(Exception):

    """
    This is raised when a failover event is ignored.
    """
    pass


class FailoverService(Service):

    def __init__(self):

        # boolean that represents if a failover event was successful or not
        self.failover_successful = False

        # list of critical services that get restarted first
        # before the other services during a failover event
        self.critical_services = ['iscsitarget', 'cifs', 'nfs', 'afp']

        # option to be given when changing the state of a service
        # during a failover event, we do not want to replicate
        # the state of a service to the other controller since
        # that's being handled by us explicitly
        self.ha_propagate = {'ha_propagate': False}

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
        self.event_lock = Lock()

    @private
    def run_call(self, method, *args, **kwargs):
        try:
            return self.middleware.call_sync(method, *args, **kwargs)
        except Exception as e:
            logger.error('Failed to run %s:%r:%r %s', method, args, kwargs, e)

    @private
    def event(self, ifname, event):

        refresh = True
        try:
            return self._event(ifname, event)
        except IgnoreFailoverEvent:
            refresh = False
        finally:
            # refreshing the failover status can cause delays in failover
            # there is no reason to refresh it if the event has been ignored
            if refresh:
                self.run_call('failover.status_refresh')

    def _zpool_export_sig_alarm(self, sig, tb):

        raise ZpoolExportTimeout()

    @private
    def generate_failover_data(self):

        # only care about name, guid, and status
        volumes = self.run_call(
            'pool.query', [], {
                'select': ['name', 'guid', 'status']
            }
        )

        # get list of all services on system
        # we query db directly since on SCALE calling `service.query`
        # actually builds a list of all services and includes if they're
        # running or not. Probing all services on the system to see if
        # they're running takes longer than what we need since failover
        # needs to be as fast as possible.
        services = self.run_call('datastore.query', 'services_services')

        failovercfg = self.run_call('failover.config')
        interfaces = self.run_call('interface.query')
        internal_ints = self.run_call('failover.internal_interfaces')

        data = {
            'services': services,
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
        if self.event_lock.locked():
            logger.warning('Failover event is already being processed, ignoring.')
            raise IgnoreFailoverEvent()

        forcetakeover = False
        if event == 'forcetakeover':
            forcetakeover = True

        # generate data to be used during the failover event
        fobj = self.generate_failover_data()

        # grab the primitive lock
        with self.event_lock:
            if not forcetakeover:
                if fobj['disabled'] and not fobj['master']:
                    # if forcetakeover is false, and failover is disabled
                    # and we're not set as the master controller, then
                    # there is nothing we need to do.
                    logger.warning('Failover is disabled, assuming backup.')
                    self.run_call('service.restart', 'keepalived')
                    return

                # any logic below here means failover is disabled and we are
                # designated as the master controller so act accordingly

                # If there is a state change on a non-critical interface then
                # ignore the event and return
                ignore = [i for i in fobj['non_crit_interfaces'] if i in ifname]
                if ignore:
                    logger.warning(f'Ignoring state change on non-critical interface:{ifname}.')
                    raise IgnoreFailoverEvent()

                # if the other controller is already master, then assume backup
                try:
                    if self.call_sync('failover.call_remote', 'failover.status') == 'MASTER':
                        logger.warning('Other node is already active, assuming backup.')
                        self.run_call('service.restart', 'keepalived')
                        return
                except Exception:
                    logger.error('Failed to contact the other node', exc_info=True)

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
                            logger.error('Failed contacting standby controller when restarting vrrp.', exc_info=True)
                        break

                # means all zpools are already imported so nothing else to do
                if not needs_imported:
                    logger.warning('Failover disabled but zpool(s) are imported. Assuming active.')
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
            logger.warning('Forcefully taking over as the MASTER node.')
            fenced_error = self.run_call('failover.fenced.start', force=True)
        else:
            # we need to check a few things before we start fenced and start the process
            # of becoming master
            #
            #   1. if the interface that we've received a MASTER event for is
            #       in a failover group with another interface. If ANY of the
            #       other interfaces in that failover group are still MASTER,
            #       then we need to ignore the event.
            #
            #   TODO: Not sure how keepalived and laggs operate so need to test this
            #           (maybe the event only gets triggered if the lagg goes down)
            #   2. if the interfaces that we've received a MASTER event for is
            #       a member of a lagg interface and the other members in that lagg
            #       are functional, then we need to ignore the event.
            #
            if not self.run_call('failover.vip.check_failover_group', ifname, fobj['groups']):
                pass

            logger.warning(f'Entering MASTER on {ifname}.')
            fenced_error = self.run_call('failover.fenced.start')

        # starting fenced daemon failed....which is bad
        # emit an error and exit
        if fenced_error:
            if fenced_error == 1:
                logger.error('Failed to register keys on disks, exiting!')
            elif fenced_error == 2:
                logger.error('Fenced is running on the remote node, exiting!')
            elif fenced_error == 3:
                logger.error('10% or more of the disks failed to be reserved, exiting!')
            elif fenced_error == 5:
                logger.error('Fenced encountered an unexpected fatal error, exiting!')
            else:
                logger.error(f'Fenced exited with code:{fenced_error} which should never happen, exiting!')

            return self.failover_successful

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
            logger.info(f'Importing {vol["name"]}')

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
                logger.error(f'Error unlocking ZFS encrypted datasets: {unlock_job.error}')
            elif unlock_job.result['failed']:
                logger.error('Failed to unlock %s ZFS encrypted dataset(s)', ','.join(unlock_job.result['failed']))

        # if we fail to import all zpools then alert the user because nothing
        # is going to work at this point
        if len(failed) == len(fobj['volumes']):
            for i in failed:
                logger.error(
                    f'Failed to import volume with name:{failed["name"]} with guid:{failed["guid"]} '
                    'with error:{failed["error"]}'
                )

            logger.error('All volumes failed to import!')
            return self.failover_successful

        # if we fail to import any of the zpools then alert the user but continue the process
        for i in failed:
            logger.error(
                f'Failed to import volume with name:{failed["name"]} with guid:{failed["guid"]} '
                'with error:{failed["error"]}. '
                'However, other zpools imported so we continued the failover process.'
            )

        logger.info('Volume imports complete.')

        # need to make sure failover status is updated in the middleware cache
        logger.info('Refreshing failover status')
        self.run_call('failover.status_refresh')

        # this enables all necessary services that have been enabled by the user
        logger.info('Enabling necessary services.')
        self.run_call('etc.generate', 'rc')

        logger.info('Configuring system dataset')
        self.run_call('etc.generate', 'system_dataset')

        # Write the certs to disk based on what is written in db.
        self.run_call('etc.generate', 'ssl')
        # Now we restart the appropriate services to ensure it's using correct certs.
        self.run_call('service.restart', 'http')

        # now we restart the services, prioritizing the "critical" services
        logger.info('Restarting critical services.')
        for i in self.critical_services:
            for j in fobj['services']:
                if i == j['srv_service'] and j['srv_enable']:
                    logger.info(f'Restarting critical service:{i}')
                    self.run_call('service.restart', i, self.ha_propagate)

        # TODO: look at nftables
        # logger.info('Allowing network traffic.')
        # run('/sbin/pfctl -d')

        logger.info('Critical portion of failover is now complete')

        # regenerate cron
        logger.info('Regenerating cron')
        self.run_call('etc.generate', 'cron')

        # sync disks is disabled on passive node
        logger.info('Syncing disks')
        self.run_call('disk.sync_all')

        logger.info('Syncing enclosure')
        self.run_call('enclosure.sync_zpool')

        # restart the remaining "non-critical" services
        logger.info('Restarting remaining services')

        logger.info('Restarting collected')
        self.run_call('service.restart', 'collectd', self.ha_propagate)

        logger.info('Restarting syslog-ng')
        self.run_call('service.restart', 'syslogd', self.ha_propagate)

        for i in fobj['services']:
            if i['srv_service'] not in self.critical_services and i['srv_enable']:
                logger.info('Restarting service:{i["srv_service"]}')
                self.run_call('service.restart', i['srv_service'], self.ha_propagate)

        # TODO: jails don't exist on SCALE (yet)
        # TODO: vms don't exist on SCALE (yet)
        # self.run_call('jail.start_on_boot')
        # self.run_call('vm.start_on_boot')

        logger.info('Initializing alert system')
        self.run_call('alert.block_failover_alerts')
        self.run_call('alert.initialize', False)

        kmip_config = self.run_call('kmip.config')
        if kmip_config and kmip_config['enabled']:
            logger.info('Syncing encryption keys with KMIP server')

            # Even though we keep keys in sync, it's best that we do this as well
            # to ensure that the system is up to date with the latest keys available
            # from KMIP. If it's unaccessible, the already synced memory keys are used
            # meanwhile.
            self.run_call('kmip.initialize_keys')

        logger.info('Failover event complete.')
        self.failover_successful = True

        return self.failover_successful

    @private
    def vrrp_backup(self, fobj, ifname, event, force_fenced):

        logger.warning(f'Entering BACKUP on {ifname}')

        # we need to stop fenced first
        self.run_call('failover.fenced.stop')

        # restarting keepalived sends a priority 0 advertisement
        # which means any VIP that is on this controller will be
        # migrated to the other controller
        logger.info('Transitioning all VIPs off this node')
        self.run_call('service.restart', 'keepalived')

        # TODO: look at nftables
        # logger.info('Enabling firewall')
        # run('/sbin/pfctl -ef /etc/pf.conf.block')

        # ticket 23361 enabled a feature to send email alerts when an unclean reboot occurrs.
        # TrueNAS HA, by design, has a triggered unclean shutdown.
        # If a controller is demoted to standby, we set a 4 sec countdown using watchdog.
        # If the zpool(s) can't export within that timeframe, we use watchdog to violently reboot the controller.
        # When this occurrs, the customer gets an email about an "Unauthorized system reboot".
        # The idea for creating a new sentinel file for watchdog related panics,
        # is so that we can send an appropriate email alert.
        # So if we panic here, middleware will check for this file and send an appropriate email.
        # ticket 39114
        with contextlib.suppress(Exception):
            with open(WATCHDOG_ALERT_FILE, 'w') as f:
                f.write(int(time.time()))
                f.flush()  # be sure it goes straight to disk
                os.fsync(f.fileno())  # be EXTRA sure it goes straight to disk

        # set a countdown = to ZPOOL_EXPORT_TIMEOUT.
        # if we can't export the zpool(s) in this timeframe,
        # we send the 'b' character to the /proc/sysrq-trigger
        # to trigger an immediate reboot of the system without
        # syncing anything to disk or stopping any userland services.
        # https://www.kernel.org/doc/html/latest/admin-guide/sysrq.html
        signal.signal(signal.SIGALRM, self._zpool_export_sig_alarm)
        try:
            signal.alarm(ZPOOL_EXPORT_TIMEOUT)
            # export the zpool(s)
            for vol in fobj['volumes']:
                if vol['status'] == 'ONLINE':
                    self.run_call('zfs.pool.export', vol['name'])
                    logger.info(f'Exported {vol["name"]}')
        except ZpoolExportTimeout:
            # have to enable the "magic" sysrq triggers
            with open('/proc/sys/kernel/sysrq') as f:
                f.write('1')

            # now violently reboot
            with open('/proc/sysrq-trigger') as f:
                f.write('b')

        # We also remove this file here, because on boot we become BACKUP if the other
        # controller is MASTER. So this means we have no volumes to export which means
        # the `ZPOOL_EXPORT_TIMEOUT` is honored.
        with contextlib.suppress(Exception):
            os.unlink(WATCHDOG_ALERT_FILE)

        logger.info('Refreshing failover status')
        self.run_call('failover.status_refresh')

        logger.info('Restarting syslog-ng')
        self.run_call('service.restart', 'syslogd', self.ha_propagate)

        logger.info('Regenerating cron')
        self.run_call('etc.generate', 'cron')

        logger.info('Stopping smartd')
        self.run_call('service.stop', 'smartd', self.ha_propagate)

        logger.info('Stopping collectd')
        self.run_call('service.stop', 'collectd', self.ha_propagate)

        # we keep SSH running on both controllers (if it's enabled by user)
        for i in fobj['services']:
            if i['srv_service'] == 'ssh' and i['srv_enable']:
                logger.info('Restarting SSH')
                self.run_call('service.restart', 'ssh', self.ha_propagate)

        # TODO: ALUA on SCALE??
        # do something with iscsi service here

        logger.info('Syncing encryption keys from MASTER node (if any)')
        self.run_call('failover.call_remote', 'failover.sync_keys_to_remote_node')

        logger.info('Successfully became the BACKUP node.')
        self.failover_successful = True

        return self.failover_successful


async def vrrp_fifo_hook(middleware, data):

    # `data` is a single line separated by whitespace for a total of 4 words.
    # we ignore the 1st word (vrrp instance) and 4th word (priority)
    # since both of them are static
    data = data.split()

    iface = data[1].strip('"')  # interface
    state = data[2]  # the state that is being transititoned to

    # we only care about MASTER or BACKUP events currently
    if state not in ('MASTER', 'BACKUP'):
        return

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
