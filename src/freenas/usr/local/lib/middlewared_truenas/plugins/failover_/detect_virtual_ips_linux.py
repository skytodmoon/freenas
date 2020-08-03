from middlewared.service import private, Service
from middlewared.utils import filter_list


class DetectVirtualIpStates(Service):

    class Config:
        namespace = 'failover.vip'

    @private
    async def check_failover_group(self, ifname, groups):

        """
        Check the other members (if any) in failover group for `iface`.
        """

        # get failover group id for `iface`
        group_id = [group for group, names in groups.items() if ifname in names][0]

        # get all interfaces in `group_id`
        ids = [names for group, names in groups.items() if group == group_id][0]

        # need to remove the passed in `ifname` from the list
        ids.remove(ifname)

        # we have more than one interface in the failover group
        # so check the members and see if they have their
        # respective VIP addresses assigned to them
        #
        # if the user provided VIP(s) is/are missing from the interface
        # then it's considered "BACKUP" else "MASTER" so return
        # True for healthy (MASTER) or False for not healthy (BACKUP)
        # maybe?? Not sure, need to work on networking API first
        for id in ids:
            pass

        # TODO: keepalived on scale is responsible for adding/removing the VIP
        # to the given interface. Furthermore, the "VHID" paradigm doesn't
        # apply so need to figure out how to best return "vrrp_config"
        return

    @private
    async def get_states(self, interfaces=None):

        masters, backups, inits = [], [], []

        if interfaces is None:
            interfaces = await self.middleware.call('interface.query')

        internal_ints = await self.middleware.call('failover.internal_interfaces')

        crit_ifaces = [i['id'] for i in filter_list(interfaces, [('failover_critical', '=', True)])]

        for iface in interfaces:
            if iface['name'] in internal_ints:
                continue
            if iface['name'] not in crit_ifaces:
                continue
            # TODO: need to figure out how to make as minimal amount of breaking
            # API changes to networking API to return "vrrp_config" instead of
            # "carp_config"
            if iface['state']['vrrp_config'][0]['state'] == 'MASTER':
                masters.append(iface['name'])
            if iface['state']['vrrp_config'][0]['state'] == 'BACKUP':
                backups.append(iface['name'])
            else:
                self.logger.warning(
                    'Unknown VRRP state %r for interface %s', iface['state']['carp_config'][0]['state'], iface['name']
                )

        return masters, backups, inits

    @private
    async def check_states(self, local, remote):

        # TODO
        # Read above comment in `get_states` method
        return []
