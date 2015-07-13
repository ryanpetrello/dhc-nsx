#    Copyright 2015 Akanda, Inc.
#    All Rights Reserved
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
# Portions of this code from excerpted from
# http://git.openstack.org/cgit/openstack/neutron/tree/neutron/plugins/ \
# vmware/plugins/base.py?h=stable/juno
#
# Copyright 2012 VMware, Inc.
# All Rights Reserved
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import uuid

from oslo.config import cfg
import six

from neutron.common import constants as n_const
from neutron.common import exceptions as n_exc
from neutron.extensions import portbindings
if '_' not in __builtins__:
    # stable/juno does not use this import
    from neutron.i18n import _
from neutron import manager
from neutron.openstack.common import excutils
from neutron.openstack.common import log
from neutron.plugins.ml2 import driver_api
from neutron.plugins.vmware.api_client import exception as api_exc
from neutron.plugins.vmware.common import config # noqa
from neutron.plugins.vmware.common import exceptions as nsx_exc
from neutron.plugins.vmware.common import nsx_utils
from neutron.plugins.vmware.common import sync as nsx_sync
from neutron.plugins.vmware.dbexts import db as nsx_db
from neutron.plugins.vmware.nsxlib import switch as switchlib


LOG = log.getLogger(__name__)


class DeferredPluginRef(object):
    def __getattr__(self, name):
        return getattr(manager.NeutronManager.get_plugin(), name)


class AkandaNsxSynchronizer(nsx_sync.NsxSynchronizer):
    """
    The NsxSynchronizer class in Neutron runs a synchronization thread to
    sync nvp objects with neutron objects. Since we don't use nvp's routers
    the sync was failing making neutron showing all the routers like if the
    were in Error state. To fix this behaviour we override the two methods
    responsible for the routers synchronization in the NsxSynchronizer class
    to be a noop

    """

    def _synchronize_state(self, *args, **kwargs):
        """
        Given the complexicity of the NSX synchronization process, there are
        about a million ways for it to go wrong. (MySQL connection issues,
        transactional race conditions, etc...)  In the event that an exception
        is thrown, behavior of the upstream implementation is to immediately
        report the exception and kill the synchronizer thread.

        This makes it very difficult to detect failure (because the thread just
        ends) and the problem can only be fixed by completely restarting
        neutron.

        This implementation changes the behavior to repeatedly fail (and retry)
        and log verbosely during failure so that the failure is more obvious
        (and so that auto-recovery is a possibility if e.g., the database
        comes back to life or a network-related issue becomes resolved).
        """
        try:
            return nsx_sync.NsxSynchronizer._synchronize_state(
                self, *args, **kwargs
            )
        except:
            LOG.exception("An error occurred while communicating with "
                          "NSX backend. Will retry synchronization "
                          "in %d seconds" % self._sync_backoff)
            self._sync_backoff = min(self._sync_backoff * 2, 64)
            return self._sync_backoff
        else:
            self._sync_backoff = 1

    def _synchronize_lrouters(self, *args, **kwargs):
        pass

    def synchronize_router(self, *args, **kwargs):
        pass


class NSXMechDriver(driver_api.MechanismDriver):
    '''NSX ML2 MechanismDriver for Neutron'''

    def initialize(self):
        self.vif_type = portbindings.VIF_TYPE_OVS
        self.vif_details = {portbindings.CAP_PORT_FILTER: True}

        config.validate_config_options()

        # need sec group handler?

        # start sync thread here
        self.nsx_opts = cfg.CONF.NSX
        self.nsx_sync_opts = cfg.CONF.NSX_SYNC
        self.cluster = nsx_utils.create_nsx_cluster(
            cfg.CONF,
            self.nsx_opts.concurrent_connections,
            self.nsx_opts.nsx_gen_timeout
        )

        self._synchronize = AkandaNsxSynchronizer(
            DeferredPluginRef(),
            self.cluster,
            self.nsx_sync_opts.state_sync_interval+1000,
            self.nsx_sync_opts.min_sync_req_delay,
            self.nsx_sync_opts.min_chunk_size,
            self.nsx_sync_opts.max_random_sync_delay
        )

    def _convert_to_transport_zones(self, network=None, bindings=None):
        return nsx_utils.convert_to_nsx_transport_zones(
            self.cluster.default_tz_uuid,
            network,
            bindings,
            default_transport_type=cfg.CONF.NSX.default_transport_type
        )

    def _find_lswitch(self, context, network_id):
        max_ports = self.nsx_opts.max_lp_per_overlay_ls

        lswitches = nsx_utils.fetch_nsx_switches(
            context._plugin_context.session,
            self.cluster,
            network_id
        )

        try:
            return [
                ls for ls in lswitches
                if (ls['_relations']['LogicalSwitchStatus']['lport_count'] <
                    max_ports)].pop(0)
        except IndexError as e:
            with excutils.save_and_reraise_exception():
                LOG.debug('No switch has available ports (%d checked)',
                          len(lswitches))

    def _convert_to_nsx_secgroup_ids(self, context, security_groups):
        return [
            nsx_utils.get_nsx_security_group_id(
                context._plugin_context.session,
                self.cluster,
                neutron_sg_id)
                for neutron_sg_id in security_groups
        ]

    def create_network_precommit(self, context):
        """Add a network to NSX

        This method does not handle provider networks correctly and
        is out-of-scope for now.
        """
        net_data = context.current

        if net_data['admin_state_up'] is False:
             LOG.warning(_("Network with admin_state_up=False are not yet "
                           "supported by this plugin. Ignoring setting for "
                           "network %s"), net_data.get('name', '<unknown>'))

        transport_zone_config = self._convert_to_transport_zones(net_data)

        nsx_switch = switchlib.create_lswitch(
            self.cluster,
            net_data['id'],
            net_data['tenant_id'],
            net_data.get('name'),
            transport_zone_config,
            shared=bool(net_data.get('shared'))
        )

        nsx_db.add_neutron_nsx_network_mapping(
           context._plugin_context.session,
           net_data['id'],
           nsx_switch['uuid']
        )

    def update_network_precommit(self, context):
        if context.original['name'] != context.current['name']:
            nsx_switch_ids = nsx_utils.get_nsx_switch_ids(
               context._plugin_context.session,
               self.cluster,
               context.current['id']
            )

            if not nsx_switch_ids or len(nsx_switch_ids) < 1:
                 LOG.warn(_("Unable to find NSX mappings for neutron "
                             "network:%s"), context.original['id'])

            try:
                switchlib.update_lswitch(
                    self.cluster,
                    lswitch_ids[0],
                    context.current['name']
                )
            except api_exc.NsxApiException as e:
                 LOG.warn(_("Logical switch update on NSX backend failed. "
                            "Neutron network id:%(net_id)s; "
                            "NSX lswitch id:%(lswitch_id)s;"
                            "Error:%(error)s"),
                            {'net_id': context.current['id'],
                             'lswitch_id': lswitch_ids[0],
                             'error': e})

    def delete_network_precommit(self, context):
        nsx_switch_ids = nsx_utils.get_nsx_switch_ids(
           context._plugin_context.session,
           self.cluster,
           context.current['id']
        )

        try:
            switchlib.delete_networks(
                self.cluster,
                context.current['id'],
                nsx_switch_ids
            )
        except n_exc.NotFound:
             LOG.warning(_("The following logical switches were not found "
                           "on the NSX backend:%s"), nsx_switch_ids)


    def create_port_precommit(self, context):
        #TODO: mac_learning

        port_data = context.current

        if port_data['device_owner'] == n_const.DEVICE_OWNER_FLOATINGIP:
            return  # no need to process further for fip

        nsx_port = None
        nsx_switch = None

        nsx_switch = self._find_lswitch(
            context,
            port_data['network_id']
        )

        nsx_sec_profile_ids = self._convert_to_nsx_secgroup_ids(
            context,
            port_data.get('security_groups') or []
        )

        nsx_port = switchlib.create_lport(
            self.cluster,
            nsx_switch['uuid'],
            port_data['tenant_id'],
            port_data['id'],
            port_data['name'],
            port_data['device_id'],
            port_data['admin_state_up'],
            port_data['mac_address'],
            port_data['fixed_ips'],
            port_security_enabled=port_data['port_security_enabled'],
            security_profiles=nsx_sec_profile_ids,
            mac_learning_enabled=None,  # TODO
            allowed_address_pairs=port_data['allowed_address_pairs']
        )

        nsx_db.add_neutron_nsx_port_mapping(
            context._plugin_context.session,
            port_data['id'],
            nsx_switch['uuid'],
            nsx_port['uuid']
        )

        if port_data['device_owner']:
            switchlib.plug_vif_interface(
                self.cluster,
                nsx_switch['uuid'],
                nsx_port['uuid'],
                "VifAttachment",
                port_data['id']
            )


        LOG.debug("port created on NSX backend for tenant "
                  "%(tenant_id)s: (%(id)s)", port_data)

    def update_port_precommit(self, context):
        #TODO: mac_learning

        port_data = context.current

        nsx_switch_id, nsx_port_id = nsx_utils.get_nsx_switch_and_port_id(
            context._plugin_context.session,
            self.cluster,
            port_data['id']
        )

        nsx_sec_profile_ids = self._convert_to_nsx_secgroup_ids(
            context,
            port_data.get('security_groups') or []
        )

        # ensure port_security_enabled flag set

        if nsx_switch_id:
            switchlib.update_port(
                self.cluster,
                nsx_switch_id,
                nsx_port_id,
                port_data['id'],
                port_data['tenant_id'],
                port_data['name'],
                port_data['device_id'],
                port_data['admin_state_up'],
                port_data['mac_address'],
                port_data['fixed_ips'],
                port_security_enabled=port_data['port_security_enabled'],
                security_profiles=nsx_sec_profile_ids,
                mac_learning_enabled=None, # TODO
                allowed_address_pairs=port_data['allowed_address_pairs']
            )

    def delete_port_precommit(self, context):
        port_data = context.current

        if port_data['device_owner'] == n_const.DEVICE_OWNER_FLOATINGIP:
             return  # no need to process further for fip


        nsx_switch_id, nsx_port_id = nsx_utils.get_nsx_switch_and_port_id(
            context._plugin_context.session,
            self.cluster,
            port_data['id']
        )

        try:
            switchlib.delete_port(self.cluster, nsx_switch_id, nsx_port_id)
            LOG.debug(
                "_nsx_delete_port completed for port %(port_id)s on network "
                "%(net_id)s",
                {'port_id': port_data['id'], 'net_id': port_data['network_id']}
            )
        except n_exc.NotFound:
            LOG.warning(_("Port %s not found in NSX"), port_data['id'])

    def bind_port(self, context):
        # TODO: handle more than 1 segment
        segment = context.network.network_segments[0]
        context.set_binding(
            segment[driver_api.ID],
            self.vif_type,
            self.vif_details,
            status=n_const.PORT_STATUS_ACTIVE
        )

        LOG.debug("Bound using segment: %s", segment)
