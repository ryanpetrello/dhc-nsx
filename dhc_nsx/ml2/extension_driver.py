from neutron.api.v2 import attributes as attr
from neutron.db import portsecurity_db
from neutron.extensions import allowedaddresspairs as addr_pair
from neutron.extensions import portsecurity as psec
from neutron import manager
from neutron.plugins.ml2 import driver_api


class PortSecurityShim(portsecurity_db.PortSecurityDbMixin):
    """ A composite class to avoid re-implementing the mixin."""
    def __getattr__(self, name):
        return getattr(manager.NeutronManager.get_plugin(), name)

class FakeContext(object):
    def __init__(self, session):
        self.session = session
        self.is_admin = False

class PortSecurityExtension(driver_api.ExtensionDriver):
    def initialize(self):
        self.shim = PortSecurityShim()

    @property
    def extension_alias(self):
        return 'port-security'

    def process_create_network(self, session, data, result):
        self.shim._process_network_port_security_create(
            FakeContext(session),
            data,
            result
        )

    def process_update_network(self, session, data, result):
        self.shim._process_network_security_update(
            FakeContext(session),
            data,
            result
        )

    def _process_port(self, func_name, session, data, result):
        port_security, has_ip = self.shim._determine_port_security_and_has_ip(
            FakeContext(session),
            data
        )
        data[psec.PORTSECURITY] = port_security
        if attr.is_attr_set(data.get(addr_pair.ADDRESS_PAIRS)):
            if not port_security:
                raise addr_pair.AddressPairAndPortSecurityRequired()

        getattr(self.shim, func_name)(FakeContext(session), data, result)

    def process_create_port(self, session, data, result):
        self._process_port(
            '_process_port_port_security_create',
            session,
            data,
            result
        )

    def process_update_port(self, session, data, result):
        if psec.PORTSECURITY in data:
            self._process_port(
                '_process_port_port_security_update',
                session,
                data,
                result
            )

    def extend_network_dict(self, session, result):
        pass  # skipping because importing the mixin attaches mixin hooks

    def extend_port_dict(self, session, result):
        # TODO: investigate whether is this runs port security multiple times
        if psec.PORTSECURITY not in result:
            result[psec.PORTSECURITY] = self.shim._get_port_security_binding(
                FakeContext(session),
                result['id']
            )
