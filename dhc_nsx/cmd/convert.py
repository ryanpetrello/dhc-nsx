import argparse
import itertools
import uuid

from oslo.db.sqlalchemy import session
import sqlalchemy as sa

def convert_nsx_to_ml2(connection, dry_run=False):
    engine = session.create_engine(connection)

    def exec_q(q):
        if dry_run:
            print q
        else:
            return engine.execute(q)

    metadata = sa.MetaData()

    table_names = [
        'networks',
        'ports',
        'ml2_port_bindings',
        'ml2_network_segments',
        'portbindingports',
        'ml2_vxlan_allocations',
    ]

    tables = {
        name: sa.Table(name, metadata, autoload=True, autoload_with=engine)
        for name in table_names
    }

    # count number of networks
    networks_table = tables['networks']
    segments_table = tables['ml2_network_segments']

    networks = engine.execute(
        networks_table.outerjoin(
            segments_table,
            networks_table.c.id==segments_table.c.network_id
        ).select(
            segments_table.c.network_id==None,
            use_labels=True
        )
    ).fetchall()

    # count number of available vnis
    vnis_alloc = tables['ml2_vxlan_allocations']

    vnis = engine.execute(
        vnis_alloc.select(vnis_alloc.c.allocated==False)
    ).fetchall()

    if len(networks) > len(vnis):
        print 'There are more networks than avaialbe VNIs'
        return

    # populate ml2_network_segments
    total = len(networks)
    for index, (network, vni) in enumerate(itertools.izip(networks, vnis)):
        print 'Allocatin VNI %s/%s' % (index, total)
        q = segments_table.insert().values(
            id=str(uuid.uuid4()),
            network_id=network.networks_id,
            network_type='vxlan',
            physical_network=None,
            segmentation_id=vni.vxlan_vni,
            is_dynamic=False
        )

        retval = exec_q(q)

    if total:
        # mark vnis in-use
        subq = sa.select([segments_table.c.segmentation_id])
        subq = subq.where(segments_table.c.network_type=='vxlan')

        q = vnis_alloc.update().where(vnis_alloc.c.vxlan_vni.in_(subq))
        q = q.values(allocated=True)

        print 'Updating allocated vnis'
        exec_q(q)

    #####
    # add ml2 ports bindings
    old_bindings = tables['portbindingports']
    new_bindings = tables['ml2_port_bindings']

    # find the ports to update
    ports_to_update = engine.execute(
        old_bindings.outerjoin(
            new_bindings,
            old_bindings.c.port_id==new_bindings.c.port_id
        ).select(
            new_bindings.c.port_id==None,
            use_labels=True
        )
    ).fetchall()

    # cache segments
    ports = tables['ports']
    q = ports.join(
        segments_table,
        ports.c.network_id==segments_table.c.network_id
    ).select(use_labels=True)

    segment_cache = {
        rv.ports_id: rv.ml2_network_segments_id
        for rv in engine.execute(q).fetchall()
    }

    total = len(ports_to_update)

    for index, old_binding in enumerate(ports_to_update):
        print 'Migrating Binding %s/%s' % (index, total)
        if old_binding.portbindingports_port_id not in segment_cache:
            print 'Port %s no longer exists, skipping...' % (
                old_binding.portbindingports_port_id
            )
        else:
            q = new_bindings.insert().values(
                port_id=old_binding.portbindingports_port_id,
                host=old_binding.portbindingports_host,
                vif_type='ovs',
                driver='dhcnsx',
                segment=segment_cache[old_binding.portbindingports_port_id],
                vnic_type='normal',
                vif_details='{"port_filter": true}'
            )
            retval = exec_q(q)

        q = old_bindings.delete(
            old_bindings.c.port_id==old_binding.portbindingports_port_id,
        )
        retval = exec_q(q)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'connection',
        help='The connection url for the target db'
    )
    parser.add_argument(
        '--dry-run',
        default=False,
        action='store_true',
        help='Conduct a dry-run'
    )

    args = parser.parse_args()
    convert_nsx_to_ml2(args.connection, args.dry_run)
