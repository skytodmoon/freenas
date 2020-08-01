"""
Default to ipv4 for openvpn protocol

Revision ID: 639842c043f1
Revises: 6d3efdc7ba5b
Create Date: 2020-05-04 21:19:22.658721-07:00

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = '639842c043f1'
down_revision = '6d3efdc7ba5b'
branch_labels = None
depends_on = None


def upgrade():
    conn = op.get_bind()
    for vpn in conn.execute("SELECT * FROM services_openvpnserver").fetchall():
        conn.execute("UPDATE services_openvpnserver SET protocol = ? WHERE id = ?", (
            f'{vpn["protocol"]}4', vpn['id']
        ))

    for vpn in conn.execute("SELECT * FROM services_openvpnclient").fetchall():
        conn.execute("UPDATE services_openvpnclient SET protocol = ? WHERE id = ?", (
            f'{vpn["protocol"]}4', vpn['id']
        ))
