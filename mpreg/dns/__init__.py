"""DNS interoperability gateway for MPREG."""

from .names import decode_node_id, encode_node_id
from .resolver import DnsResolver, DnsResolverConfig
from .server import DnsGateway

__all__ = [
    "DnsGateway",
    "DnsResolver",
    "DnsResolverConfig",
    "encode_node_id",
    "decode_node_id",
]
