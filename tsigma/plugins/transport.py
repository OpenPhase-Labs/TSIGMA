"""Transport security for plugins the host dials over a network.

A mode-1 plugin is a child process on loopback: the kernel is the boundary and
nothing crosses a wire. A mode-2 plugin under systemd or k8s is different - the
host dials an address that may be a pod IP or a cluster DNS name, and the
`GRPCBroker` means the plugin also dials *back* into the host. Over a network,
plaintext there is not a channel, it is an unauthenticated callback surface.

So the rule this module enforces: **loopback may be plaintext, anything else must
present credentials.** A deployment cannot reach a remote plugin in the clear by
forgetting to configure TLS - it is refused at construction, before anything is
dialled, with a message naming the address that caused it.

mTLS is the intended shape rather than server-only TLS. The plugin is untrusted
(contract ADR-0007), so the host wants to know which plugin answered; and the
plugin is about to be handed a broker callback into the host, so it wants to know
the host is the host. A client certificate is what makes both directions
answerable.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from pathlib import Path

import grpc

LOOPBACK_HOSTS = {"localhost", "127.0.0.1", "::1", "[::1]"}


class TransportSecurityError(ValueError):
    """A dial target requires credentials it was not given."""


@dataclass(frozen=True)
class TLSConfig:
    """Paths to the PEM material for one plugin connection.

    `ca` alone verifies the plugin's certificate (server-side TLS). Adding
    `cert` and `key` presents the host's own certificate too, which is what lets
    the plugin authenticate the host on the broker callback.

    A SELF-SIGNED certificate needs no special handling and no weakening: point
    `ca` at the self-signed certificate itself, or at the cluster's own CA. That
    trusts exactly that one issuer and nothing else - stricter than a public CA
    bundle, not looser.

    `server_name` overrides the name the certificate is checked against. It is
    for the ordinary k8s case where the certificate is issued for a service name
    but the host dials a pod IP. The certificate is still verified in full; only
    the name it is matched against changes. There is no option here to skip
    verification, and grpc-python exposes no way to do it - which is just as
    well on a connection that hands the plugin a callback into the host.
    """

    ca: str
    cert: str | None = None
    key: str | None = None
    server_name: str | None = None

    def credentials(self) -> grpc.ChannelCredentials:
        def read(path: str | None) -> bytes | None:
            if path is None:
                return None
            data = Path(path).read_bytes()
            if not data.strip():
                raise TransportSecurityError(f"{path} is empty")
            return data

        if (self.cert is None) != (self.key is None):
            raise TransportSecurityError(
                "mutual TLS needs both 'cert' and 'key'; one without the other "
                "cannot present a client certificate",
            )
        return grpc.ssl_channel_credentials(
            root_certificates=read(self.ca),
            private_key=read(self.key),
            certificate_chain=read(self.cert),
        )

    def channel_options(self) -> list[tuple[str, str]]:
        """Channel args for this configuration; empty unless a name is overridden."""
        if self.server_name is None:
            return []
        return [("grpc.ssl_target_name_override", self.server_name)]


def host_of(target: str) -> str:
    """The host part of a dial target, without the port."""
    if target.startswith("unix:"):
        return "unix"
    host, separator, _port = target.rpartition(":")
    return (host if separator else target).strip("[]")


def is_local(target: str) -> bool:
    """True when the target cannot leave this machine.

    A unix socket is filesystem-scoped. A loopback address is kernel-scoped.
    Everything else - a pod IP, a cluster DNS name, another host - is a wire.
    """
    host = host_of(target)
    if host == "unix":
        return True
    if host in LOOPBACK_HOSTS:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        # A DNS name. It may well resolve to loopback, but the host cannot know
        # that at construction and must not assume the safe answer.
        return False


def require_transport_security(target: str, tls: TLSConfig | None) -> None:
    """Refuse a networked target with no credentials.

    Called before a channel exists, so a misconfigured deployment fails at
    startup with the address in the message rather than silently carrying
    ingestion data and a broker callback in the clear.
    """
    if tls is not None or is_local(target):
        return
    raise TransportSecurityError(
        f"{target} is not loopback and no TLS is configured; a networked plugin "
        f"connection must present credentials. Add a [tls] section with at least "
        f"a 'ca', or dial the plugin on loopback.",
    )
