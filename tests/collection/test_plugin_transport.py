"""A plugin dialled over a network must present credentials.

Mode 1 is a child process on loopback - the kernel is the boundary. Mode 2 under
k8s or systemd is a wire, and the broker means the plugin also dials back INTO
the host, so plaintext there is an unauthenticated callback surface rather than
just an unencrypted read.
"""

import pytest

from tsigma.plugins.manifest import ManifestError, spec_from_manifest
from tsigma.plugins.transport import (
    TLSConfig,
    TransportSecurityError,
    is_local,
    require_transport_security,
)

CA = "/etc/certs/cluster-ca.pem"


def _manifest(address, **extra):
    return {
        "name": "asc3", "process_model": "external", "subsystems": ["decoder"],
        "handshake": {
            "core_version": 1, "app_version": 1,
            "network": "tcp", "address": address,
        },
        **extra,
    }


class TestWhatCountsAsLocal:
    @pytest.mark.parametrize("target", [
        "127.0.0.1:7001", "localhost:7001", "[::1]:7001", "unix:/run/p.sock",
    ])
    def test_loopback_and_sockets_are_local(self, target):
        assert is_local(target)

    @pytest.mark.parametrize("target", [
        "10.244.3.17:7001",                        # a pod IP
        "decoder.plugins.svc.cluster.local:7001",  # cluster DNS
        "192.168.1.40:7001",                       # another box on the LAN
    ])
    def test_anything_routable_is_not(self, target):
        assert not is_local(target)

    def test_a_dns_name_is_not_assumed_local(self):
        # It may well resolve to loopback. The host cannot know that when the
        # manifest is read, and must not guess in the permissive direction.
        assert not is_local("plugins.internal:7001")


class TestItFailsClosed:
    def test_a_networked_target_without_credentials_is_refused(self):
        with pytest.raises(TransportSecurityError, match="not loopback"):
            require_transport_security("10.244.3.17:7001", None)

    def test_loopback_without_credentials_is_fine(self):
        require_transport_security("127.0.0.1:7001", None)

    def test_the_refusal_names_the_address(self):
        with pytest.raises(TransportSecurityError, match="10.244.3.17:7001"):
            require_transport_security("10.244.3.17:7001", None)

    def test_a_manifest_is_refused_at_parse_not_at_dial(self):
        # So one bad manifest is skipped by name and the rest still load,
        # rather than taking startup down when the connection is built.
        with pytest.raises(ManifestError, match="not loopback"):
            spec_from_manifest(_manifest("10.244.3.17:7001"), source="asc3.toml")

    def test_credentials_make_it_acceptable(self):
        spec = spec_from_manifest(
            _manifest("10.244.3.17:7001", tls={"ca": CA}), source="asc3.toml",
        )
        assert spec.tls.ca == CA


class TestSelfSignedNeedsNoWeakening:
    """Trusting one self-signed CA is stricter than a public bundle, not looser."""

    def test_a_self_signed_ca_is_just_a_ca(self):
        spec = spec_from_manifest(
            _manifest("decoder.plugins.svc:7001", tls={"ca": "/etc/certs/self-signed.pem"}),
            source="x",
        )
        assert spec.tls.ca == "/etc/certs/self-signed.pem"

    def test_a_name_override_handles_a_cert_issued_for_the_service(self):
        # The everyday k8s mismatch: cert says the service name, host dials a pod IP.
        spec = spec_from_manifest(
            _manifest("10.244.3.17:7001",
                      tls={"ca": CA, "server_name": "decoder.plugins.svc"}),
            source="x",
        )
        assert spec.tls.channel_options() == [
            ("grpc.ssl_target_name_override", "decoder.plugins.svc")
        ]

    def test_no_override_means_no_channel_options(self):
        assert TLSConfig(ca=CA).channel_options() == []


class TestMutualTLS:
    def test_a_client_certificate_needs_its_key(self):
        with pytest.raises(TransportSecurityError, match="both"):
            TLSConfig(ca=CA, cert="/etc/certs/host.pem").credentials()

    def test_a_key_needs_its_certificate(self):
        with pytest.raises(TransportSecurityError, match="both"):
            TLSConfig(ca=CA, key="/etc/certs/host-key.pem").credentials()

    def test_a_tls_block_needs_a_ca(self):
        with pytest.raises(ManifestError, match="ca"):
            spec_from_manifest(
                _manifest("10.244.3.17:7001", tls={"cert": "/c.pem"}), source="x",
            )
