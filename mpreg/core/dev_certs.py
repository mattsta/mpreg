"""Dev/self-signed certificate helpers for local TLS curriculum (Phase J F12).

Generates an ephemeral CA + server (and optional client) certificate chain
suitable for ``wss://127.0.0.1`` drills. **Not for production.**
"""

from __future__ import annotations

import datetime
import ipaddress
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


@dataclass(frozen=True, slots=True)
class DevTlsMaterial:
    """Paths to generated PEM material + the owning temp directory."""

    directory: Path
    ca_cert: Path
    ca_key: Path
    server_cert: Path
    server_key: Path
    client_cert: Path
    client_key: Path

    def cleanup(self) -> None:
        """Remove the temp directory tree (best-effort)."""
        shutil.rmtree(self.directory, ignore_errors=True)


def _name(common_name: str) -> x509.Name:
    return x509.Name(
        [
            x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MPREG Dev"),
            x509.NameAttribute(NameOID.COMMON_NAME, common_name),
        ]
    )


def _key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def _write_cert(path: Path, cert: x509.Certificate) -> None:
    path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))


def _write_key(path: Path, key: rsa.RSAPrivateKey) -> None:
    path.write_bytes(
        key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def generate_dev_tls_material(
    *,
    directory: Path | str | None = None,
    common_name: str = "localhost",
    days: int = 2,
) -> DevTlsMaterial:
    """Create a mini CA, server cert (SAN localhost/127.0.0.1), and client cert.

    When *directory* is omitted a fresh temp dir is created. Caller owns cleanup
    via :meth:`DevTlsMaterial.cleanup` (or OS temp cleanup on process exit).

    Certificates include Subject Key Identifier / Authority Key Identifier so
    modern OpenSSL (3.x) path building accepts the chain.
    """
    root = (
        Path(directory)
        if directory is not None
        else Path(tempfile.mkdtemp(prefix="mpreg-dev-tls-"))
    )
    root.mkdir(parents=True, exist_ok=True)

    now = datetime.datetime.now(datetime.UTC)
    until = now + datetime.timedelta(days=days)

    ca_key = _key()
    ca_name = _name("MPREG Dev CA")
    ca_ski = x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key())
    ca_builder = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(until)
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_cert_sign=True,
                crl_sign=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(ca_ski, critical=False)
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
    )
    ca_cert = ca_builder.sign(ca_key, hashes.SHA256())

    server_key = _key()
    server_name = _name(common_name)
    server_builder = (
        x509.CertificateBuilder()
        .subject_name(server_name)
        .issuer_name(ca_name)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(until)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.DNSName(common_name),
                    x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
                    x509.IPAddress(ipaddress.IPv6Address("::1")),
                ]
            ),
            critical=False,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(server_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(ca_ski),
            critical=False,
        )
    )
    server_cert = server_builder.sign(ca_key, hashes.SHA256())

    client_key = _key()
    client_name = _name("mpreg-dev-client")
    client_builder = (
        x509.CertificateBuilder()
        .subject_name(client_name)
        .issuer_name(ca_name)
        .public_key(client_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now)
        .not_valid_after(until)
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                key_encipherment=True,
                content_commitment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.CLIENT_AUTH]),
            critical=False,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(client_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_subject_key_identifier(ca_ski),
            critical=False,
        )
    )
    client_cert = client_builder.sign(ca_key, hashes.SHA256())

    material = DevTlsMaterial(
        directory=root,
        ca_cert=root / "ca.pem",
        ca_key=root / "ca.key",
        server_cert=root / "server.pem",
        server_key=root / "server.key",
        client_cert=root / "client.pem",
        client_key=root / "client.key",
    )
    _write_cert(material.ca_cert, ca_cert)
    _write_key(material.ca_key, ca_key)
    _write_cert(material.server_cert, server_cert)
    _write_key(material.server_key, server_key)
    _write_cert(material.client_cert, client_cert)
    _write_key(material.client_key, client_key)
    return material


__all__ = ["DevTlsMaterial", "generate_dev_tls_material"]
