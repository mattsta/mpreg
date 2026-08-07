"""
Test helper utilities for MPREG testing.

This module provides utility functions and fixtures to simplify
test setup and ensure proper resource management during testing.
"""

import asyncio
import ssl
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Protocol

from mpreg.core.port_allocator import allocate_port, port_context, port_range_context


class Shutdownable(Protocol):
    """Protocol for objects that can be shutdown."""

    async def shutdown(self) -> None: ...


class Stoppable(Protocol):
    """Protocol for objects that can be stopped."""

    async def stop(self) -> None: ...


class Closeable(Protocol):
    """Protocol for objects that can be closed."""

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ServerClientUrls:
    """Pair of server/client URLs for tests."""

    server_url: str
    client_url: str


def make_server_url(port: int, host: str = "127.0.0.1", protocol: str = "ws") -> str:
    """Create a server URL from a port number."""
    return f"{protocol}://{host}:{port}"


def make_client_url(port: int, host: str = "127.0.0.1", protocol: str = "ws") -> str:
    """Create a client connection URL from a port number."""
    return f"{protocol}://{host}:{port}"


async def wait_for_condition(
    predicate: Callable[[], bool],
    *,
    timeout: float = 5.0,
    interval: float = 0.05,
    error_message: str | None = None,
) -> None:
    """Poll a condition until it is true or a timeout is reached."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    message = error_message or f"Condition not met within {timeout:.1f}s"
    raise AssertionError(message)


@contextmanager
def test_server_url(category: str = "servers") -> Iterator[str]:
    """Context manager that provides a server URL with automatic port cleanup."""
    with port_context(category) as port:
        yield make_server_url(port)


@contextmanager
def test_client_url(category: str = "clients") -> Iterator[str]:
    """Context manager that provides a client URL with automatic port cleanup."""
    with port_context(category) as port:
        yield make_client_url(port)


test_server_url.__test__ = False
test_client_url.__test__ = False


@contextmanager
def server_cluster_urls(count: int, category: str = "servers") -> Iterator[list[str]]:
    """Context manager that provides multiple server URLs."""
    with port_range_context(count, category) as ports:
        yield [make_server_url(port) for port in ports]


@contextmanager
def server_client_pair() -> Iterator[ServerClientUrls]:
    """Context manager that provides a server URL and client URL pair."""
    with port_range_context(2, "testing") as ports:
        server_url = make_server_url(ports[0])
        client_url = make_client_url(ports[0])  # Client connects to server
        yield ServerClientUrls(server_url=server_url, client_url=client_url)


class TestPortManager:
    """
    Helper class for managing multiple ports in a single test.

    Useful for complex tests that need to manage many servers/clients
    with proper cleanup.
    """

    __test__ = False

    def __init__(self):
        self._allocated_ports: list[int] = []

    def get_server_port(self, category: str = "servers") -> int:
        """Allocate a server port."""
        port = allocate_port(category)
        self._allocated_ports.append(port)
        return port

    def get_client_port(self, category: str = "clients") -> int:
        """Allocate a client port."""
        port = allocate_port(category)
        self._allocated_ports.append(port)
        return port

    def get_server_url(self, category: str = "servers", host: str = "127.0.0.1") -> str:
        """Get a server URL with allocated port."""
        port = self.get_server_port(category)
        return make_server_url(port, host)

    def get_client_url(self, category: str = "clients", host: str = "127.0.0.1") -> str:
        """Get a client URL with allocated port."""
        port = self.get_client_port(category)
        return make_client_url(port, host)

    def get_port_range(self, count: int, category: str = "testing") -> list[int]:
        """Allocate a range of ports."""
        from mpreg.core.port_allocator import allocate_port_range

        ports = allocate_port_range(count, category)
        self._allocated_ports.extend(ports)
        return ports

    def get_server_cluster(self, count: int, host: str = "127.0.0.1") -> list[str]:
        """Get multiple server URLs."""
        ports = self.get_port_range(count, "servers")
        return [make_server_url(port, host) for port in ports]

    def cleanup(self) -> None:
        """Release all allocated ports."""
        from mpreg.core.port_allocator import release_port

        for port in self._allocated_ports:
            release_port(port)
        self._allocated_ports.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


# Convenience functions for quick URL generation
def quick_server_url(category: str = "servers") -> str:
    """Quickly allocate a server URL (manual cleanup required)."""
    port = allocate_port(category)
    return make_server_url(port)


def quick_client_url(category: str = "clients") -> str:
    """Quickly allocate a client URL (manual cleanup required)."""
    port = allocate_port(category)
    return make_client_url(port)


def quick_server_cluster(count: int, category: str = "servers") -> list[str]:
    """Quickly allocate multiple server URLs (manual cleanup required)."""
    from mpreg.core.port_allocator import allocate_port_range

    ports = allocate_port_range(count, category)
    return [make_server_url(port) for port in ports]


def create_test_ssl_context() -> ssl.SSLContext:
    """Create a test SSL context for secure transport testing.

    This creates a self-signed SSL context suitable for testing.
    DO NOT use in production - this is for testing only.

    Returns:
        SSL context configured for testing
    """
    # Create a self-signed context for testing
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)

    # For testing, we'll use a very permissive context
    # In a real implementation, you'd load proper certificates
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE

    # Generate a self-signed certificate for testing
    # This would normally be done with proper cert files
    try:
        import datetime
        import ipaddress
        import os
        import tempfile

        try:
            from cryptography import x509  # type: ignore[import-not-found]
            from cryptography.hazmat.primitives import (  # type: ignore[import-not-found]
                hashes,
                serialization,
            )
            from cryptography.hazmat.primitives.asymmetric import (
                rsa,  # type: ignore[import-not-found]
            )
            from cryptography.x509.oid import NameOID  # type: ignore[import-not-found]
        except ImportError:
            # Cryptography not available - return basic SSL context without certificate generation
            return context

        # Generate private key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )

        # Create certificate
        subject = issuer = x509.Name(
            [
                x509.NameAttribute(NameOID.COUNTRY_NAME, "US"),
                x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, "Test"),
                x509.NameAttribute(NameOID.LOCALITY_NAME, "Test"),
                x509.NameAttribute(NameOID.ORGANIZATION_NAME, "MPREG Test"),
                x509.NameAttribute(NameOID.COMMON_NAME, "localhost"),
            ]
        )

        now = datetime.datetime.now(datetime.UTC)
        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(private_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now)
            .not_valid_after(now + datetime.timedelta(days=1))
            .add_extension(
                x509.SubjectAlternativeName(
                    [
                        x509.DNSName("localhost"),
                        x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                    ]
                ),
                critical=False,
            )
            .sign(private_key, hashes.SHA256())
        )

        # Create temporary files for cert and key
        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".pem"
        ) as cert_file:
            cert_file.write(cert.public_bytes(serialization.Encoding.PEM))
            cert_path = cert_file.name

        with tempfile.NamedTemporaryFile(
            mode="wb", delete=False, suffix=".key"
        ) as key_file:
            key_file.write(
                private_key.private_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption(),
                )
            )
            key_path = key_file.name

        # Load the certificate into the context
        context.load_cert_chain(cert_path, key_path)

        # Clean up temporary files
        os.unlink(cert_path)
        os.unlink(key_path)

    except ImportError:
        # Fallback if cryptography is not available
        # Just return a basic context that allows self-signed certs
        pass
    except Exception:
        # If anything goes wrong with cert generation, just use basic context
        pass

    return context


class AsyncObjectManager:
    """
    Helper class for managing async objects that need proper shutdown.

    This provides a shared abstraction for the common pattern of managing
    async objects (like servers, adapters, managers) that need explicit
    shutdown in test fixtures.
    """

    def __init__(self):
        self._shutdownable_objects: list[Shutdownable] = []
        self._stoppable_objects: list[Stoppable] = []
        self._closeable_objects: list[Closeable] = []

    def add_shutdownable(self, obj: Shutdownable) -> Shutdownable:
        """Add an object with shutdown() method to be managed."""
        self._shutdownable_objects.append(obj)
        return obj

    def add_stoppable(self, obj: Stoppable) -> Stoppable:
        """Add an object with stop() method to be managed."""
        self._stoppable_objects.append(obj)
        return obj

    def add_closeable(self, obj: Closeable) -> Closeable:
        """Add an object with close() method to be managed."""
        self._closeable_objects.append(obj)
        return obj

    async def shutdown_all(self) -> None:
        """Shutdown all managed objects in reverse order."""
        # Shutdown in reverse order for proper dependency cleanup

        # First shutdown objects with explicit shutdown method
        for obj in reversed(self._shutdownable_objects):
            try:
                await obj.shutdown()
            except Exception as e:
                print(f"Warning: Failed to shutdown {type(obj).__name__}: {e}")

        # Then stop objects with stop method
        for stoppable_obj in reversed(self._stoppable_objects):
            try:
                await stoppable_obj.stop()
            except Exception as e:
                print(f"Warning: Failed to stop {type(stoppable_obj).__name__}: {e}")

        # Finally close objects with close method
        for closeable_obj in reversed(self._closeable_objects):
            try:
                await closeable_obj.close()
            except Exception as e:
                print(f"Warning: Failed to close {type(closeable_obj).__name__}: {e}")

    def clear(self) -> None:
        """Clear all managed objects without shutting them down."""
        self._shutdownable_objects.clear()
        self._stoppable_objects.clear()
        self._closeable_objects.clear()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.shutdown_all()
