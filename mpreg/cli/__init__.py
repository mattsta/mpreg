"""
MPREG Command Line Interface.

This package provides comprehensive CLI tools for managing MPREG federation
clusters, monitoring health, and performing administrative operations.
"""

from .federation_cli import FederationCLI
from .main import cli, main

__all__ = ["FederationCLI", "main", "cli"]
