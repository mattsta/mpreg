# Project History

This document maintains a high-level log of architectural changes and significant feature implementations.

## 2025-07-03

### `feat: Standardize RPC command representation`

**Purpose:** To improve code clarity, reduce redundancy, and establish a single source of truth for how commands are represented within the system.

**Reasoning:** The original implementation had separate and inconsistent data structures for commands in the client and server (`Command` in client, `RPCFun` in server). This created unnecessary complexity and cognitive overhead. By introducing a single, shared `RPCCommand` model in `mpreg/model.py`, we establish a clear, unified data transfer object for all RPC communications. This change is a foundational step toward a more maintainable and extensible command management system, simplifying future development and reducing the likelihood of errors.
