# Project History

This document maintains a high-level log of architectural changes and significant feature implementations.

## 2025-07-03

### `feat: Standardize RPC command representation`

**Purpose:** To improve code clarity, reduce redundancy, and establish a single source of truth for how commands are represented within the system.

**Reasoning:** The original implementation had separate and inconsistent data structures for commands in the client and server (`Command` in client, `RPCFun` in server). This created unnecessary complexity and cognitive overhead. By introducing a single, shared `RPCCommand` model in `mpreg/model.py`, we establish a clear, unified data transfer object for all RPC communications. This change is a foundational step toward a more maintainable and extensible command management system, simplifying future development and reducing the likelihood of errors.

### `feat: Decouple command execution with CommandRegistry`

**Purpose:** To improve modularity and encapsulation by separating the responsibility of command storage and lookup from the main server and cluster logic.

**Reasoning:** The previous implementation mixed command registration and storage directly within the `Cluster` and `MPREGServer` classes, leading to tight coupling. By introducing a dedicated `CommandRegistry`, we create a clear, single-purpose component for managing commands. This follows the Single Responsibility Principle and makes the codebase cleaner, easier to reason about, and more extensible for future features like dynamic command loading or introspection.
