# Project History

This document maintains a high-level log of architectural changes and significant feature implementations.

## 2025-07-03

### `feat: Standardize RPC command representation`

**Purpose:** To improve code clarity, reduce redundancy, and establish a single source of truth for how commands are represented within the system.

**Reasoning:** The original implementation had separate and inconsistent data structures for commands in the client and server (`Command` in client, `RPCFun` in server). This created unnecessary complexity and cognitive overhead. By introducing a single, shared `RPCCommand` model in `mpreg/model.py`, we establish a clear, unified data transfer object for all RPC communications. This change is a foundational step toward a more maintainable and extensible command management system, simplifying future development and reducing the likelihood of errors.

### `feat: Decouple command execution with CommandRegistry`

**Purpose:** To improve modularity and encapsulation by separating the responsibility of command storage and lookup from the main server and cluster logic.

**Reasoning:** The previous implementation mixed command registration and storage directly within the `Cluster` and `MPREGServer` classes, leading to tight coupling. By introducing a dedicated `CommandRegistry`, we create a clear, single-purpose component for managing commands. This follows the Single Responsibility Principle and makes the codebase cleaner, easier to reason about, and more extensible for future features like dynamic command loading or introspection.

### `feat: Implement unified data models with Pydantic`

**Purpose:** To standardize all network-transported data structures, leveraging Pydantic for automatic validation and serialization/deserialization.

**Reasoning:** The previous approach relied on manual dictionary manipulation and `orjson` calls, which was error-prone and lacked type safety. By introducing Pydantic models, we gain strong typing, automatic data validation, and simplified serialization/deserialization. This significantly improves the reliability and maintainability of the system, reducing the likelihood of data-related bugs and making future development more efficient.

### `feat: Implement configuration as code with Pydantic-settings`

**Purpose:** To replace the existing YAML configuration with a more robust and type-safe solution.

**Reasoning:** The previous YAML-based configuration was prone to errors due to lack of type validation and limited programmatic access. By adopting `pydantic-settings`, we introduce strong typing for configuration parameters, enabling automatic validation and better integration with the Python codebase. This change enhances the reliability, maintainability, and extensibility of the configuration system, allowing for easier management of settings across different environments and reducing the risk of misconfigurations.

### `feat: Implement flexible serialization with JsonSerializer`

**Purpose:** To abstract the serialization logic, making it easier to support multiple serialization formats and improving the extensibility of the system.

**Reasoning:** Previously, `orjson.dumps` and `orjson.loads` were used directly throughout the codebase, tightly coupling the application to a specific serialization implementation. By introducing a `Serializer` interface and a `JsonSerializer` concrete class, we decouple the serialization concerns. This change lays the groundwork for future integration of other serialization methods (e.g., `cloudpickle` for more complex Python objects) without requiring significant changes to the core server logic. The `cloudpickle` implementation itself is a separate, future task.

### `feat: Implement robust connection handling with exponential backoff`

**Purpose:** To enhance the reliability and resilience of network communication by implementing automatic reconnection with exponential backoff.

**Reasoning:** The previous connection handling was basic and lacked mechanisms for automatically recovering from transient network failures. By integrating exponential backoff into the `Connection` class, we ensure that the system can gracefully handle temporary disconnections and automatically attempt to re-establish connections with increasing delays. This significantly improves the stability and fault-tolerance of the MPREG cluster, reducing manual intervention and enhancing overall system availability.

### `feat: Add keyword argument support to RPC commands`

**Purpose:** To enhance the flexibility and expressiveness of RPC calls by allowing the use of keyword arguments.

**Reasoning:** Previously, RPC commands only supported positional arguments, which could lead to less readable and more maintainable code, especially for functions with many parameters. By extending the `RPCCommand` model to include a `kwargs` field, we enable developers to use named arguments, improving code clarity and making RPC interfaces more self-documenting. This change aligns with modern Python practices and enhances the overall usability and extensibility of the MPREG system.

### `feat: Implement request timeouts`

**Purpose:** To prevent RPC client requests from blocking indefinitely, improving the robustness and predictability of the system.

**Reasoning:** Without timeouts, a client request could hang indefinitely if the server is unresponsive or a network issue occurs, leading to a poor user experience and potential resource exhaustion. By adding an optional `timeout` parameter to the client's `request` method and enforcing it with `asyncio.wait_for`, we ensure that requests either complete within a specified duration or fail gracefully. This makes the client more resilient to network and server issues, providing a more reliable and responsive system.

### `feat: Add keyword argument support to RPC commands`

**Purpose:** To enhance the flexibility and expressiveness of RPC calls by allowing the use of keyword arguments.

**Reasoning:** Previously, RPC commands only supported positional arguments, which could lead to less readable and maintainable code, especially for functions with many parameters. By extending the `RPCCommand` model to include a `kwargs` field, we enable developers to use named arguments, improving code clarity and making RPC interfaces more self-documenting. This change aligns with modern Python practices and enhances the overall usability and extensibility of the MPREG system.

### `feat: Enhance Server Discovery & Gossip Protocol`

**Purpose:** To build a robust, self-organizing, and secure gossip mechanism that handles diverse network topologies and prevents accidental cross-cluster communication.

**Reasoning:** The previous basic implementations of server discovery and gossip laid the groundwork but lacked crucial features for real-world deployments. This enhancement addresses key areas:
-   **Cluster Identification:** Ensures that only peers belonging to the same logical cluster can communicate, preventing accidental cross-talk and improving security.
-   **Configurable Advertised Addresses (Multiple):** Provides flexibility for nodes to advertise multiple network interfaces (e.g., internal, external, CDN) allowing other nodes to intelligently discover and connect via the most suitable path. This simplifies network configuration and improves connectivity in complex environments.
-   **Dynamic Peer Management and Propagation:** Enables the cluster to dynamically learn about and propagate information about all known healthy peers, leading to faster and more accurate cluster state convergence.
-   **Connection Management based on Gossip:** Allows the server to proactively establish and maintain connections to a dynamic set of peers, improving overall cluster resilience and responsiveness.

### `refactor: Clear server and client roles`

**Purpose:** To clearly separate the responsibilities of the MPREGServer into distinct server (listening) and client (connecting to peers) roles.

**Reasoning:** The `MPREGServer` previously conflated both server and client functionalities, leading to a less modular and harder-to-maintain codebase. By extracting the client-side logic into a dedicated `MPREGClient` class, we adhere to the Single Responsibility Principle. This separation enhances code clarity, simplifies testing of individual components, and provides a cleaner architecture for future development, especially for implementing more sophisticated peer-to-peer communication and gossip protocols.
