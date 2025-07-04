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

### `feat: Implement basic server discovery`

**Purpose:** To enable servers to announce their capabilities upon connection, laying the groundwork for dynamic service discovery within the cluster.

**Reasoning:** The initial architecture relied on static peer lists, which limited scalability and resilience. By introducing a basic server discovery mechanism, where servers send a HELLO message with their functions and resources upon connecting to a peer, we move towards a more decentralized and self-organizing cluster. This is a foundational step for implementing a full gossip protocol, allowing the cluster to dynamically discover and manage available services.

### `refactor: Clear server and client roles`

**Purpose:** To clearly separate the responsibilities of the MPREGServer into distinct server (listening) and client (connecting to peers) roles.

**Reasoning:** The `MPREGServer` previously conflated both server and client functionalities, leading to a less modular and harder-to-maintain codebase. By extracting the client-side logic into a dedicated `MPREGClient` class, we adhere to the Single Responsibility Principle. This separation enhances code clarity, simplifies testing of individual components, and provides a cleaner architecture for future development, especially for implementing more sophisticated peer-to-peer communication and gossip protocols.
