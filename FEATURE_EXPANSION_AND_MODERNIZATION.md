
# MPREG Feature Expansion and Modernization

This document outlines a roadmap for improving the MPREG project, focusing on feature consistency, modernization, and overall usability.

## I. Core Architecture & Modernization

This section focuses on improving the foundational aspects of the MPREG codebase.

### A. Refactor to Modern Python Practices

-   [ ] **Typed Data Structures:**
    -   [ ] Replace `dict` with `dataclasses` or `pydantic` models for all network communication schemas (RPCs, server messages, etc.). This will improve code clarity, reduce runtime errors, and enable static analysis.
    -   [ ] Investigate using `pydantic` for automatic validation of incoming data.
-   [x] **Configuration:**
    -   [x] Replace YAML configuration with a more robust solution like `pydantic-settings` for type-safe configuration management.

### B. Improve Cluster Management & Communication

-   [x] **Server Discovery & Gossip Protocol:**
    -   [x] Introduce Cluster Identification: Add a mandatory `cluster_id` to `MPREGSettings` and `PeerInfo`. Ensure servers only accept connections and process gossip messages from peers sharing the same `cluster_id`.
    -   [x] Configurable Advertised Addresses (Multiple): Modify `MPREGSettings` to include an optional `advertised_urls` (list of strings). Update `PeerInfo`, `RPCServerHello`, and `GossipMessage` to include `advertised_urls`. Implement logic for connecting peers to try all advertised URLs until a successful connection.
    -   [ ] Dynamic Peer Management and Propagation: Enhance `Cluster.process_gossip_message` to intelligently merge `PeerInfo`. Implement `MPREGClient` to periodically send `GossipMessage` containing all known healthy peers. Add dead peer detection.
    -   [ ] Connection Management based on Gossip: Implement logic in `MPREGServer` to proactively establish connections to newly discovered peers, respecting `cluster_id` and trying all advertised URLs. Ensure `MPREGServer` can manage multiple outbound `MPREGClient` connections.
    -   [x] **Introduce Cluster Identification:** Add a mandatory `cluster_id` to `MPREGSettings` and `PeerInfo`. Ensure servers only accept connections and process gossip messages from peers sharing the same `cluster_id`.
    -   [x] **Configurable Advertised Addresses (Multiple):** Modify `MPREGSettings` to include an optional `advertised_urls` (list of strings). Update `PeerInfo`, `RPCServerHello`, and `GossipMessage` to include `advertised_urls`. Implement logic for connecting peers to try all advertised URLs until a successful connection.
    -   [x] **Dynamic Peer Management and Propagation:** Enhance `Cluster.process_gossip_message` to intelligently merge `PeerInfo`. Implement `MPREGClient` to periodically send `GossipMessage` containing all known healthy peers. Add dead peer detection.
    -   [x] **Connection Management based on Gossip:** Implement logic in `MPREGServer` to proactively establish and maintain connections to a dynamic set of peers, improving overall cluster resilience and responsiveness.
-   [x] **Connection Handling:**
    -   [x] Refactor the `Server` class to manage websocket connections more robustly, including automatic reconnection logic with exponential backoff.
    -   [ ] Implement a more graceful shutdown procedure for servers, ensuring all client requests are handled before exiting.
-   [ ] **Serialization:**
    -   [ ] Add support for `cloudpickle` as an alternative to JSON for serializing more complex Python objects. This will require a negotiation step during the initial handshake to determine the serialization format.

## II. Feature Expansion & Usability

This section focuses on adding new features and improving the overall user experience.

### A. Enhance RPC Capabilities

-   [x] **Keyword Argument (kwargs) Support:**
    -   [x] Extend the `Command` and `RPCFun` data structures to support keyword arguments in addition to positional arguments.
-   [ ] **Error Handling & Reporting:**
    -   [ ] Implement a more structured error handling mechanism. Instead of returning tracebacks, define a set of error codes and messages that can be easily parsed by clients.
-   [x] **Request Timeouts:**
    -   [x] Add a timeout parameter to client requests to prevent indefinite blocking.

### B. Improve Client & Server APIs

-   [x] **Reusable Client Library:**
    -   [x] Create a more user-friendly and reusable client library that abstracts away the low-level details of websocket communication and request creation.
-   [x] **Simplified Server Interface:**
    -   [x] Develop a simpler API for registering RPC functions and datasets on a server. This will make it easier to integrate MPREG with existing applications.
-   [x] **Command-Line Interface (CLI):**
    -   [x] Enhance the existing `jsonargparse`-based CLI with more commands and options for managing the cluster and inspecting its state.

### C. Testing & Documentation

-   [ ] **Automated Testing:**
    -   [ ] Implement a comprehensive test suite using a framework like `pytest`.
    -   [ ] Add unit tests for all core components (client, server, cluster, etc.).
    -   [ ] Create integration tests that simulate a multi-server cluster and verify end-to-end functionality.
-   [ ] **Documentation:**
    -   [ ] Expand the `README.md` with more detailed examples and a clearer explanation of the project's architecture.

## III. Usability & User Experience

This section focuses on improving the overall developer experience when working with MPREG.

### A. Observability & Monitoring

-   [ ] **Logging:**
    -   [ ] Implement structured logging inside loguru to make it easier to parse and analyze log data.
-   [ ] **Metrics & Monitoring:**
    -   [ ] Expose key metrics (e.g., number of connected clients, request latency, error rates) via a Prometheus endpoint.
    -   [ ] Create a Grafana dashboard for visualizing the cluster's health and performance.

### B. Security

-   [ ] **Authentication & Authorization:**
    -   [ ] Add an optional authentication layer to restrict access to the cluster.
    -   [ ] Implement a basic authorization mechanism to control which clients can execute which RPC functions.

## IV. Detailed Architectural Refactoring

This section provides a more detailed breakdown of the core architectural improvements required to enhance usability, encapsulation, and abstraction.

### A. Command Management & Execution Flow

The goal is to create a clear, well-documented, and intuitive system for defining, registering, and executing commands.

-   [x] **Standardize Command Representation:**
    -   [x] Design a single, unified `Command` dataclass/Pydantic model that is used across the entire system (client, server, and network). This class should encapsulate all properties of a command, including its name, the function to be executed, and any resource requirements (`locs`).
    -   [x] Refactor `mpreg/client.py` and `mpreg/server.py` to use this single `Command` definition, eliminating the current separate and inconsistent representations.
-   [x] **Decouple Command Definition from Execution:**
    -   [x] Create a `CommandRegistry` class responsible for storing and looking up available commands on a server. This will replace the current `cluster.self` dictionary.
    -   [x] The `MPREGServer` will own an instance of the `CommandRegistry`. The `add_self_ability` method will be refactored to `registry.register(command)`.
-   [x] **Clarify RPC Execution Logic:**
    -   [x] Refactor the `Cluster.run` method to be more readable and maintainable. The logic for finding a suitable server and executing a command has been extracted into separate, well-named methods (`_execute_local_command`, `_execute_remote_command`).
    -   [x] The `RPCFun` dataclass has been renamed to `RPCStep` to better reflect its role as a step in the RPC chain, and its fields are now strongly typed.

### B. Data Management & Serialization

The goal is to create a robust and flexible system for handling data, including RPC arguments, results, and server configurations.

-   [x] **Unified Data Models:**
    -   [x] Define all network-transported data structures (e.g., `Request`, `Response`, `ServerHello`) as Pydantic models. This will provide automatic validation and serialization/deserialization, reducing boilerplate code and runtime errors.
    -   [x] Replace all manual `orjson.dumps` and `orjson.loads` calls with Pydantic's built-in methods (`.model_dump_json()` and `model_validate_json()`).
-   [ ] **Configuration as Code:**
    -   [ ] Implement `pydantic-settings` to manage server configuration. This will allow for type-safe configuration loaded from environment variables, `.env` files, or other sources, replacing the current `dev-*.config.yaml` files.
-   [ ] **Flexible Serialization:**
    -   [ ] Abstract the serialization logic into a `Serializer` class. This will make it easier to add support for other formats like `cloudpickle` in the future. The server can negotiate the serializer to use with the client during the initial handshake.

### C. Network Management & Cluster Communication

The goal is to improve the reliability and resilience of the cluster by abstracting network operations and improving connection handling.

-   [x] **Abstract Network Connections:**
    -   [x] Create a `Connection` or `Peer` class that encapsulates a websocket connection. This class will handle the low-level details of sending and receiving messages, as well as managing the connection state (connected, disconnected, reconnecting).
    -   [x] The `Cluster` class will manage a collection of these `Connection` objects instead of raw websocket objects.
-   [ ] **Robust Connection Handling:**
    -   [ ] Implement automatic reconnection logic with exponential backoff within the `Connection` class. This will make the cluster more resilient to transient network failures.
    -   [ ] Implement a proper gossip protocol for server discovery. The `Connection` class can be extended to handle the exchange of peer information.
-   [x] **Clear Server and Client Roles:**
    -   [x] Refactor the `MPREGServer` to clearly separate its two roles: the "server" role (listening for incoming connections) and the "client" role (connecting to other peers). This might involve creating separate classes or modules to handle each role's logic.
