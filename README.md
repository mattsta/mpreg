# mpreg: Matt’s Protocol for Results Everywhere Guaranteed

Do you need results? Everywhere? Guaranteed? Then you need MPREG!

Status: This has evolved from a demo proof of concept into a **production-ready distributed RPC system** with comprehensive test coverage and robust architectural patterns. MPREG implements a network-enabled multi-function-call dependency resolver with custom function topologies on every request, now featuring concurrent request handling, self-managing components, and automatic cluster discovery. I still haven't found anything else equivalent to the "gossip cluster group hierarchy low latency late-binding dependency resolution" approach explored here. A similar system is the nice https://github.com/pipefunc/pipefunc but it is designed around running local things or running in "big cluster mode" so it doesn't meet these distributed coordination ideas.

## What is it?

`mpreg` allows you to define a distributed cluster multi-call function topology across multiple processes or servers so you can run your requests against one cluster endpoint and automatically receive results from your data anywhere in the cluster.

Basically, `mpreg` helps you decouple "run function X against data Y" without needing to know where "function X" and "data Y" exists in your cluster.

Why is this useful? I made this because I had some models with datasets I wanted to run across multiple servers/processes (they didn't work well multithreaded or forked due to GIL and COW issues), but then I had a problem where I needed 8 processes each with their own port numbers and datasets, but I didn't want to make a static mapping of "host, port, dataset, available functions" — so now, each process can register itself with (available functions, available datasets) and your clients just connect to the cluster and say "run function X on dataset Y" then the cluster auto-routes your requests to the processes having both the required data and functions available.

## Extra Features

Of course, just a simple mapping of "lookup dataset, lookup function, run where both match" isn't entirely interesting.

To spice things up a bit, `mpreg` implemenets a fully resolvable function call hirerachy as your RPC mechanism. basically: your RPC function calls can reference the output of other function calls and they all get resolved in the cluster-side before returned to your client.

## Examples

### Simple Example: Call Things

```python
# Modern API
async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
    result = await client.call("echo", "hi there!")
    # Returns: "hi there!"

# Or using the lower-level client directly
from mpreg.model import RPCCommand
result = await client._client.request([
    RPCCommand(name="first", fun="echo", args=("hi there!",), locs=frozenset())
])
```

and it returns the function call value matched to your RPC request name for the function call:

```json
{"first": "hi there!"}
```

### More Advanced: Use previous function as next argument
RPC requests have your RPC reference name, your target function name, and the function arguments. The trick here is: if your function arguments match the name of other RPC reference names, the other RPC is resolved first, then the RPC's return value is used in place of the name.

We can also call multiple functions at once with unique names:

```python
# Modern dependency resolution - these execute in proper order automatically
result = await client._client.request([
    RPCCommand(name="first", fun="echo", args=("hi there!",), locs=frozenset()),
    RPCCommand(name="second", fun="echo", args=("first",), locs=frozenset()),  # Uses result from "first"
])
```

and it returns the `first` RPC returned value as the parameter to the `second` name:

```python
{"second": "hi there!"}
```

### More Clear: Name RPC Names Better

Direct string matching on the function parameters can be confusing as above with "first" suddenly becoming a magic value, so let's name them better:

```python
result = await client._client.request([
    RPCCommand(name="|first", fun="echo", args=("hi there!",), locs=frozenset()),
    RPCCommand(name="|second", fun="echo", args=("|first",), locs=frozenset()),
    RPCCommand(name="|third", fun="echos", args=("|first", "AND ME TOO"), locs=frozenset()),
])
```

and this one returns:

```json
{"|second": "hi there!", "|third": ["hi there!", "AND ME TOO"]}
```

Note how it returns all FINAL level RPCs having no further resolvable arguments (so `mpreg` supports one-call-with-multiple-return-values just fine).

### More Examples:

#### 3-returns-1 using multiple replacements

```python
result = await client._client.request([
    RPCCommand(name="|first", fun="echo", args=("hi there!",), locs=frozenset()),
    RPCCommand(name="|second", fun="echo", args=("|first",), locs=frozenset()),
    RPCCommand(name="|third", fun="echos", args=("|first", "|second", "AND ME TOO"), locs=frozenset()),
])
```

returns:

```json
{"|third": ["hi there!", "hi there!", "AND ME TOO"]}
```

Note how here it returns only `|third` because `third` contains _both_ `|first` _and_ `|second` (so all return values have been resolved in the final result).

#### 4-returns-1 using multiple replacements

```python
result = await client._client.request([
    RPCCommand(name="|first", fun="echo", args=("hi there!",), locs=frozenset()),
    RPCCommand(name="|second", fun="echo", args=("|first",), locs=frozenset()),
    RPCCommand(name="|third", fun="echos", args=("|first", "|second", "AND ME TOO"), locs=frozenset()),
    RPCCommand(name="|4th", fun="echo", args=("|third",), locs=frozenset()),
])
```

returns:

```json
{"|4th": ["hi there!", "hi there!", "AND ME TOO"]}
```

### Extra Argument

You may have noticed the `locs=frozenset()` parameter in all those `RPCCommand()` calls. For these `echo` tests there's no specific dataset to consult, but if your cluster had named datasets/resources registered, you'd provide your resource name(s) there:

```python
# Route to specific resources/datasets
result = await client.call("train_model", training_data, locs=frozenset(["gpu-cluster", "dataset-v2"]))
```

When called fully with `(name, function, args, locs)`, the cluster routes your request to the best matching cluster nodes having `(function, resource)` matches (because you may have common functions like "run model" but the output changes depending on _which model/dataset_ you are running against).

`mpreg` cluster nodes can register multiple datasets and your RPC requests can also provide multiple dataset requests per call. Your RPC request will only be sent to a cluster node matching _all_ your datasets requested (but the server can have _more_ datasets than your request, so it doesn't need to be a 100% server-dataset-match).

This quad tuple of `(name, function, args, dataset)` actually simplifies your workflows because now you don't need to make 20 different function names for running datasets — you just have common functions but custom data defined on each node, then the cluster knows how to route your requests based both on requests datasets and requested function name availability (if multiple cluster nodes have the same functions and datasets registered, matches are randomly load balanced when requested).

## Quick Demo Running

### Intial Setup

```bash
pip install poetry -U

[clone repo and use clone]

poetry install
```

### Terminal 1 (Server 1)

```bash
# Run a server with specific resources
poetry run python -c "
from mpreg.server import MPREGServer
from mpreg.config import MPREGSettings
import asyncio

server = MPREGServer(MPREGSettings(
    port=9001, 
    name='Primary Server',
    resources={'model-a', 'dataset-1'}
))
asyncio.run(server.server())
"
```

### Terminal 2 (Server 2)

```bash  
# Run a second server that connects to the first
poetry run python -c "
from mpreg.server import MPREGServer
from mpreg.config import MPREGSettings
import asyncio

server = MPREGServer(MPREGSettings(
    port=9002,
    name='Secondary Server', 
    resources={'model-b', 'dataset-2'},
    peers=['ws://127.0.0.1:9001']
))
asyncio.run(server.server())
"
```

### Terminal 3 (Client)

```python
# Connect and make calls
from mpreg.client_api import MPREGClientAPI
import asyncio

async def main():
    async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
        # Simple call
        result = await client.call("echo", "Hello MPREG!")
        print(f"Result: {result}")
        
        # Multi-step workflow
        from mpreg.model import RPCCommand
        workflow = await client._client.request([
            RPCCommand(name="step1", fun="echo", args=("first step",), locs=frozenset()),
            RPCCommand(name="step2", fun="echo", args=("step1",), locs=frozenset()),
        ])
        print(f"Workflow result: {workflow}")

asyncio.run(main())
```

## Status

The above demos all work! The system has evolved significantly since the early prototype days and now includes:

✅ **Production-Ready**: Comprehensive test coverage (41 tests) and robust error handling  
✅ **Modern Client API**: Easy-to-use `MPREGClientAPI` with context manager support  
✅ **Concurrent Requests**: Multiple simultaneous requests over single connections  
✅ **Self-Managing Components**: Automatic connection pooling, peer discovery, and cleanup  
✅ **Distributed Coordination**: Gossip protocol for cluster formation and function discovery  
✅ **Resource-Based Routing**: Intelligent function routing based on available datasets/resources

To register your own functions with custom datasets/resources, you can now easily do:

```python
# Register custom functions on your server
server.register_command("my_function", my_callable, ["my-dataset", "gpu-required"])

# Client automatically discovers and routes to the right server
result = await client.call("my_function", args, locs=frozenset(["my-dataset"]))
```

## Scattered Details

### Server

- server component listens for inbound requests from clients and any other servers
- all servers are peer servers. any server may act as a router or load balancer for any other server.
- each server is configured on startup with a config file describing:
  - an input of RESOURCES (set[str]) to advertise to the mesh/hive/cluster
    - note: you can also have servers with no RESOURCES which can act as load balancers
    - any server with NO RESOURCES is labeled BALANCER while others with RESOURCES are SERVERS
  - an input of NAME for reporting to the Mesh/Hive/Cluster (MHC)
  - peers: a list of peer contact locations for joining yourself to the MHC


any server can connect to any other server. servers gossip their membership updates automatically with the implemented gossip protocol for distributed discovery.


## What's New (Recent Improvements)

🎉 **Major Architecture Modernization (v2.0)**:
- **Concurrent Client Support**: Multiple requests over single WebSocket connections using Future-based message dispatching
- **Self-Managing Components**: All components now handle their own lifecycle, connections, and cleanup automatically  
- **Production-Ready Testing**: 41 comprehensive tests covering distributed scenarios, concurrency, error handling, and edge cases
- **Robust Connection Management**: Persistent connection pooling, automatic reconnection, and proper cleanup
- **Enhanced Error Handling**: Comprehensive timeout handling, graceful degradation, and proper error isolation

## TODO (Future Enhancements)

- Currently format is only JSON for all parameters and return values. Eventually add [`cloudpickle`](https://github.com/cloudpipe/cloudpickle) support.
- ✅ ~~Add more one-command automated tests so we don't need to manually set up the servers and clients.~~ **DONE!**
- ✅ ~~Add more easily reusable client library.~~ **DONE!** (MPREGClientAPI with context manager support)
- ✅ ~~Add more easily usable server interface for software to register their RPC names and datasets when acting as live nodes.~~ **DONE!** (server.register_command())
- ✅ ~~We could clean up some of the internal logic to use more actual objects instead of free floating dicts everywhere~~ **DONE!** (Comprehensive dataclass usage)
- **Security**: Authentication and authorization mechanisms
- **Monitoring**: Built-in metrics and observability features
- **Auto-scaling**: Dynamic cluster scaling based on load
- and lots more!
