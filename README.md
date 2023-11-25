# mpreeg: Matt’s Protocol for Results Everywhere Guaranteed

Do you need results? Everywhere? Guaranteed? Then you need MPREG!


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
await self.request([Command("first", "echo", ["hi there!"], [])])
```

and it returns the function call value matched to your RPC request name for the function call:

```json
{"first": "hi there!"}
```

### More Advanced: Use previous function as next argument
RPC requests have your RPC reference name, your target function name, and the function arguments. The trick here is: if your function arguments match the name of other RPC reference names, the other RPC is resolved first, then the RPC's return value is used in place of the name.

We can also call multiple functions at once with unique names:

```python
await self.request(
    [
        Command("first", "echo", ["hi there!"], []),
        Command("second", "echo", ["first"], []),
    ]
)
```

and it returns the `first` RPC returned value as the parameter to the `second` name:

```
{"second": "hi there!"}
```

### More Clear: Name RPC Names Better

Direct string matching on the function parameters can be confusing as above with "first" suddenly becoming a magic value, so let's name them better:

```python
await self.request(
    [
        Command("|first", "echo", ["hi there!"], []),
        Command("|second", "echo", ["|first"], []),
        Command("|third", "echos", ["|first", "AND ME TOO"], []),
    ]
)
```

and this one returns:

```json
{"|second": "hi there!", "|third": ["hi there!", "AND ME TOO"]}
```

Note how it returns all FINAL level RPCs having no further resolvable arguments (so `mpreg` supports one-call-with-multiple-return-values just fine).

### More Examples:

#### 3-returns-1 using multiple replacements

```python
await self.request(
    [
        Command("|first", "echo", ["hi there!"], []),
        Command("|second", "echo", ["|first"], []),
        Command("|third", "echos", ["|first", "|second", "AND ME TOO"], []),
    ]
)
```

returns:

```json
{"|third": ["hi there!", "hi there!", "AND ME TOO"]}
```

#### 4-returns-1 using multiple replacements

```python
await self.request(
    [
        Command("|first", "echo", ["hi there!"], []),
        Command("|second", "echo", ["|first"], []),
        Command("|third", "echos", ["|first", "|second", "AND ME TOO"], []),
        Command("|4th", "echo", ["|third"], []),
    ]
)
```

returns:

```json
{"|4th": ["hi there!", "hi there!", "AND ME TOO"]}
```

### Extra Argument

You may have noticed the extra empty list `[]` argument in all those `Command()` calls. For these `echo` tests there's no dataset to consult, but if your cluster had named datasets registered, you'd provide your dataset name(s) as the final parameter there.

When called fully with `(rpc name, rpc function, rpc arguments, rpc data)`, the cluster routes your request to the best matching cluster nodes having `(function, dataset)` matches (because you may have common functions like "run model" but the output changes depending on _which model_ you are running as your dataset).

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
poetry run mpreg-server --config ./dev-1.config.yaml start
```
### Terminal 2 (Server 2)

```bash
poetry run mpreg-server --config ./dev-2.config.yaml start
```

### Terminal 3 (Client)

```
poetry run mpreg-client ws://127.0.0.1:7773 run
```

## Status

The above demos work, but the entire system isn't clealy abstracted for external use easily yet.

To register your own clients with custom datasets, the logic is currently manually configured around the `self.cluster.add_self_ability` calls for the local node and remote nodes are registered around the `fun1` `fun2` manual tests. Additional actually usable handlers would look the same, but it just needs a better API/client interface to make it work from external code properly.

## Scattered Details

### Server

- server component listens for inbound requests from clients and any other servers
- all servers are peer servers. any server may act as a router or load balancer for any other server.
- each server is configured on startup with a config file describing:
  - an input of RESOURCES (set[str]) to advertise to the mesh/hive/cluster
    - note: you can also have servers with no RESOURCES which can act as load balancers
    - any server with NO RESOURCES is labeled BALANCER while other with RESOURCES are SERVERS
  - an input of NAME for reporting to the Mesh/Hive/Cluster (MHC)
  - peers: a list of peer contact locations for joining yourself to the MHC


any server can connect to any other server. servers gossip their membership updates (TODO).


## TODO

- Currently format is only JSON for all parameters and return values. Eventually add [`cloudpickle`](https://github.com/cloudpipe/cloudpickle) support.
- Add more one-command automated tests so we don't need to manually set up the servers and clients.
- Add more easily reusable client library.
- Add more easily usable server interface for software to register their RPC names and datasets when acting as live nodes.
- We could clean up some of the internal logic to use more actual objects instead of free floating dicts everywhere
- and lots more!
