# Distributed File System with Stream Processing

## Description
This tool implements RainStorm, which is a Stream Processing System that allows data to be processed in stages and results outputted continuously. It relies on MP3 Hybrid Distributed File System to store input and custom tasks across nodes. We chose to implement it so that there is a “leader” node, where the operation is initially performed by the user, and the rest are “worker” nodes that are each part of one of three stages: Batching, Operation 1 Processing, or Operation 2 Processing.

This project integrates three key components—a Distributed Group Membership protocol, a Hybrid Distributed File System, and a Stream Processing System—to provide fault-tolerant, scalable, and efficient data handling. Made to run on 10 VMs the Distributed Group Membership underpins the entire architecture, using an Introducer node to onboard new members via an RPC connection while broadcasting “NEW” messages over UDP. Each member maintains its own MembershipList, listening for “PING,” “DEL,” “ALIVE,” and “SUS” signals to track active nodes and detect failures. When suspicion is enabled, a member that suspects another node of failure broadcasts a “SUS” message and increments an incarnation number, and if the suspected node remains unresponsive, a “DEL” is disseminated, effectively removing it from the system. This heartbeat-based mechanism ensures continuous awareness of membership status with minimal communication overhead.

Building on this foundation, the Hybrid Distributed File System arranges members in a virtual ring defined by the hash of their IP addresses. Files, also hashed by filename, are uploaded once (“create”) and automatically replicated on three successor nodes. This replication occurs via a pull-based approach in which each node regularly checks its predecessors for files it must host. To optimize performance, any appended data is deferred until a merge is triggered by a read (“get”) operation, ensuring minimal overhead for write operations. During a merge, the system collects appends from all live replicas, orders them by their original timestamps, and consolidates them into a single file. Consistency is guaranteed by replicating files on three nodes and eventually applying appends in the same order across replicas, while consistent hashing ensures a node that fails and rejoins reclaims leadership for its previously held files.

On top of this file system, the Stream Processing System distributes tasks across three stages, each responsible for a portion of the input data. A leader node orchestrates the pipeline by dispatching work partitions to Stage 1 nodes, which in turn batch their results to Stage 2 for further processing, and finally Stage 3 for completion. Each stage dynamically pulls the required operation binaries from the file system if not already available, and it relies on RPC calls with timeouts to guarantee fault tolerance. Exactly-once semantics are enforced by tracking batch IDs, preventing duplicated outputs in the event of retries. While the system is highly robust, ongoing refinements—such as storing file line counts to eliminate the need for full file reads—could further enhance efficiency. 

Overall, the project creates a fully functioning stream processing application using a distributed File System with stable group membership.


## Installation
Install go 1.21.0.

## Usage
First, SSH into each VM and start at the project root. \
On one of these VMs (modify `introducerConfig.json` appropriately), begin the `introducer` process: `go run main.go introducer`. \
Then, start the members by beginning the `member` process: `go run main.go member`.

Each VM can accept the following commands:
- `create localfilename HyDFSfilename` - create file on FileSystem
- `get HyDFSfilename localfilename` - get file to current member
- `append localfilename HyDFSfilename` - add to file on FileSystem
- `ls HyDFSfilename` - list VMs which given file is on
- `store` - list files on member
- `getfromreplica VMaddress HyDFSfilename localfilename` - get file from specfic member
- `list_mem_ids` - list member ids of all active nodes on file system

Now, in order to run a Stream Processing job:
- `h_op1`, `h_op2`, `h_input`, and `h_output` should be created on the HyDFS.
- `RainStorm <h_op1> <h_op2> <h_input> <h_output> <num_tasks>` - It will take the programs h_op1 and h_op2 and run those on the data.
