# Fault Tolerance Cluster Project
This is a showcase of my Introduction to Distributed Systems course project. Over the course of the semester I built a fail-fast, fault-tolerant cluster to balance an arbitrary workload. Work is submitted, and results are received from an HTTP accesible API.

Full technical requirments can be found in [this](https://github.com/ShalomGottesman/FaultTolerantCluster_Public/blob/main/COM-3800-Fall-2021-Semester-Project.pdf) PDF

# Project Overview
The cluster provides a service to compile and run Java files. A user can submit java files to a REST API, where the gateway receives the file and forwards it to another node in the cluster for compiling and running. Results are sent back to the gateway, and then back to the client. Within the cluster, nodes have to arrange themselves to distribute work evenly, and respond accordingly if one should fail.

# Project Requirments
+ Nodes have to use [Gossip](https://en.wikipedia.org/wiki/Gossip_protocol) style [heartbeat](https://en.wikipedia.org/wiki/Heartbeat_(computing)) to inform each other of liveness. Should a node be discovered to have failed, nodes should inform other nodes that that has taken place. All information related to heartbeat should be logged. 

+ With the exception of a Gateway node, nodes must use the [Zookeeper leader election](https://en.wikipedia.org/wiki/Leader_election) algorithm to elect a leader of the cluster who is in charge of distributing work to the worker nodes. Each node should have a unique ID, and vote preference is by the natural ordering of the IDs (Highest ID wins)

+ If a worker node fails, the request it was working on must not be lost.

+ If the leader of the cluster fails, the work being done by the worker nodes must not be lost while a new leader election takes place

# Running and Testing the Application
## Requirements 
+ Java 11
+ Maven

## System Requirements
+ The provided script (see below) runs the application with 8 nodes running concurrently on the same machine. Each node starts up mulitple threads and does a lot of inter and intra node talking. An underpowered machine will have a very hard time running this script. Having a processor with 16 threads should allow the script to run smoothly.

## Running the Application
[This bash script](https://github.com/ShalomGottesman/FaultTolerantCluster_Public/blob/main/script) showcases the requirements above. (After cloning the repository in a Unix environment you will need to make it runnable) 
The script has ten steps, explained below. Each step will print what step the script is up to.

The script takes the following steps:
1. Compile the program.
2. Initialize the Cluster with 7 nodes and a Gateway node.
3. Wait for Leader to be elected. Capture locations of Log files relating to Gossip Heartbeat for each Node. Print each node's status in the cluster.
4. Send 9 requests serially, print the responses.
5. Kill node with the lowest ID (Guaranteed to NOT be the Leader), watch as each node discovers the dead node. Print the cluster status again.
6. Kill the Leader node. Send 9 more requests in parallel. Watch as System handles the re-election and manages the backlog of work that was waiting while the election was taking place.
7. Receive the responses from the System and print them with the origional request. Showcases the system not losing work despite the election.
8. Send one more request, and print the response.
9. Display paths to the heartbeat logs for each server.
10. Shutdown the cluster.
