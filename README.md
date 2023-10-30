# Fault Tolerance Cluster Project
This is a showcase of my Introduction to Distributed Systems course project. Over the course of the semester I built a failt-fast, fault-tolerant cluster to balance an arbitrary workload. Work is submitted, and results were received from, an HTTP accesible API.

Full technical requirments can be found in this PDF

# Project Overview
The cluster provides an service to compile and run Java files. A user can use a provided client to submit java files to a REST API, where the gateway receiving the work forwards the file to another node in the cluster for compiling and running. Results are forwarded back to the gateway, and then back to the client. Within the cluster, nodes have to arrange themselves to distribute work evenly, and respond accordingly if one should fail.

# Project Requirments
+ Nodes have to use Gossip style heartbeat to inform each other of liveness. Should a node be discovered to have failed, nodes should inform other nodes that that has taken place. All information related to heartbeat should be logged. 

+ With the exception of a Gateway node, nodes must use Zookeeper to elect a leader of the cluster who is in charge of distributing work to the worker nodes. Each node should have a unique ID, and vote preference is by the nateral ordering of the IDs (Highest ID wins)

+ If a worker node fails, the request it was working on must not be lost.

+ If the leader of the cluster fails, the work being done by the worker nodes must not be lost while a new leader election takes place

# Running and Testing the Application
## Requirements 
+ Java 11
+ Maven

## System Requirements
+ The provided script (see below) runs the application with multiple nodes running concurrently on the same machine. Each node starts up mulitple threads and does a lot of inter and intra node talking. An underpowered machine will have a very hard time running this script. Haveing a processor with 16 threads should allow the script to run smoothly.

This script showcases the requirements above. The script takes the following steps:
1. Compile the program
2. Initialize the Cluster with 7 nodes and a Gateway node
3. Wait for Leader to be elected. Capture locations of Log files relating to Gossip Heartbeat for each Node. Print each node's status in the cluster
4. Send 9 requests serially, print the responses
5. Kill node with the lowest ID (Guareanteed to NOT be the Leader), watch as each node discovers the dead node. Print the cluster status again
6. Kill the Leader node. Send 9 more requests in parallel. Watch as System handles the reelection and manages the backlog of work that was waiting for the election to take place
7. Receive the responses from the System and print them with the origional request. Showcases the system not losing work despite the election
8. Send one more request, and print the response
9. Display paths to the heartbeat logs for each server
10. Shutdown the cluster
