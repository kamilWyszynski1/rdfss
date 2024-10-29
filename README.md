# RDFSS

RDFSS is a rust Distributed File Storage System implementation.


# How to


Step-by-Step Guide to Building a Distributed File Storage System
###  Define Requirements and Architecture
Before diving into code, define the basic requirements and high-level architecture of your system.

**Requirements**:

* Basic Operations: Upload (store), download (retrieve), and delete files.
* Scalability: Ability to distribute files across multiple nodes.
* Fault Tolerance: Replicate data to handle node failures.
* Consistency: Decide between strong or eventual consistency.
* Data Integrity: Ensure that files are not corrupted during transfer/storage.

**Architecture**:

* Client: The interface for users to interact with the file system (upload, download).
* Storage Nodes: Nodes where file chunks are stored.
* Metadata Service: Keeps track of where files (or file chunks) are stored, their size, etc.
* Master Node (optional): A central node for coordinating storage nodes and handling replication policies.

**High-Level Flow**:

* Upload: Client uploads a file, which is split into chunks and distributed to storage nodes. Metadata service stores chunk locations.
* Download: Client requests a file, and the system retrieves the necessary chunks from the correct storage nodes.
* Replication: Ensure multiple copies of chunks are stored for fault tolerance.

**Design Core Components**:

Storage Nodes
Each storage node is responsible for storing file chunks and responding to requests to upload, download, or delete chunks.

Steps:

* File Chunking: When a file is uploaded, split it into smaller chunks (e.g., 64MB each).
* Chunk Storage: Store each chunk as a separate file on the local filesystem of the storage node. Use a unique ID for each chunk (e.g., SHA-256 hash of the content).
* Chunk Retrieval: Implement APIs for retrieving chunks based on the unique ID.

Metadata:

* Store mappings of fileID -> chunkIDs.
* Keep track of which storage node holds which chunk.

**Replication Strategy:**

* When uploading a chunk, store it on multiple storage nodes.
* For example, each chunk could be replicated across 3 different storage nodes (based on availability and fault tolerance requirements).

**Master Node**

If you need a centralized coordinator, the master node will:

* Manage the distribution of chunks across storage nodes.
* Handle node failures and replication policies.
* Keep the metadata service updated.

The master node could be a single point of failure, so eventually, you might consider adding replication or leader election (e.g., using Raft or Zookeeper) to avoid downtime.

**File Operations Implementation**

File Upload
* Step 1: The client sends the file to the master node or directly to a storage node.
* Step 2: The file is split into chunks and distributed across storage nodes.
* Step 3: The metadata service updates the chunk locations for the file.
Example (in Go):

File Download
* Step 1: The client requests the file by file ID.
* Step 2: The metadata service provides the list of chunk IDs and the storage nodes where the chunks are located.
* Step 3: The client retrieves each chunk from the respective storage nodes and reconstructs the file.

File Deletion
* Step 1: Delete the metadata entry for the file.
* Step 2: Delete the chunks from the respective storage nodes.

**Replication and Fault Tolerance**
  
   a. Replication
   * Replicate Chunks: When a chunk is uploaded, replicate it to multiple storage nodes to ensure availability even if some nodes fail.
   * Replication Strategy: Use a leader-follower model where one node holds the primary copy and others hold replicas.
   
   b. Handling Node Failures
   * If a storage node goes down, implement logic to re-replicate the chunks it was holding on another available node.
   * You can use heartbeats to detect when a node is unresponsive and trigger replication.

**Testing the System**
   * Single Node Testing: Start with one node to ensure chunking, storing, and retrieval are working.
   * Multiple Node Setup: Deploy multiple storage nodes on Docker or a cloud service (e.g., AWS EC2 instances) and test the distribution of file chunks.
   * Failure Simulation: Shut down a storage node to test how replication and fault tolerance mechanisms handle node failures.

**Optimizations and Extensions**
   * Erasure Coding: Implement erasure coding (instead of replication) to reduce storage overhead while maintaining fault tolerance.
   * Compression: Compress chunks before storing them to save space.
   * Access Control: Implement permissions and authentication for file operations (similar to S3’s policies).
   * Monitoring: Add monitoring tools (like Prometheus or Grafana) to track the health and performance of nodes.
   * Consistency Model: Implement eventual consistency or strong consistency, depending on use case requirements.