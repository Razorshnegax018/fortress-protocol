# Fortress Protocol

## A pBFT (Practical Byzantine Fault Tolerant) implementation

### What is pBFT? An architectural breakdown

In distributed systems theory (multiple computers working together as one), there are two types of consensus algorithms (logic ran on those computers to make sure they have the same data) designed to be resistant to two types of errors and adverse events: Crash fault tolerant and Byzantine Fault tolerant

- Crash fault tolerant algorithms, such as Raft or Paxos, designed to be resistant to intra-network *crashes* - the failure of indiviudal nodes (hence the name). For this project, the important thing to notes is that *all nodes are trustworthy and obedient to the leader*

- Byzantine fault tolerant algorithms, such as pBFT and the Proof-of family (proof of stake/work/history) are designed to be resistant to intra-network *sabatoges* - nodes within the network lying or misleading each other for personal gain

(TODO)

---

### Fortress Protocol - my implementation breakdown

Architectural breakdown

- Fortress protocol has three distinct “node” (machine/computers in the network) types with three distinct responsibilities  
    
  - The leader node \- the node that sends the *pre-prepare* message to all connected peers, starting off consensus. While they start consensus with sending the *Pre-Prepare* message, they are treated by all other peers as another peer node  
      
  - The peer node(s) (also called “validator nodes” in some other blockchain networks), which are nodes that contain a copy of the protocol’s state  
      
  - The bootnode, which is a router node that has the list of the leader and all connected peers.  
      
- For pBFT, a leader or peer node has two “states” or code sections: The peer/client connection section, where the node accepts TCP connection requests from peers (leader node) or clients (peer node).  
    
- Then there is the consensus section, where the node communicates with all other nodes to achieve “consensus”, meaning each node has the same state.  
    
- Consensus is the operation of communication between all nodes in the network to achieve agreement on the chain/protocol’s state. For pBFT, a mathematical proof worked out that a protocol should have N \= 3f \+ 1 nodes, and require a quorum of 2f \+ 1 for voting of each stage, where f \= \# of faulty nodes. This means that we calculate quorums using 2 ((N \- 1\) / 3\) \+ 1 votes, where f \= (N \- 1\) / 3\.  
    
- The different natures of these two sections means that they take two different architectural approaches to deal with them 

  - *Leader node:* For the leader node, I use a standard single multithreaded tokio runtime. The work of accepting peer connections and handling communications between networks is spread out between the cores like a standard runtime

  - The consensus section is a high frequency “hot loop” that I place on its own thread that I place a “high priority” on to hint MacOS to run it on a Performance Cluster.  
    - With Mac Silicon, you’re unable to formally “pin” threads to cores like we can with linux, so I use a tokio LocalSet (run all tokio tasks on this thread, and critically, don’t let the other threads touch you/take your cache lines) alongside QoS classes (explained below) to achieve the same effect \- so data and instructions can stay tight inside the cache

--- 

  - Diving deeper into the mechanics of each section:  
    - As described before, the leader node starts off as a multithreaded runtime. The runtime starts and a transaction manager, a task that owns the state of the protocol, is initialized.   
        
    - On runtime startup, the *consensus task* is started. A new OS thread is spawned and its QoS (quality of service, userspace hints for “how much performance does this thread require”) class is placed to “Highest”. This gives the kernel a strong preference to place that thread into a “performance cluster” \- a group of high clock speed cores that share an L2 cache, the closest MacOS gets to thread pinning.  
        
    - The leader node then moves into the “peer acceptance loop” where it waits for incoming TCP connection requests from peers.  
        
    - Whenever it gets an acceptance from any given TCP request, it first starts a timeout where a peer is given 2 seconds to respond before dropping the connection.  When a peer responds, the leader attempts to deserialize their response to extract their pubkey and passes it to a *reader task* \- a long running task that reads all further network requests from a verified validator peer and routes them to their proper area  
        
    - At the very end of the leader node’s accept loop, they send a "registration request” to the consensus engine to register a new peer into the list of known peers. The registry is owned fully by the consensus core/cluster and registration requests routed there to avoid heavy repeated locks  
        
      - There are generally two requests a peer can make to a leader and to each other (technically): A peer vote, which is when a peer casts a vote in consensus. Cryptographic verification takes place in the reader task and is distributed across cores as well.


      - The other request a reader task accepts is a consensus request. This can only be sent to a peer by a leader over the network, as validator peers do not have the authority to start consensus.  
          
    - *Peer node:* The peer node runs the same fundamental architecture, but with differences in setup functionality. Peers have  the bootnode address hardcoded into static memory, which they connect to so they may request a list of all connected peers and start reader tasks for each of them  
        
    - Special care is taken to distinguish leader writer half from other peers, but the leader reader half is treated as a standard peer and given a standard reader task

    - Once the peer has active connections to all other peers, it goes through  the same process described in the leader acceptance loop  
        
  - *Bootnode: TODO*

Specific low level optimizations

* *Thread “pinning” for cache coherency*: Thread pinning is whenever you tell the OS that a thread can only only run on a specific core, keeping its data close to cache and avoiding costly cross core chatter as important data jumps between cores 

  * However, as stated above, MacOS does not have a standard method of pinning threads to specific cores. So the closest thing is my strategy \- I start a new thread, set that thread’s QoS class to the highest, and then start a new local set on that thread to disable work stealing. That’s the most performant I can make my consensus engine

* *Zero-copy network deserialization*: when a new packet is received from the network, it comes in the form of raw, tightly packed, unreadable bytes. I use a library called bincode to read those bytes and turn them into usable data, a process called “deserialization”

  * Whenever we do that using bincode’s standard settings, we allocate a new buffer on each deserialization. This behavior is poor for performance for a myriad of reasons, long explanation short, it’s inefficient for cache and memory paging (cache is filled with pointers instead of data, allocs possibly scattered across pages, page faults over and over)

  * We combat this with a process called *Zero copy deserialization.* Using an advanced rust feature known as lifetimes, we simply create references to the data from the network and tie its *lifetime* to the network buffer. This allows for network communication at high, cache efficient speeds


* *Reusable buffers:* a short explanation for why constant allocations are poor for performance is written above, which leads to the optimization of reusable buffers. They are used in this project for various reasons, but the most common use case is creating a “serialization pool”. Similar to deserialization, serialization of usable data into tightly packed bytes for the network is an operation that allocates bytes into a separate buffer under standard bincode rules. So we use a function called “serialize\_into” that takes a reference to a large bytes buffer (serialization pool) and then writes the bytes into that single, already used/hot memory in cache.
