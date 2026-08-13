# Fortress Protocol

## A pBFT (Practical Byzantine Fault Tolerant) implementation

### What is pBFT? An architectural breakdown

In distributed systems theory (multiple computers working together as one), there are two types of consensus algorithms (logic ran on those computers to make sure they have the same data) designed to be resistant to two types of errors and adverse events: Crash fault tolerant and Byzantine Fault tolerant

- Crash fault tolerant algorithms, such as Raft or Paxos, designed to be resistant to intra-network *crashes* - the failure of indiviudal nodes (hence the name). For this project, the important thing to notes is that *all nodes are trustworthy and obedient to the leader*

- Byzantine fault tolerant algorithms, such as pBFT and the Proof-of family (proof of stake/work/history) are designed to be resistant to intra-network *sabatoges* - nodes within the network lying or misleading each other for personal gain

In the blockchain protocol space, 

---

### Fortress Protocol - my implementation breakdown

(TODO)

--- 

### Journey documentation

This is unfortunately retroactive documentation that I started after the project

- June 9:
	- Right now I'm refactoring the p2p networking layer so that instead of looping through the bootnode static list, the moment it connects to a peer it asks . 

	- in bootnode theory it seems like the network doesn't work unless you trust at least a bootstrap node. So the peer is going to trust that the addresses sent by the bootnodes are valid

	- The issue I'm dealing with now: How does a peer know what the address of the leader is to send their transaction request to - and how can they trust that address?

		- Going to deal with on June 10th

	- I realized that there was some logic that I really needed to clean up in the leader logic before I stared working on the peer logic, so I'm choosing to finish that

	- 11:36 PM - I've finished what I think to be my first draft of the leader logic. 
	
		- I finsihed the code for the commit voting - which was re-using the same mpsc queue from the prepare voting, making sure to try_recv out any stuck prepare votes. 

		- I created a timer (tokio select) that waits for a prepare and commit quorum for 200ms before cancelling the vote and failing consensus 
		
			- (either CONSENSUS-FAILED broadcast needs to be sent, or at the strategic position in peer code the socket.send error needs to be handled as consensus failure. Leaning towards second option)

		- I put in an router-worker type architecture where on commit, the worker sends the transaction over a channel to a manager task that owns the transation log and protocol state

		- Tomorrow we're going to start the difficulty of balancing trust with necessity - how to add a new node to the network.

- June (basically 11th cause it makes me seem consistent but actually) 12th:

	- 1:26 AM: So it's not like I lie it's just that I'm a little optimistic with my projections. I haven't been working for the past 2 (yes 2, the day doesn't end until I go to sleep, so it's just the 25th hour of June 11th) because I've been doing an RTL project with hardcaml - maybe more on that later.

	- Peer node logic:

		- So the bootnode strategy - they give new clients:
			1. A list of trusted peers to connect to who are online right now, and 

			2. A flood of data (transaction histories, hashes, and view numbers) to verifiy the transaction history themselves ot get "caught up" with their own work. 

		- So that means it will take time for the new nodes to verify and join up. 
		
		- For the sequence counter it simply gets incremented on each transaction, and for the leader view number, the number correlates to the first number of the leader's port (so on my local environment between 3-8) and is incremented to switch the leader on every 5 transactions

		- I haven't decided what to do whenever a new node finds specific things wrong with the network

	- Leader node logic:
		- Some optimizations to be done. What brought this on at the very end, the heavy and repeated atomic loading and storing of the sequence and the leader numbers. Could introduce terrible true contention. I thought to myself, "I want my transactions to be strictly sequential, we're not doing parallel processing"

		- That means none of the data I was dealing with - the registry, the atomics - actually needed to be shared

		- Optimization 1. Another router-worker type architecture where each transaction request gets their own "handle request" function yes where they have the votes reader task and deserialize the transaction, but they all route the transaction to a single "consensus engine" task with no shared state. So that means registry, sequence counters, and view numbers are non-shared 

		- Optimization 2. Thinking more about bottlenecks, I was looking at my code. "if let Err(_) = commit_sender.send". *I need to handle failed transactions very carefully,* I thought. *Failure means a messed-up state machine and wrong hashes*. But then, how and when would I handle this

- June 12th (actually):

	- Yeah, about the single consensus engine task...*sucks teeth* router patten with it means that I need crazy message passing for managing ownership, because consensus engine needs more than just the transaction.

	I'm thinking single-permit semaphore to make this *effectively* sequential, to get rid of contention and of course acquiring the lock only once for the entire length of the consensus engine (cause we lock and loop through the registry twice, wasteful asf)

	but we *technically* don't need the mutex if we go for the singe semaphore, which rust's type system doesn't care about because arc doesn't implement DerefMut... (translation for normies: rust's compiler literally won't let you edit the Arc itself)

	dude this is exactly what claude was talking about one time with you needing to sometimes design around not the actual concurrency models and more constructed ownership models in rust. like it's not bad, concurrency models are encoded *in* those ownership models, but like, there's that grey area with zero overlap between the two and erring on the side of safety is fine just slows dev velocity a little.

- June 13th:
	- 1:00 AM: All the "shared state" (ownership misnomer) I plan to pack into a single mutex whose lock I instantly acquire only once. that removes:

		- Extra mutex lock (previously talked about before, I had 2 Mutex lock acquisitions)

		- Multiple (I think at least 3+) atomic operations for the sequence and view number reading and writing, which nukes performance if contended which are still heavy even if under no contention

- June 17th
	- 10:00 AM: He returns! Laziness has yet to win!

		- Leader logic is taking heavy optimization time, although I think once I have the leader logic down peer logic is more or less in the bag. I hope

		- I've gone ahead and implemented the consensus tools (registry, sequence number/view number) into a single struct behind a tokio lock that's blazing fast to acquire (under zero contention it's just an atomic toggle)

			- This gets rid of the crazy heavy atomic op spamming of the view and sequence number reading and writing

		- I also added timeouts to the broadcasting/tcpstream sending logic so the engine wouldn't hang on trying to send to a dead peer

- June 18th
	- 9:57 PM: More work on the client logic. I've been ignoring it this past week+ because of leader concurrency logic rewrites
	
		- The bootnode strategy is, create a new, seperate file/server to host the bootnode logic to decouple it from the leader privliges. New nodes connect to bootnode first which gives them a list of connected addresses

			- In a production system there would be a hashset to track which addresses already exist to avoid connection conflict. However, I will be working with like 5, 10 servers max. of 7900+ possible addresses/ports. Not only is that like a 3% chance of conflict, it's *client-side* conflict that gets resloved by looping the conneciton logic

			- for base verification (account balance checks and whatnot), that's not the scope/purpose of this project. I *might* make an anti-overwrite service (once you claim a key, no one else can write to it), but who knows

		- BOOTSTRAPPING STRATEGY: I understand how inefficient this is, but we're going for simplicity and foundational skills and knowledge in this implementation. Because of that, the boostratpping strategy I will use is Initial Block Download and verification of the entire chain (how bitcoin does it)

- June 20th
	- 4:30 AM: Yes back to leader logic. You look at your code, trace through it, and find subtle bugs and concurrent logic errors that make you fear the concept of writing networking protocol/distributed systems code like backend code

		- For example, take my handle client function. I spawned the reader task for step 2 *after* calling the consensus engine. The engine would be stuck waiting for reader responses that weren't coming

		- Or the framed "loop". I was looking at my code and thinking, *how do we get mutliple transactions from the same node? I pass ownership of everything important to the consensus function*. Then I look closer to understnad why rust never flagged it, and the reason was it was an *if* let, not a *while* let.

	- 5:37 AM: I understand that I haven't been documenting this - because documenting does nothing but fuck with my thought processs - but I'm really going through it in deciding what I should do. 

		- Packing everything into a tools struct may or may not be the right move. The idea is, we have to look at the reality of what is actually happening on the machine, not what the rust type system says. what's actually happening is that transactions - calling the consensus engine - is exponentially more frequent then new nodes joining. And so that crazy message passing I was scared of doesn't really happen as the registry is with the consensus engine 99.999% of the time

		- That means, message passing is a trivial, tail event cost. I can create a timeout and if the chain is under low load, I send the registry back for new node registration. I search up the way pros do it and they wait for time slices called "epochs" to end before they allow new nodes but I'll stick with timeout.

		- this shifts the bottleneck from not the registry, but from the other tools that consensus still needs - senders and recievers. They'd need to be passed back, and sometimes forth, over and over again in each transaction

	- 9:52 AM: Which means I'm going back to the single atomic 

- June 22nd
	- 9:11 PM: Big developments. I have always known that thread-safe primitives are not actually needed in my project but I never did anything about it because I assumed shared requirements. But I had thought to myself, "what alternatives are there?" and to the internet I went. Of course the answer was Arc<Mutex>'s single threaded cousin - Rc<RefCell>. I was genuinely so excited to finally use these insanely overlooked parts of the rust std. 

- June 25th
	- 11:03 PM: We're working backwards with documentation here. I think I've landed on my final architecture, which really is just an adaptation of an incredibly common architecture. Yes we are back to the Actor model.
		- If you're cracked at Rust and Tokio, you'd know the actual issue with single semaphore. No, C++ scum, it's not contention (single threaded, and semaphore automatically creates an internal queue), and it's not the atomic overhead of the semaphore counter. It's something that requires true Tokio knowledge - the waker overhead.
			- The way that the Tokio scheduler works is that when a task hits an await, calls the poll function (returning Poll::Pending), it registers said waiting task with a Waker object tied to the I/O event that it's waiting for and places it into a waiting queue. A linked list. 

			- That's the issue. Single Semaphore puts tasks waiting to execute the hot loop *to sleep* - it doesn't matter that the "I/O" event they're waiting on is an atomic usize toggle, task management overhead - which is lightweight for backend *filthy chuds* who aren't doing real low latency work - is far too high for us performance engineers. 

			- We'd need waiting tasks to stay awake while they're waiting to enter into the hot loop. Unless the tasks "entering into the hot loop" wasn't something that was done on current thread...

			- I have already discussed both the dual runtime architecture and the "crazy message passing" required for a hot loop actor strategy. Here's how I mitigated those worries

				- Yeah, there is crazy message passing. That's how all tasks - the task-per-connection reader tasks living in the multithreaded runtime (simply just to pass on network events from client sockets), and the onboarding task that accepts connection requests from peer nodes (to request a node socket be added to the registry) - communicate with the consensus tasks. But really? I use a non-atomic queue/channel to pass between tasks on the single thread, and there was already going to be message passing between 

- June 26th:
	- 10:26 PM: Finally back to working on the whole system. 
		- I spoiled plans on the registration (to be continued)

- June 27th:
	11:54 AM: Like I said, working on the whole system. Finally breaking ground on the bootnode logic
		- Earlier I discussed registration mechanics and how the bootnode stays in sync with the rest of the network on the registry. The plan is, whenever the consensus actor gets a registration request, it then takes the updated list (I needed to change the registry to store not just the sockets but the addresses as well) and sends it to a long lived connection to the bootnode, which will then simply replace its registry with the value from the network.

- June 29th
	- 8:22 PM: Still working on the bootnode logic, we run into a similar issue as the consensus hot loop. The plauge of deciding how to handle shared state. The scene is that we only have two tasks in the bootnode file - the task that recieves new nodes from the leader node to update bootnode internal registry, and the task that sends new nodes registry and full transaction history
	
	- Each of those tasks need exclusive write access to the registry specifically, because one of them specifically might completely relocate the buffer. So that means I'm thinking of single threaded runtime as well

	- That brings up the same issue as before - we'd need the task to wait before it gets scheduled. Similar to consensus actor since we're doing repeated network requests there'd be multiple await points, meaning tokio might schedule task b, which means we'd need a true await system...

	- Of course that brings back the idea of single semaphore, which I trashed due to waker thrashing overhead (waker registration schematics), but wouldn't be the wrong tool here because node registration isn't a hot loop.

	- Both of the following methods - tokio mutex and single semaphore - are incredibly similar in performance on single thread, because tokio mutex is implemented as a single semaphore under the hood, but Rc + tokio mutex was chosen because runtime Refcell tracking may add slight overhead

	- "Well if new nodes need to connect to the bootnode first why don't you have the bootnode handle registration?" Well I didn't think of that 3 weeks ago. And I'm tired of stripping logic out of the infra_main file. 

	- I'm also super tired of writing the words "registry" and "socket" and "registration", and "buffer"

- July 1st
	- 2:15 AM: Finishing up p2p layer work. Logic is, leader node sends bootnode list of addresses in a tightly packed payload. Debated fastest ways to split up the payload along address lines - Framed.next? BytesMut.chunks_exact? 

	- Key is copying. We get the payload from the network inside a Framed.next, so it gets dropped at the end of the while loop iteration. Until the address list is empty, we copy into the master address list in 14 byte chunks - addr_list.split_to, and then the master list extends from slice of that split_to.

	- Then, we're allowed to create a staging bytesmut for us to create connection packets and bincode to serialize those packets into, and then send the entire tightly packed list, this time as a list of connection packets.

	- ...This is stupid. Universal language is the key. Instead of having mismatched formats we use Connection packet on infra main as well

- July 3rd
	- (Retroactive documentation)

- July 5th
	- 9:38 AM: I have addressed the fact that this is not the most effecient p2p networking strategy that *I* can create, and that I saw visible improvements (you might see some as well but if I don't then they aren't happening). However, I'm looking at code structure now, seeing the "unused variable" warning squiggles unde the connection address variable, and thinking "this is stupid". I think a about improvement, and perhaps it isn't as far away as I thought.

		- I'm thinking a single task bootnode. A new peer connects to the bootnode and the bootnode handles registration in that same function. No need for multiple tasks, and no need for bootnode to communicate with the leader node at all (under a static leader system, which this implementation will be)

		- Which in the bootnode code is rearranging some things and the leader node code is *removing* some lines. Simple fix, simple win.

	- 9:51 AM: More thought and actually? We're not doing that. Let's think about some basic blockchain architecture. Ownership of the registry being with the bootnode means a heartbeat system and furious communication with the leader to keep its registry fresh, when...the leader can just handle it itself.

- July 7th
	- 9:05 AM: going back to work on peer node logic after a week of bootnode "work" (that word is used loosely here). Need to review cryptograpic signing and commit certificate/prepare vote certification (verifying each peer is who they say they are in the leader node logic as well)

- July 8th
	- 12:47 PM: Here's the issue at hand. For receiving votes, the consensus actor gets those votes as an anonyomus packet of bytes. Adding headers to those bytes during heavy vote broadcasting is a waste of bandwidth. So we store pubkeys locally. 

		- Simple solution, right? Issue is, the packet is *just* the signed vote. that's it. There's no way for us to know who's who with just the packet we get from the reader actor

		- That means in order to know who's who, we'd need to move verification to the reader runtime and only have it send verified votes through the channel.

- July 10th
	- 11:54 PM: On the peer node logic, Mutex or Channels/lock free queues. That's the question of the month. Let's map out the path of a transaction in order to get a better read on what the actual situation is:

		- Client sends a transaction request to the peer node. Peer node verifies the transaction hash with the client's pubkey (and on actual chains checks balances and permissions). Once the transaction is deemed 'valid', what the peer...

		- Nah... I have a bit of a bias against channels but they're clearly the correct choice here. I accept client transactions, verify, drop the transaction into the consensus engine channel, and then move onto the next client

- July 11th
	- 1:19 AM: I don't get any value by retyping everything here. So this is what I arrived to summarized: 
		- Option 2 confirmed: consensus engine does its own crypto verification, not the accepting workers.

		- Workers race to accept client transactions, verify signatures themselves, drop into the engine's channel, keep accepting.

		- Skipped verify_batch. All-or-nothing failure means no way to ID which peer sent a bad signature — dealbreaker in a Byzantine system. Also pointless: network round trips dominate, not verification cost.

- ??? (Some work done some day) (I couldn't be bothered to continue retroactive documentation)

- July 17th 
	- 1:16 AM: Mostly util work on the infra peer done today. Made utils to automate the making of codecs and frameds. Poor sleep schedule - I mean like staying up all night and then not really sleeping during the day, going to bed around 1 or 2 am is actually an achievement for me - is finally really catching up to be and so locking in on actually important logic has been impossible 

- August 1st
	- Literally *just* started testing the code. Which is good, brings into view some contradictions to deal with:
		- Should the bootnode connect to the leader first or the leader connect to the bootnode first? Let's reason about what failure looks like for each

		#### BEWARE - THE REASONING BELOW IS COMPLETE NONSENSE

		- Bootnode connects to leader - The bootnode is the ultimate relay station for the network, and while the network doesn't *run* without the leader, it doesn't *exist* without the bootnode. No new nodes could join. So bootnode tries to connect to leader, fails, and keeps trying 

		- Bootnode should connect to the leader. The bootnode... (*realizes just spewed a bunch of nonsense*)

		#### NONSENSE CONCLUDED

		- Leader should connect to bootnode. If node A is connecting to node B, then the A -> B relationship *failing* needs to not be as dangerous than the other way around.

		- Leader A, Bootnode B. If the leader connecting to the bootnode fails - the bootnode is offline - , you just...give the peer nodes the leader address and peer node addresses to connect to by hand

		- Leader B, Bootnode A. if the bootnode connecting to the leader fails, right now as my code is written the network does not work. Even if I had leader change logic there would need to be a heavy consensus execution for a leader change. Much more expensive than copying and pasting some IP addresses into a vec

		- Leader connecting to bootnode wins

- August 8th
	- Yes, we're still doing this. 

	- Massive logic implementations for the Peer node. I was going with the flow on how the peer node was shaping up - an inverted version of the leader architecture, where the multithreaded runtime was the main runtime and the consensus engine actor was the one where I spawned and handed things off to.

	- I wanted high code reusability between the two files, and with the inverted architecture that wasn't going to happen. So peer node architecture now reflects leader node. The main runtime is the consensus engine management runtime whereas

		- This isn't the most optimal architecture. It would be more advantageous if the consensus logic only ran on its own core, so that conensus logic wouldn't have to fight onboarding logic for Li1 space

- August 11th and 12th
	- Finally optimized my deserialization loop to be zero copy - getting rid of the extra buffer bincode would allocate for data upon receiving bytes from the network. Caused a deep dive into lifetimes, refactoring ConnectionPacket to use `&'a [u8]` instead of `Bytes`, and, for the Bootnode Storage, needing to store peer addresses in a seperate storage struct that I hope bincode can translate between
