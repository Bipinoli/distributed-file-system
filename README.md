# Distributed file system based on Frangipani paper

Frangipani paper (https://pdos.csail.mit.edu/6.824/papers/thekkath-frangipani.pdf)

This was part of our distributed systems course.


Some highlights:
- File system operations based on [FUSE](https://www.kernel.org/doc/html/next/filesystems/fuse.html) (userspace filesystem framework in Linux)
- Reliable RPC (at-least-once model with retransmission) with sequence number and sliding window approach
- Separate lock server & extent server. The Lock server provides locks for concurrent filesystem operations, and the extent server keeps the directory tree & all the content.
- Distributed lock implementation (lock server). Not session dependent i.e. a granter doesn't keep the state within (e.g. mutex, conditions) so it's independent of the granted.
- Caching locks in the clients for efficiency
- Replication of lock servers & keeping them in sync using [replicated state machine approach](https://courses.mpi-sws.org/ds-ws18/labs/schneider-rsm.pdf)(RSM)
- Implemented [basic paxos](https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) for consensus on view changes such as master failure, new nodes, etc. in RSM
