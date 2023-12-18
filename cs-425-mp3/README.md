## Simple Distributed File System (SDFS)

This repository presents a simple distributed file system which can be used to store, read, and delete files across multiple machines. Data in the SDFS is re-replicated when machines fail.

Usage:

Update list of VMs being used in ssh.py (excluding the introducer). Then run `python3 ssh.py` from the first machine (i.e. the introducer). This will also start the introducer which is joined to the network by default.

Run `python3 main.py` from every machine. You can enter `join` to join the network.

`list_mem` will print the membership list at any point.

`store` will print the files stored in the SDFS.

`ls sdfs_filename` prints the VM addresses where sdfs_filename is stored.

`put local_filename sdfs_filename` adds a local file into the SDFS.

`get sdfs_filename local_filename` transfers a file from the SDFS into the local filesystem.

`delete sdfs_filename` deletes a file from the SDFS.

`multiread sdfs_filename local_filename VM1 VM2 VM3 VM4` starts a read operation on all VMs listed (VM1, VM2, VM3, VM4) and transfers sdfs_filename into the local filesystem.

Other additional commands:

`enable suspicion` will switch from gossip mode to gossip+suspicion mode for failure detection.

`disable suspicion` will switch back to gossip mode for failure detection.

Any failures (and suspected failures) will be detected and printed on the terminal. Logs for debugging are also populated on every machine.
