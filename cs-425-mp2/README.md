## Distributed Failure Detector

Usage:

Update list of VMs being used in ssh.py (excluding the introducer). Then run `python3 ssh.py` from the first machine (i.e. the introducer). This will also start the introducer which is joined to the network by default.

Run `python3 main.py` from every machine. You can enter `join` to join the network.

`list_mem` will print the membership list at any point.

`enable suspicion` will switch from gossip mode to gossip+suspicion mode.

`disable suspicion` will switch back to gossip mode.

Any failures (and suspected failures) will be detected and printed on the terminal. Logs for debugging are also populated on every machine.
