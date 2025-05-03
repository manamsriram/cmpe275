requires python 3.11.6 

Python packages
grpcio 1.71.0 grpcio-tools 1.71.0

Protobuf version
libprotoc 30.2 

Run guide

In the mini3 folder run 

`python run_all.py`

In another terminal run

`python -m client.client path_to_csv`

Todo:
- Score and message routing based on score (lowest wins)
- Health check (figure out what happens if the check fails: start new election and reconnect network for disconnected nodes. )
- Network error handling (nodes going down or coming back up)
- Query from client (leader must find the replicated data. If old node comes online again, then re-replicate to the old node. If client queries and query returns less
than replication factor then re-replicate that row. Assign new vector clock value.)