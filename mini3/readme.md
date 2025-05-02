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
- Message passing optimization (1 way only)
- Add and remove nodes from system (recheck for leaders)
- Score and message routing based on score
- Integration with leader election
- Health check
- Query from client (leader must find the replicated data)
- Network error handling (nodes going down or coming back up)
- Data updates 

