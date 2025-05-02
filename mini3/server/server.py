#!/usr/bin/env python3
import os
import json
import random
import argparse
import grpc
from collections import deque
from concurrent import futures
from proto import crash_pb2, crash_pb2_grpc


def load_configs(config_path):
    """
    Load all node configs from the same directory as `config_path`.
    Returns:
      nodes: list of {id,address,port}
      adj: dict mapping node_id to list of peer_ids
    """
    base_dir = os.path.dirname(os.path.abspath(config_path))
    nodes = []
    adj = {}
    for fname in os.listdir(base_dir):
        if not fname.lower().endswith(".json"):
            continue
        path = os.path.join(base_dir, fname)
        cfg = json.load(open(path, "r"))
        nid = cfg["node_id"]
        nodes.append({"id": nid, "address": cfg["address"], "port": cfg["port"]})
        adj[nid] = [p["id"] for p in cfg.get("peers", [])]
    return nodes, adj


class CrashReplicatorServicer(crash_pb2_grpc.CrashReplicatorServicer):
    def __init__(self, node_id, nodes, adj, peers, is_leader):
        self.node_id = node_id
        self.nodes = nodes
        self.adj = adj
        self.peers = peers
        self.is_leader = is_leader

        # stubs to direct neighbors
        self.stubs = {}
        for p in peers:
            pid = p["id"]
            addr = f"{p['address']}:{p['port']}"
            channel = grpc.insecure_channel(addr)
            self.stubs[pid] = crash_pb2_grpc.CrashReplicatorStub(channel)

        # in-memory bounded store
        self.store = deque()
        self.store_size = 0
        self.max_store_bytes = 150 * 1024 * 1024  # 150MB limit

        # track processed row_ids
        self.seen_ids = set()

    def _store(self, raw):
        self.store.append(raw)
        self.store_size += len(raw)
        while self.store_size > self.max_store_bytes:
            old = self.store.popleft()
            self.store_size -= len(old)

    def SendCrashes(self, request_iterator, context):
        meta = dict(context.invocation_metadata())
        count = 0
        for record in request_iterator:
            count += 1
            rid = record.row_id
            raw = record.SerializeToString()

            # skip if already processed this row
            if rid in self.seen_ids:
                continue
            self.seen_ids.add(rid)

            # leader picks new targets per record
            if self.is_leader:
                selected = random.sample(self.nodes, 2)
                target_ids = {t["id"] for t in selected}
                targets_md = ",".join(target_ids)
            else:
                targets_md = meta.get("targets")
                if not targets_md:
                    return crash_pb2.Ack(
                        success=False, message="Missing targets metadata"
                    )
                target_ids = set(targets_md.split(","))

            # local store if designated target
            if self.node_id in target_ids:
                self._store(raw)
                print(f"[{self.node_id}] Stored {rid} Targets are {target_ids}")
            else:
                print(f"[{self.node_id}] Skipped {rid} Targets are {target_ids}")

            # forward to all direct neighbors
            for nbr in self.adj.get(self.node_id, []):
                try:

                    def one():
                        yield record

                    # send with only targets metadata
                    self.stubs[nbr].SendCrashes(
                        one(), metadata=[("targets", targets_md)]
                    )
                    print(f"[{self.node_id}] Forwarded {rid} to neighbor {nbr}")
                except Exception as e:
                    print(f"[{self.node_id}] Error forwarding to {nbr}: {e}")

        return crash_pb2.Ack(success=True, message=f"Processed {count} records")

def serve(config_path):
    # load all configs and adjacency
    nodes, adj = load_configs(config_path)
    # load own config
    cfg = json.load(open(config_path, "r"))
    node_id = cfg["node_id"]
    address = cfg["address"]
    port = cfg["port"]
    
    # default leader placeholder
    # leader election goes here
    is_leader = node_id == "A"

    # get peers
    peer_ids = adj.get(node_id, [])
    peers = [n for n in nodes if n["id"] in peer_ids]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    servicer = CrashReplicatorServicer(node_id, nodes, adj, peers, is_leader)
    crash_pb2_grpc.add_CrashReplicatorServicer_to_server(servicer, server)
    listen = f"{address}:{port}"
    server.add_insecure_port(listen)
    server.start()
    print(f"[{node_id}] Listening on {listen}, leader={is_leader}, peers={peer_ids}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"[{node_id}] Shutting down...")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start CrashReplicator node")
    parser.add_argument("config", help="Path to node config JSON")
    args = parser.parse_args()
    serve(args.config)
