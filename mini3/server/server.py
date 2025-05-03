#!/usr/bin/env python3
import os
import json
import random
import argparse
import grpc
import threading
import time
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


def find_path(adj, start, end):
    """
    BFS to find a path from start to end in the adjacency map.
    Returns a list of node_ids [start, ..., end] or None if no path.
    """
    if start == end:
        return [start]
    visited = {start}
    queue = [[start]]
    while queue:
        path = queue.pop(0)
        node = path[-1]
        for nbr in adj.get(node, []):
            if nbr in visited:
                continue
            new_path = path + [nbr]
            if nbr == end:
                return new_path
            visited.add(nbr)
            queue.append(new_path)
    return None


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

        self.path_cache = {}

        # track processed row_ids
        self.seen_ids = set()

        # periodic logging thread
        t = threading.Thread(target=self._log_store_count, daemon=True)
        t.start()

    def _store(self, raw):
        self.store.append(raw)
        self.store_size += len(raw)
        while self.store_size > self.max_store_bytes:
            old = self.store.popleft()
            self.store_size -= len(old)

    def _log_store_count(self):
        while True:
            time.sleep(10)
            size_bytes = self.store_size
            size_mb = size_bytes / (1024 * 1024)
            count = len(self.store)
            print(
                f"[{self.node_id}] Stored records count: {count}, storage used: {size_bytes} bytes ({size_mb:.2f} MB)"
            )

    def SendCrashes(self, request_iterator, context):
        # capture incoming metadata for this RPC
        meta = dict(context.invocation_metadata())
        count = 0

        for record in request_iterator:
            count += 1
            rid = record.row_id
            # skip duplicates
            if rid in self.seen_ids:
                continue
            self.seen_ids.add(rid)

            # serialize once for store/forward
            raw = record.SerializeToString()

            # determine per-record paths & position
            if self.is_leader:
                # choose two targets (including self) for replication
                targets = random.sample(self.nodes, 2)
                paths = {}
                for t in targets:
                    key = (self.node_id, t["id"])
                    # use cached path if available
                    if key in self.path_cache:
                        pth = self.path_cache[key]
                    else:
                        pth = find_path(self.adj, self.node_id, t["id"])
                        if pth:
                            self.path_cache[key] = pth
                    if pth:
                        paths[t["id"]] = pth
                pos = 0
            else:
                # read precomputed paths & position
                paths_json = meta.get("paths")
                if not paths_json:
                    return crash_pb2.Ack(success=False, message="Missing path metadata")
                paths = json.loads(paths_json)
                pos = int(meta.get("pos", "0"))

            # for each target path, route record
            for tid, pth in paths.items():
                # validate current position
                if pos < 0 or pos >= len(pth):
                    continue
                # only act when this node is the current hop
                if pth[pos] != self.node_id:
                    continue
                # if final hop, store locally
                if pos == len(pth) - 1:
                    self._store(raw)
                    # print(f"[{self.node_id}] Stored {rid} for target {tid}")
                    continue
                # forward to next hop
                next_hop = pth[pos + 1]
                stub = self.stubs.get(next_hop)
                if not stub:
                    continue
                # prepare metadata for next
                meta_out = [("paths", json.dumps(paths)), ("pos", str(pos + 1))]

                def one():
                    yield record

                try:
                    ack = stub.SendCrashes(one(), metadata=meta_out)
                    if ack.success:
                        # print(
                        #     f"[{self.node_id}] Forwarded {rid} to {next_hop} (target {tid}) - ACK received: {ack.message}"
                        # )
                        pass
                    else:
                        print(
                            f"[{self.node_id}] Forward to {next_hop} reported failure: {ack.message}"
                        )
                except Exception as e:
                    print(f"[{self.node_id}] Error forwarding to {next_hop}: {e}")

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

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
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
