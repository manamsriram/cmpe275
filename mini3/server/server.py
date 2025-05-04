#!/usr/bin/env python3
import os
import json
import random
import argparse
import grpc
import time
import threading
from collections import deque
from concurrent import futures
from proto import crash_pb2, crash_pb2_grpc
import psutil


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
    def __init__(self, node_id, nodes, adj, peers):
        self.node_id = node_id
        self.nodes = nodes
        self.adj = adj
        self.peers = peers
        self.path_cache = {}

        # vector_clocks[row_id] = { node_id: clock, … }
        self.vector_clocks = {}
        # node_sets[row_id] = set(node_ids holding that row)
        self.node_sets = {}
        # each follower’s local copy for answering queries
        self.local_records = {}

        # Raft state
        self.current_term = 0
        self.voted_for = None
        self.is_leader = False
        self.state = "follower"  # follower, candidate, or leader
        self.leader_id = None
        self.votes_received = 0
        self.vote_requests_seen = {}
        self.election_timer = None
        self.heartbeat_timer = None
        self.election_timeout = random.uniform(300, 600) / 1000  # 300-600ms
        self.heartbeat_interval = 50 / 1000  # 50ms

        self.last_score_time = time.time()
        self.score_time_interval = 10  # Calculate every 10 seconds
        self.rows_since_last_score = 0
        self.score_row_threshold = 10000  # Calculate every 10,000 rows

        # Start health monitoring thread
        health_thread = threading.Thread(target=self.monitor_node_health, daemon=True)
        health_thread.start()

        t = threading.Thread(target=self._log_store_count, daemon=True)
        t.start()

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
        self.max_store_bytes = 200 * 1024 * 1024  # 200MB limit

        self.path_cache = {}

        # track processed row_ids
        self.seen_ids = set()

        # Start election timer
        self.reset_election_timer()

    def _log_store_count(self):
        while True:
            time.sleep(10)
            size_bytes = self.store_size
            size_mb = size_bytes / (1024 * 1024)
            count = len(self.store)
            print(
                f"[{self.node_id}] Stored records count: {count}, storage used: {size_bytes} bytes ({size_mb:.2f} MB)"
            )

    def calculate_server_score(self):
        # Get system load average (1, 5, 15 minute averages)
        try:
            load_avg = os.getloadavg()[0]  # Use 1-minute average
        except AttributeError:
            load_avg = 0  # Default for Windows which doesn't have getloadavg

        # Get current I/O wait percentage - handle platform differences
        cpu_times = psutil.cpu_times_percent()
        io_wait = getattr(cpu_times, "iowait", 0)  # Default to 0 if not available

        # Rest of your code remains the same
        net_io = psutil.net_io_counters()
        net_usage = (net_io.bytes_sent + net_io.bytes_recv) / (1024 * 1024)
        memory_stored = self.store_size / (1024 * 1024)

        score = (
            (0.3 * min(100, load_avg * 10))
            + (0.3 * io_wait)
            + (0.2 * min(100, net_usage))
            + (0.2 * min(100, memory_stored))
        )

        return {
            "server_id": self.node_id,
            "score": score,
            "load_avg": load_avg,
            "io_wait": io_wait,
            "net_usage_mb": net_usage,
            "memory_stored_mb": memory_stored,
        }

    def PropagateResourceScore(self, request, context):
        """Propagate resource score request through the network"""
        # Track visited nodes to prevent cycles
        visited_nodes = list(request.visited_nodes)

        # If we've already seen this request, don't process it again
        if self.node_id in visited_nodes:
            return crash_pb2.ResourceScoreResponse()

        # Add ourselves to visited nodes
        visited_nodes.append(self.node_id)

        # Calculate our own score
        my_score = self.calculate_server_score()

        # Create a ResourceScore object for our score
        resource_score = crash_pb2.ResourceScore(
            server_id=my_score["server_id"],
            score=my_score["score"],
            load_avg=my_score["load_avg"],
            io_wait=my_score["io_wait"],
            net_usage_mb=my_score["net_usage_mb"],
            memory_stored_mb=my_score["memory_stored_mb"],
        )

        # Initialize response with our score
        collected_scores = [resource_score]

        # Propagate to neighbors
        for nbr_id in self.adj.get(self.node_id, []):
            if nbr_id not in visited_nodes:
                try:
                    # Create propagation request
                    prop_request = crash_pb2.ResourceScoreRequest(
                        original_requester=request.original_requester,
                        visited_nodes=visited_nodes,
                    )

                    # Forward to neighbor
                    response = self.stubs[nbr_id].PropagateResourceScore(prop_request)

                    # Add neighbor's collected scores to our collection
                    collected_scores.extend(response.collected_scores)
                except Exception as e:
                    print(
                        f"[{self.node_id}] Error propagating score request to {nbr_id}: {e}"
                    )

        # Return all collected scores
        return crash_pb2.ResourceScoreResponse(
            server_id=self.node_id,
            score=my_score["score"],
            load_avg=my_score["load_avg"],
            io_wait=my_score["io_wait"],
            net_usage_mb=my_score["net_usage_mb"],
            memory_stored_mb=my_score["memory_stored_mb"],
            collected_scores=collected_scores,
        )

    def reset_election_timer(self):
        if self.election_timer:
            self.election_timer.cancel()
        # Random timeout to prevent split votes - increased to 300-600ms
        timeout = random.uniform(300, 600) / 1000
        self.election_timer = threading.Timer(timeout, self.start_election)
        self.election_timer.daemon = True
        self.election_timer.start()
        # print(f"[{self.node_id}] Reset election timer, will timeout in {timeout:.3f}s")

    def reset_heartbeat_timer(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = threading.Timer(
            self.heartbeat_interval, self.send_heartbeat
        )
        self.heartbeat_timer.daemon = True
        self.heartbeat_timer.start()

    def start_election(self):
        if self.state == "leader":
            return

        # Become candidate
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = 1  # Vote for self
        self.leader_id = None

        print(f"[{self.node_id}] Starting election for term {self.current_term}")

        # Create vote request
        request = crash_pb2.VoteRequest(
            term=self.current_term, candidate_id=self.node_id
        )

        # Request votes from direct neighbors and propagate
        for nbr_id in self.adj.get(self.node_id, []):
            try:
                print(f"[{self.node_id}] Sending vote request to {nbr_id}")
                response = self.stubs[nbr_id].RequestVote(
                    request, metadata=[("seen_nodes", self.node_id)]
                )

                # Process response immediately
                if self.state != "candidate":
                    return

                if response.term > self.current_term:
                    self.current_term = response.term
                    self.state = "follower"
                    self.voted_for = None
                    self.reset_election_timer()
                    return

                if response.vote_granted:
                    self.votes_received += 1
                    print(
                        f"[{self.node_id}] Received vote from {nbr_id}, total: {self.votes_received}"
                    )

                    if self.votes_received > len(self.nodes) / 2:
                        self.become_leader()
                        return
            except Exception as e:
                print(f"[{self.node_id}] Error requesting vote from {nbr_id}: {e}")

        # DO NOT reset election timer here - this was causing issues

    def RequestVote(self, request, context):
        meta = dict(context.invocation_metadata())
        seen_nodes = meta.get("seen_nodes", "")
        seen_list = seen_nodes.split(",") if seen_nodes else []
        original_candidate = meta.get("original_candidate", request.candidate_id)

        # If the candidate's term is less than ours, reject
        if request.term < self.current_term:
            return crash_pb2.VoteResponse(term=self.current_term, vote_granted=False)

        # If the candidate's term is greater than ours, update our term
        if request.term > self.current_term:
            self.current_term = request.term
            self.state = "follower"
            self.is_leader = False
            self.voted_for = None

        # If we haven't voted for anyone yet in this term, vote for the candidate
        vote_granted = False
        if (
            self.voted_for is None or self.voted_for == request.candidate_id
        ) and request.term >= self.current_term:
            self.voted_for = request.candidate_id
            vote_granted = True
            # Reset election timer since we received a valid request
            self.reset_election_timer()

        print(
            f"[{self.node_id}] Received vote request from {request.candidate_id}, granted: {vote_granted}"
        )

        # Forward vote request to neighbors who haven't seen it yet
        seen_list.append(self.node_id)
        new_seen = ",".join(seen_list)

        for nbr_id in self.adj.get(self.node_id, []):
            if nbr_id not in seen_list:
                try:
                    # print(f"[{self.node_id}] Forwarding vote request to {nbr_id}")
                    self.stubs[nbr_id].RequestVote(
                        request, metadata=[("seen_nodes", new_seen)]
                    )
                except Exception as e:
                    print(
                        f"[{self.node_id}] Error forwarding vote request to {nbr_id}: {e}"
                    )

        # Send response directly to original candidate if not us
        if (
            original_candidate != self.node_id
            and original_candidate != request.candidate_id
        ):
            try:
                # Create stub if needed
                if original_candidate not in self.stubs:
                    for node in self.nodes:
                        if node["id"] == original_candidate:
                            addr = f"{node['address']}:{node['port']}"
                            channel = grpc.insecure_channel(addr)
                            self.stubs[original_candidate] = (
                                crash_pb2_grpc.CrashReplicatorStub(channel)
                            )

                # Send vote response directly
                self.stubs[original_candidate].ReceiveVote(
                    crash_pb2.VoteResponse(
                        term=self.current_term,
                        vote_granted=vote_granted,
                        voter_id=self.node_id,
                    )
                )
            except Exception as e:
                print(
                    f"[{self.node_id}] Error sending vote to original candidate {original_candidate}: {e}"
                )

        return crash_pb2.VoteResponse(term=self.current_term, vote_granted=vote_granted)

    def become_leader(self):
        if self.state != "candidate":
            return

        self.state = "leader"
        self.is_leader = True
        self.leader_id = self.node_id
        print(f"[{self.node_id}] Became leader for term {self.current_term}")
        
        # Clear path cache when becoming leader
        self.path_cache = {}
        
        # Cancel election timer and start sending heartbeats
        if self.election_timer:
            self.election_timer.cancel()
        self.send_heartbeat()

    def GetLeader(self, request, context):
        is_leader = self.state == "leader"
        leader_id = self.leader_id if not is_leader else self.node_id
        
        # Create a list of ALL known server endpoints from our nodes list
        server_endpoints = []
        for node in self.nodes:
            server_endpoints.append(crash_pb2.ServerEndpoint(
                server_id=node["id"],
                address=node["address"],
                port=node["port"]
            ))
        
        # Find the leader's address and port
        leader_address = ""
        leader_port = 0
        if leader_id:
            for node in self.nodes:
                if node["id"] == leader_id:
                    leader_address = node["address"]
                    leader_port = node["port"]
                    break
        
        return crash_pb2.LeaderResponse(
            is_leader=is_leader,
            leader_address=leader_address,
            leader_port=leader_port,
            server_endpoints=server_endpoints
        )


    def send_heartbeat(self):
        if self.state != "leader":
            return

        term = self.current_term

        # print(f"[{self.node_id}] Sending heartbeats for term {term}")

        for nbr_id in self.adj.get(self.node_id, []):
            try:
                request = crash_pb2.AppendEntriesRequest(
                    term=term, leader_id=self.node_id
                )

                # Initialize seen_nodes with just the leader
                seen_nodes = self.node_id

                # print(f"[{self.node_id}] Sending heartbeat to {nbr_id}")
                response = self.stubs[nbr_id].AppendEntries(
                    request, metadata=[("seen_nodes", seen_nodes)]
                )

                if response.term > self.current_term:
                    self.current_term = response.term
                    self.state = "follower"
                    self.is_leader = False
                    self.voted_for = None
                    self.reset_election_timer()
                    return
            except Exception as e:
                print(f"[{self.node_id}] Error sending heartbeat to {nbr_id}: {e}")

        # Schedule next heartbeat
        self.reset_heartbeat_timer()

    def AppendEntries(self, request, context):
        meta = dict(context.invocation_metadata())
        seen_nodes = meta.get("seen_nodes", "")
        seen_list = seen_nodes.split(",") if seen_nodes else []

        # If the leader's term is less than ours, reject
        if request.term < self.current_term:
            return crash_pb2.AppendEntriesResponse(
                term=self.current_term, success=False
            )

        # Valid leader, reset election timer
        self.reset_election_timer()

        # If the leader's term is greater than or equal to ours, update our state
        if request.term >= self.current_term:
            self.current_term = request.term
            self.state = "follower"
            self.is_leader = False
            self.leader_id = request.leader_id

        # print(
        #     f"[{self.node_id}] Received heartbeat from {request.leader_id} for term {request.term}"
        # )

        # Forward heartbeat to neighbors who haven't seen it yet
        seen_list.append(self.node_id)
        new_seen = ",".join(seen_list)

        for nbr_id in self.adj.get(self.node_id, []):
            if nbr_id not in seen_list:
                try:
                    # print(f"[{self.node_id}] Forwarding heartbeat to {nbr_id}")
                    self.stubs[nbr_id].AppendEntries(
                        request, metadata=[("seen_nodes", new_seen)]
                    )
                except Exception as e:
                    print(
                        f"[{self.node_id}] Error forwarding heartbeat to {nbr_id}: {e}"
                    )

        original_leader = meta.get("original_leader", request.leader_id)
        if original_leader != self.node_id and original_leader != request.leader_id:
            try:
                if original_leader not in self.stubs:
                    # Create stub if needed
                    for node in self.nodes:
                        if node["id"] == original_leader:
                            addr = f"{node['address']}:{node['port']}"
                            channel = grpc.insecure_channel(addr)
                            self.stubs[original_leader] = (
                                crash_pb2_grpc.CrashReplicatorStub(channel)
                            )

                # Send heartbeat ack directly to leader
                self.stubs[original_leader].HeartbeatAck(
                    crash_pb2.HeartbeatAckRequest(
                        term=self.current_term,
                        follower_id=self.node_id,
                        success=True,
                    )
                )
            except Exception as e:
                print(
                    f"[{self.node_id}] Error sending heartbeat ack to leader {original_leader}: {e}"
                )

        # For heartbeats, just acknowledge
        return crash_pb2.AppendEntriesResponse(term=self.current_term, success=True)

    def _store(self, raw):
        self.store.append(raw)
        self.store_size += len(raw)
        while self.store_size > self.max_store_bytes:
            old = self.store.popleft()
            self.store_size -= len(old)

    def SendCrashes(self, request_iterator, context):
        """
        Client‐streaming RPC. Now accepts updates for the same row_id
        if the incoming vector_clock is larger than previously seen.
        """
        meta = dict(context.invocation_metadata())
        processed = 0

        # Parse headers for follower‐side routing
        if not self.is_leader:
            vector_clock = int(meta.get("vector_clock", "0"))
            node_set = meta.get("node_set", "")
            paths_meta = meta.get("paths") or ""
            pos_meta = int(meta.get("pos", "0"))

        for record in request_iterator:
            processed += 1
            rid = record.row_id

            # Determine incoming clock
            incoming_vc = vector_clock if not self.is_leader else 0
            # Get this node’s previously stored clock for rid
            prev_vc = self.vector_clocks.get(rid, {}).get(self.node_id, -1)

            # Skip if we've already stored a version as new or newer
            if rid in self.seen_ids and incoming_vc <= prev_vc:
                continue

            # Mark seen and update this node’s clock
            self.seen_ids.add(rid)
            self.vector_clocks.setdefault(rid, {})[self.node_id] = incoming_vc

            self.rows_since_last_score += 1

            raw = record.SerializeToString()

            if self.is_leader:
                # Check if we need to recalculate scores
                current_time = time.time()
                time_elapsed = current_time - self.last_score_time
                recalculate = (
                    time_elapsed >= self.score_time_interval
                    or self.rows_since_last_score >= self.score_row_threshold
                )

                if recalculate:
                    try:
                        # Reset counters
                        self.last_score_time = current_time
                        self.rows_since_last_score = 0

                        # Create initial request with ourselves as original requester
                        request = crash_pb2.ResourceScoreRequest(
                            original_requester=self.node_id, visited_nodes=[]
                        )

                        # Start propagation from ourselves
                        response = self.PropagateResourceScore(request, context)

                        # Sort collected scores (lower is better)
                        sorted_scores = sorted(
                            response.collected_scores, key=lambda x: x.score
                        )

                        # Choose the 2 best server IDs
                        best_servers = [
                            s.server_id
                            for s in sorted_scores[: min(2, len(sorted_scores))]
                        ]

                        self.cached_targets = best_servers

                    except Exception as e:
                        print(f"[{self.node_id}] Error collecting resource scores: {e}")
                        # Fall back to random selection
                        self.cached_targets = random.sample(self.nodes, 2)

                # Use cached_targets (list of IDs), or fall back to two random IDs
                if hasattr(self, "cached_targets"):
                    targets = self.cached_targets
                else:
                    targets = random.sample([n["id"] for n in self.nodes], 2)

                node_set = ",".join(targets)
                vector_clock = 0  # resets for downstream

                # record intended replicas
                self.node_sets[rid] = set(targets)

                # build paths
                paths = {}
                for tid in targets:
                    key = (self.node_id, tid)
                    pth = self.path_cache.get(key) or find_path(
                        self.adj, self.node_id, tid
                    )
                    if pth:
                        self.path_cache[key] = pth
                        paths[tid] = pth
                paths_meta = json.dumps(paths)
                pos = 0
            else:
                try:
                    paths = json.loads(paths_meta) if paths_meta else {}
                    pos = pos_meta
                except json.JSONDecodeError:
                    print(f"[{self.node_id}] Invalid JSON in paths_meta: {paths_meta}")
                    paths = {}

            # Route or store
            for tid, pth in paths.items():
                if pos < 0 or pos >= len(pth) or pth[pos] != self.node_id:
                    continue

                # Final hop: store/overwrite locally
                if pos == len(pth) - 1:
                    self._store(raw)
                    self.local_records[rid] = record
                    # Ensure this node is in the set
                    self.node_sets.setdefault(rid, set()).add(self.node_id)
                    print(
                        f"[{self.node_id}] stored/updated row {rid}, vc={incoming_vc}"
                    )

                else:
                    # Forward onward
                    next_hop = pth[pos + 1]
                    stub = self.stubs.get(next_hop)
                    if not stub:
                        continue
                    meta_out = [
                        ("paths", paths_meta),
                        ("pos", str(pos + 1)),
                        ("vector_clock", str(vector_clock)),
                        ("node_set", node_set),
                    ]

                    def one():
                        yield record

                    try:
                        ack = stub.SendCrashes(one(), metadata=meta_out)
                        if not ack.success:
                            print(
                                f"[{self.node_id}] fwd to {next_hop} failed: {ack.message}"
                            )
                    except Exception as e:
                        print(f"[{self.node_id}] Error forwarding to {next_hop}: {e}")

        return crash_pb2.Ack(success=True, message=f"Processed {processed} records")

    def HeartbeatAck(self, request, context):
        if self.state != "leader":
            return crash_pb2.HeartbeatAckResponse(received=False)

        print(
            f"[{self.node_id}] Received heartbeat ack from {request.follower_id} for term {request.term}"
        )

        # You could track which followers have acknowledged heartbeats
        # This is useful for monitoring cluster health

        return crash_pb2.HeartbeatAckResponse(received=True)

    def QueryRow(self, request, context):
        """
        Unary RPC to fetch a single row by row_id via routed forwarding.
        Leader:
          - broadcast routed QueryRow to every node
          - collect successes
          - if successes < RF, repair missing replicas
          - if successes == 0, return NOT_FOUND
          - return freshest record
        Followers:
          - if local, reply
          - else forward to leader
        """
        rid = request.row_id
        meta = dict(context.invocation_metadata())
        path_json = meta.get("query_path")

        # 1) Routed hop-by-hop
        if path_json is not None:
            path = json.loads(path_json)
            pos = int(meta.get("query_pos", "0"))
            if rid in self.local_records:
                return crash_pb2.QueryResponse(record=self.local_records[rid])
            if pos < len(path) - 1:
                return self.stubs[path[pos + 1]].QueryRow(
                    request,
                    metadata=[("query_path", path_json), ("query_pos", str(pos + 1))],
                )
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return crash_pb2.QueryResponse()

        # 2) Non-leader forwards to leader
        if not self.is_leader:
            if not self.leader_id:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                return crash_pb2.QueryResponse()
            leader = next(n for n in self.nodes if n["id"] == self.leader_id)
            stub = crash_pb2_grpc.CrashReplicatorStub(
                grpc.insecure_channel(f"{leader['address']}:{leader['port']}")
            )
            return stub.QueryRow(request)

        # 3) Leader entrypoint: gather successes
        successes = []
        if rid in self.local_records:
            successes.append((self.node_id, self.local_records[rid]))

        for node in self.nodes:
            nid = node["id"]
            if nid == self.node_id:
                continue
            path = find_path(self.adj, self.node_id, nid)
            if not path or len(path) < 2:
                continue
            try:
                resp = self.stubs[path[1]].QueryRow(
                    request,
                    metadata=[("query_path", json.dumps(path)), ("query_pos", "1")],
                )
                successes.append((nid, resp.record))
            except grpc.RpcError:
                continue

        RF = 2

        # 4) If no successes, return NOT_FOUND
        if not successes:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"row_id {rid} not found on any node")
            return crash_pb2.QueryResponse()

        # 5) Repair if fewer than RF successes
        if len(successes) < RF:
            best_node, best_rec = successes[0]
            orig = self.node_sets.get(rid, {n["id"] for n in self.nodes})
            got_ids = {nid for nid, _ in successes}
            missing = orig - got_ids

            # re-replicate missing originals
            for m in missing:
                try:
                    self._replicate_to(m, best_rec)
                    got_ids.add(m)
                except:
                    # or pick a fresh node
                    candidates = [n["id"] for n in self.nodes if n["id"] not in got_ids]
                    if not candidates:
                        continue
                    new = random.choice(candidates)
                    try:
                        self._replicate_to(new, best_rec)
                        orig.discard(m)
                        orig.add(new)
                        self.node_sets[rid] = orig
                        got_ids.add(new)
                    except:
                        pass

            # overwrite one live replica to ensure freshness
            try:
                self._replicate_to(best_node, best_rec)
            except:
                pass

        # 6) Pick freshest and return
        best_node, best_rec = successes[0]
        return crash_pb2.QueryResponse(record=best_rec)

    # helper to replicate a single record to node `nid`
    def _replicate_to(self, nid, record):
        """
        Sends a one-off SendCrashes with the given record to node `nid`.
        Raises on RPC error.
        """
        # build a trivial path [leader -> nid]
        path = find_path(self.adj, self.node_id, nid)
        if not path or len(path) < 2:
            raise RuntimeError(f"No route to {nid}")
        stub = self.stubs[path[1]]
        meta = [
            ("paths", json.dumps({nid: path})),
            ("pos", "0"),
            # optionally carry forward vector_clock and node_set...
        ]

        # use a generator to wrap the single record
        def one():
            yield record

        ack = stub.SendCrashes(one(), metadata=meta)
        if not ack.success:
            raise RuntimeError(f"replication to {nid} failed: {ack.message}")
        
    def update_topology(self, new_node=None, removed_node=None):
        """
        Update the network topology when a node joins or leaves.
        Clears path cache and triggers a new leader election if needed.
        """
        if new_node:
            # Add the new node to our list if not already present
            if new_node["id"] not in [n["id"] for n in self.nodes]:
                self.nodes.append(new_node)
                print(f"[{self.node_id}] Added new node {new_node['id']} to topology")
                
            # Update adjacency list if this node is a peer
            if new_node["id"] in self.adj.get(self.node_id, []):
                # Create stub for the new peer
                addr = f"{new_node['address']}:{new_node['port']}"
                channel = grpc.insecure_channel(addr)
                self.stubs[new_node["id"]] = crash_pb2_grpc.CrashReplicatorStub(channel)
                print(f"[{self.node_id}] Created connection to new peer {new_node['id']}")
        
        if removed_node:
            # Remove the node from our list
            self.nodes = [n for n in self.nodes if n["id"] != removed_node]
            
            # Update adjacency list
            if removed_node in self.adj.get(self.node_id, []):
                self.adj[self.node_id].remove(removed_node)
                # Close connection if it exists
                if removed_node in self.stubs:
                    del self.stubs[removed_node]
                    print(f"[{self.node_id}] Removed connection to peer {removed_node}")
        
        # Clear path cache since topology changed
        self.path_cache = {}
        
        # If the leader was removed, trigger a new election
        if removed_node and removed_node == self.leader_id:
            print(f"[{self.node_id}] Leader {removed_node} is down, starting election")
            self.leader_id = None
            self.start_election()
        
        # If we're the leader, recalculate paths
        if self.is_leader:
            print(f"[{self.node_id}] Topology changed, recalculating paths as leader")
            # Clear cached targets to force recalculation
            if hasattr(self, "cached_targets"):
                del self.cached_targets

    def detect_and_repair_network_partition(self):
        """
        Detect if the network is partitioned and attempt to repair it
        by establishing new connections between disconnected segments.
        """
        print(f"[{self.node_id}] Checking for network partitions...")
        
        # Build a graph of reachable nodes
        reachable = set([self.node_id])
        queue = [self.node_id]
        
        while queue:
            current = queue.pop(0)
            for nbr_id in self.adj.get(current, []):
                if nbr_id in reachable:
                    continue
                    
                # Check if we can reach this neighbor
                can_reach = False
                if current == self.node_id:
                    try:
                        self.stubs[nbr_id].GetLeader(crash_pb2.LeaderRequest(), timeout=1)
                        can_reach = True
                    except:
                        pass
                else:
                    # Assume nodes in our reachable set can reach their neighbors
                    can_reach = True
                    
                if can_reach:
                    reachable.add(nbr_id)
                    queue.append(nbr_id)
        
        # If we can't reach all nodes, we have a partition
        if len(reachable) < len(self.nodes):
            print(f"[{self.node_id}] Network partition detected! Reachable: {reachable}")
            
            # Find unreachable nodes
            unreachable = set(n["id"] for n in self.nodes) - reachable
            
            # Try to establish new connections to bridge the partition
            for r_id in reachable:
                for u_id in unreachable:
                    # Skip if already connected
                    if u_id in self.adj.get(r_id, []):
                        continue
                        
                    # Try to create a new connection
                    r_node = next((n for n in self.nodes if n["id"] == r_id), None)
                    u_node = next((n for n in self.nodes if n["id"] == u_id), None)
                    
                    if r_node and u_node:
                        print(f"[{self.node_id}] Attempting to bridge partition by connecting {r_id} to {u_id}")
                        
                        # Update adjacency lists
                        self.adj.setdefault(r_id, []).append(u_id)
                        self.adj.setdefault(u_id, []).append(r_id)
                        
                        # If we're creating a connection for ourselves, establish the stub
                        # If we're creating a connection for ourselves, establish the stub
                        if r_id == self.node_id:
                            # Create our stub to the unreachable node
                            addr = f"{u_node['address']}:{u_node['port']}"
                            channel = grpc.insecure_channel(addr)
                            self.stubs[u_id] = crash_pb2_grpc.CrashReplicatorStub(channel)
                            
                            # Now tell the unreachable node to create a stub back to us
                            try:
                                # Create a topology update request
                                request = crash_pb2.TopologyUpdateRequest(
                                    update_type="add",
                                    node_id=self.node_id,
                                    address=next((n["address"] for n in self.nodes if n["id"] == self.node_id), ""),
                                    port=next((n["port"] for n in self.nodes if n["id"] == self.node_id), 0)
                                )
                                
                                # Send the request to create a connection back to us
                                self.stubs[u_id].UpdateTopology(request)
                                print(f"[{self.node_id}] Notified {u_id} to create connection back to us")
                            except Exception as e:
                                print(f"[{self.node_id}] Failed to notify {u_id} to create connection: {e}")

                            
                        # Clear path cache to force recalculation of routes
                        self.path_cache = {}
                        return True
            
            return False
        return True


    def monitor_node_health(self):
        """
        Periodically check if all nodes are reachable.
        Triggers topology update if a node is unreachable.
        """
        while True:
            time.sleep(5)  # Check every 5 seconds
            
            for node in self.nodes:
                nid = node["id"]
                if nid == self.node_id:
                    continue
                    
                # Skip if not a direct peer
                if nid not in self.adj.get(self.node_id, []):
                    continue
                    
                try:
                    # Try to get leader info as a simple health check
                    request = crash_pb2.LeaderRequest()
                    self.stubs[nid].GetLeader(request, timeout=2)
                except Exception as e:
                    print(f"[{self.node_id}] Node {nid} appears to be down: {e}")
                    # Update topology to handle the node failure
                    self.update_topology(removed_node=nid)
                    # Re-replicate data from the failed node
                    # self.handle_node_failure(nid)
            self.detect_and_repair_network_partition()


    def RegisterNode(self, request, context):
        """
        Allow new nodes to register with the network.
        """
        new_node = {
            "id": request.node_id,
            "address": request.address,
            "port": request.port
        }
        
        # Update our topology with the new node
        self.update_topology(new_node=new_node)
        
        # If we're the leader, broadcast the updated topology
        if self.is_leader:
            self.broadcast_topology_update(new_node=new_node)
        
        return crash_pb2.RegisterNodeResponse(
            success=True,
            current_leader=self.leader_id or "",
            nodes=[n["id"] for n in self.nodes]
        )

    def GetAllNodes(self, request, context):
        """
        Return information about all nodes in the network.
        Used by clients when leader changes.
        """
        node_infos = []
        for node in self.nodes:
            node_infos.append(crash_pb2.NodeInfo(
                node_id=node["id"],
                address=node["address"],
                port=node["port"]
            ))
        
        return crash_pb2.GetAllNodesResponse(nodes=node_infos)
    
    def UpdateTopology(self, request, context):
        if request.update_type == "add":
            # Create a stub to the node that sent the request
            addr = f"{request.address}:{request.port}"
            channel = grpc.insecure_channel(addr)
            self.stubs[request.node_id] = crash_pb2_grpc.CrashReplicatorStub(channel)
            
            # Update adjacency list
            if request.node_id not in self.adj.get(self.node_id, []):
                self.adj.setdefault(self.node_id, []).append(request.node_id)
            
            print(f"[{self.node_id}] Created connection to {request.node_id}")
            
            # Clear path cache
            self.path_cache = {}
            
        return crash_pb2.TopologyUpdateResponse(success=True)

    def broadcast_topology_update(self, new_node=None, removed_node=None):
        """
        Broadcast topology changes to all nodes.
        """
        for node in self.nodes:
            nid = node["id"]
            if nid == self.node_id:
                continue
                
            try:
                # Create stub if needed
                if nid not in self.stubs:
                    addr = f"{node['address']}:{node['port']}"
                    channel = grpc.insecure_channel(addr)
                    self.stubs[nid] = crash_pb2_grpc.CrashReplicatorStub(channel)
                    
                # Send topology update
                if new_node:
                    request = crash_pb2.TopologyUpdateRequest(
                        update_type="add",
                        node_id=new_node["id"],
                        address=new_node["address"],
                        port=new_node["port"]
                    )
                else:
                    request = crash_pb2.TopologyUpdateRequest(
                        update_type="remove",
                        node_id=removed_node
                    )
                    
                self.stubs[nid].UpdateTopology(request)
            except Exception as e:
                print(f"[{self.node_id}] Error sending topology update to {nid}: {e}")


def serve(config_path):
    # load all configs and adjacency
    nodes, adj = load_configs(config_path)
    # load own config
    cfg = json.load(open(config_path, "r"))
    node_id = cfg["node_id"]
    address = cfg["address"]
    port = cfg["port"]

    # get peers
    peer_ids = adj.get(node_id, [])
    peers = [n for n in nodes if n["id"] in peer_ids]

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    servicer = CrashReplicatorServicer(node_id, nodes, adj, peers)
    crash_pb2_grpc.add_CrashReplicatorServicer_to_server(servicer, server)
    listen = f"{address}:{port}"
    server.add_insecure_port(listen)
    server.start()
    print(
        f"[{node_id}] Listening on {listen}, state={servicer.state}, peers={peer_ids}"
    )
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"[{node_id}] Shutting down...")
        if servicer.election_timer:
            servicer.election_timer.cancel()
        if servicer.heartbeat_timer:
            servicer.heartbeat_timer.cancel()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start CrashReplicator node")
    parser.add_argument("config", help="Path to node config JSON")
    args = parser.parse_args()
    serve(args.config)
