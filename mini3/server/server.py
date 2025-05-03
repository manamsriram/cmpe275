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
import psutil
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
    def __init__(self, node_id, nodes, adj, peers):
        self.node_id = node_id
        self.nodes = nodes
        self.adj = adj
        self.peers = peers
        self.path_cache = {}

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
        self.lock = threading.RLock()

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
        self.max_store_bytes = 150 * 1024 * 1024  # 150MB limit

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
        io_wait = getattr(cpu_times, 'iowait', 0)  # Default to 0 if not available
        
        # Rest of your code remains the same
        net_io = psutil.net_io_counters()
        net_usage = (net_io.bytes_sent + net_io.bytes_recv) / (1024 * 1024)
        memory_stored = self.store_size / (1024 * 1024)
        
        score = (0.3 * min(100, load_avg * 10)) + \
                (0.3 * io_wait) + \
                (0.2 * min(100, net_usage)) + \
                (0.2 * min(100, memory_stored))
        
        return {
            "server_id": self.node_id,
            "score": score,
            "load_avg": load_avg,
            "io_wait": io_wait,
            "net_usage_mb": net_usage,
            "memory_stored_mb": memory_stored
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
            memory_stored_mb=my_score["memory_stored_mb"]
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
                        visited_nodes=visited_nodes
                    )
                    
                    # Forward to neighbor
                    response = self.stubs[nbr_id].PropagateResourceScore(prop_request)
                    
                    # Add neighbor's collected scores to our collection
                    collected_scores.extend(response.collected_scores)
                except Exception as e:
                    print(f"[{self.node_id}] Error propagating score request to {nbr_id}: {e}")
        
        # Return all collected scores
        return crash_pb2.ResourceScoreResponse(
            server_id=self.node_id,
            score=my_score["score"],
            load_avg=my_score["load_avg"],
            io_wait=my_score["io_wait"],
            net_usage_mb=my_score["net_usage_mb"],
            memory_stored_mb=my_score["memory_stored_mb"],
            collected_scores=collected_scores
        )


    def reset_election_timer(self):
        with self.lock:
            if self.election_timer:
                self.election_timer.cancel()
            # Random timeout to prevent split votes - increased to 300-600ms
            timeout = random.uniform(300, 600) / 1000
            self.election_timer = threading.Timer(timeout, self.start_election)
            self.election_timer.daemon = True
            self.election_timer.start()
            # print(f"[{self.node_id}] Reset election timer, will timeout in {timeout:.3f}s")

    def reset_heartbeat_timer(self):
        with self.lock:
            if self.heartbeat_timer:
                self.heartbeat_timer.cancel()
            self.heartbeat_timer = threading.Timer(self.heartbeat_interval, self.send_heartbeat)
            self.heartbeat_timer.daemon = True
            self.heartbeat_timer.start()

    def start_election(self):
        with self.lock:
            if self.state == "leader":
                return
                
            # Calculate resource score before starting election
            my_score = self.calculate_server_score()
            # Become candidate
            self.state = "candidate"
            self.current_term += 1
            self.voted_for = self.node_id
            self.votes_received = 1  # Vote for self
            self.leader_id = None
            
            print(f"[{self.node_id}] Starting election for term {self.current_term}, score: {my_score['score']}")            
            # Create vote request
            request = crash_pb2.VoteRequest(
                term=self.current_term,
                candidate_id=self.node_id,
                score=my_score["score"],
                load_avg=my_score["load_avg"],
                io_wait=my_score["io_wait"]
            )

            
            # Request votes from direct neighbors and propagate
            for nbr_id in self.adj.get(self.node_id, []):
                try:
                    print(f"[{self.node_id}] Sending vote request to {nbr_id}")
                    response = self.stubs[nbr_id].RequestVote(
                        request,
                        metadata=[("seen_nodes", self.node_id)]
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
                        print(f"[{self.node_id}] Received vote from {nbr_id}, total: {self.votes_received}")
                        
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
        
        with self.lock:
            # If the candidate's term is less than ours, reject
            if request.term < self.current_term:
                return crash_pb2.VoteResponse(term=self.current_term, vote_granted=False)
                
            # If the candidate's term is greater than ours, update our term
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = "follower"
                self.is_leader = False
                self.voted_for = None

            my_score = self.calculate_server_score()

            # If we haven't voted for anyone yet in this term, vote for the candidate
            vote_granted = False
            if (self.voted_for is None or self.voted_for == request.candidate_id) and request.term >= self.current_term:
                if request.score <= my_score["score"]:
                    self.voted_for = request.candidate_id
                    vote_granted = True
                    # Reset election timer since we received a valid request
                    self.reset_election_timer()
                
            print(f"[{self.node_id}] Received vote request from {request.candidate_id}, score: {request.score}, my score: {my_score['score']}, granted: {vote_granted}")            
            # Forward vote request to neighbors who haven't seen it yet
            seen_list.append(self.node_id)
            new_seen = ",".join(seen_list)
            
            for nbr_id in self.adj.get(self.node_id, []):
                if nbr_id not in seen_list:
                    try:
                        print(f"[{self.node_id}] Forwarding vote request to {nbr_id}")
                        self.stubs[nbr_id].RequestVote(
                            request,
                            metadata=[("seen_nodes", new_seen)]
                        )
                    except Exception as e:
                        print(f"[{self.node_id}] Error forwarding vote request to {nbr_id}: {e}")
            
            # Send response directly to original candidate if not us
            if original_candidate != self.node_id and original_candidate != request.candidate_id:
                try:
                    # Create stub if needed
                    if original_candidate not in self.stubs:
                        for node in self.nodes:
                            if node["id"] == original_candidate:
                                addr = f"{node['address']}:{node['port']}"
                                channel = grpc.insecure_channel(addr)
                                self.stubs[original_candidate] = crash_pb2_grpc.CrashReplicatorStub(channel)
                    
                    # Send vote response directly
                    self.stubs[original_candidate].ReceiveVote(
                        crash_pb2.VoteResponse(
                            term=self.current_term,
                            vote_granted=vote_granted,
                            voter_id=self.node_id
                        )
                    )
                except Exception as e:
                    print(f"[{self.node_id}] Error sending vote to original candidate {original_candidate}: {e}")

            return crash_pb2.VoteResponse(term=self.current_term, vote_granted=vote_granted)

    def become_leader(self):
        with self.lock:
            if self.state != "candidate":
                return
                
            self.state = "leader"
            self.is_leader = True
            self.leader_id = self.node_id
            print(f"[{self.node_id}] Became leader for term {self.current_term}")
            
            # Cancel election timer and start sending heartbeats
            if self.election_timer:
                self.election_timer.cancel()
            self.send_heartbeat()

    def GetLeader(self, request, context):
        with self.lock:
            is_leader = self.state == "leader"
            leader_id = self.leader_id if not is_leader else self.node_id
            
            # If we know who the leader is
            if leader_id:
                # Find the leader's address and port
                for node in self.nodes:
                    if node["id"] == leader_id:
                        return crash_pb2.LeaderResponse(
                            is_leader=is_leader,
                            leader_address=node["address"],
                            leader_port=node["port"]
                        )
            
            # If we don't know who the leader is
            return crash_pb2.LeaderResponse(
                is_leader=is_leader,
                leader_address="",
                leader_port=0
            )

    def send_heartbeat(self):
        if self.state != "leader":
            return
            
        with self.lock:
            term = self.current_term
        
        # print(f"[{self.node_id}] Sending heartbeats for term {term}")
            
        for nbr_id in self.adj.get(self.node_id, []):
            try:
                request = crash_pb2.AppendEntriesRequest(
                    term=term,
                    leader_id=self.node_id
                )
                
                # Initialize seen_nodes with just the leader
                seen_nodes = self.node_id
                
                # print(f"[{self.node_id}] Sending heartbeat to {nbr_id}")
                response = self.stubs[nbr_id].AppendEntries(
                    request, 
                    metadata=[("seen_nodes", seen_nodes)]
                )
                
                with self.lock:
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
        
        with self.lock:
            # If the leader's term is less than ours, reject
            if request.term < self.current_term:
                return crash_pb2.AppendEntriesResponse(term=self.current_term, success=False)
                
            # Valid leader, reset election timer
            self.reset_election_timer()
            
            # If the leader's term is greater than or equal to ours, update our state
            if request.term >= self.current_term:
                self.current_term = request.term
                self.state = "follower"
                self.is_leader = False
                self.leader_id = request.leader_id
            
            # print(f"[{self.node_id}] Received heartbeat from {request.leader_id} for term {request.term}")
            
            # Forward heartbeat to neighbors who haven't seen it yet
            seen_list.append(self.node_id)
            new_seen = ",".join(seen_list)
            
            for nbr_id in self.adj.get(self.node_id, []):
                if nbr_id not in seen_list:
                    try:
                        # print(f"[{self.node_id}] Forwarding heartbeat to {nbr_id}")
                        self.stubs[nbr_id].AppendEntries(
                            request,
                            metadata=[("seen_nodes", new_seen)]
                        )
                    except Exception as e:
                        print(f"[{self.node_id}] Error forwarding heartbeat to {nbr_id}: {e}")
            
            original_leader = meta.get("original_leader", request.leader_id)
            if original_leader != self.node_id and original_leader != request.leader_id:
                try:
                    if original_leader not in self.stubs:
                        # Create stub if needed
                        for node in self.nodes:
                            if node["id"] == original_leader:
                                addr = f"{node['address']}:{node['port']}"
                                channel = grpc.insecure_channel(addr)
                                self.stubs[original_leader] = crash_pb2_grpc.CrashReplicatorStub(channel)
                    
                    # Send heartbeat ack directly to leader
                    self.stubs[original_leader].HeartbeatAck(
                        crash_pb2.HeartbeatAckRequest(
                            term=self.current_term,
                            follower_id=self.node_id,
                            success=True
                        )
                    )
                except Exception as e:
                    print(f"[{self.node_id}] Error sending heartbeat ack to leader {original_leader}: {e}")
            
            # For heartbeats, just acknowledge
            return crash_pb2.AppendEntriesResponse(term=self.current_term, success=True)

    def _store(self, raw):
        self.store.append(raw)
        self.store_size += len(raw)
        while self.store_size > self.max_store_bytes:
            old = self.store.popleft()
            self.store_size -= len(old)

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

            # Track rows since last score calculation
            self.rows_since_last_score += 1

            # serialize once for store/forward
            raw = record.SerializeToString()

            # determine per-record paths & position
            if self.is_leader:
                # Check if we need to recalculate scores
                current_time = time.time()
                time_elapsed = current_time - self.last_score_time
                recalculate = (time_elapsed >= self.score_time_interval or 
                            self.rows_since_last_score >= self.score_row_threshold)
                
                if recalculate:
                    try:
                        # Reset counters
                        self.last_score_time = current_time
                        self.rows_since_last_score = 0
                        
                        # Create initial request with ourselves as original requester
                        request = crash_pb2.ResourceScoreRequest(
                            original_requester=self.node_id,
                            visited_nodes=[]
                        )
                        
                        # Start propagation from ourselves
                        response = self.PropagateResourceScore(request, context)
                        
                        # Sort collected scores (lower is better)
                        sorted_scores = sorted(response.collected_scores, key=lambda x: x.score)
                        
                        # Choose the 2 best servers
                        best_servers = [s.server_id for s in sorted_scores[:min(2, len(sorted_scores))]]
                        
                        # Find the nodes corresponding to these server IDs
                        self.cached_targets = []
                        for sid in best_servers:
                            target = next((n for n in self.nodes if n["id"] == sid), None)
                            if target:
                                self.cached_targets.append(target)
                        
                    except Exception as e:
                        print(f"[{self.node_id}] Error collecting resource scores: {e}")
                        # Fall back to random selection
                        self.cached_targets = random.sample(self.nodes, 2)
                
                # Use cached targets
                targets = self.cached_targets if hasattr(self, 'cached_targets') else random.sample(self.nodes, 2)

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

    def HeartbeatAck(self, request, context):
        with self.lock:
            if self.state != "leader":
                return crash_pb2.HeartbeatAckResponse(received=False)
                
            # print(f"[{self.node_id}] Received heartbeat ack from {request.follower_id} for term {request.term}")
            
            # You could track which followers have acknowledged heartbeats
            # This is useful for monitoring cluster health
            
            return crash_pb2.HeartbeatAckResponse(received=True)


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
    print(f"[{node_id}] Listening on {listen}, state={servicer.state}, peers={peer_ids}")
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
