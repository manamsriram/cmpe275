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

        # Add these variables for failure detection and backoff
        self.node_health = {n["id"]: True for n in nodes}  # Track node health
        self.heartbeat_failures = {}  # Maps node_id to failure count
        self.backoff_until = {}       # Maps node_id to timestamp when to retry
        self.initial_backoff = 1      # 1 second initial backoff
        self.max_backoff = 30         # Maximum 30 seconds backoff
        self.backoff_factor = 2       # Double the backoff each time
        self.failure_threshold = 3    # Number of failures before marking node as down

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

    def update_topology(self, nodes_changed=True):
        """Update routing tables when nodes change"""
        # Invalidate cached paths
        self.path_cache = {}
        
        # If nodes changed, start a new election
        if nodes_changed:
            print(f"[{self.node_id}] Network topology changed, starting new election")
            self.state = "follower"
            self.is_leader = False
            self.voted_for = None
            self.start_election()


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
            if self.state == "candidate":
                # Add randomized backoff before next election
                timeout = random.uniform(300, 600) / 1000
                threading.Timer(timeout, self.reset_election_timer).start()
            
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
            
            # Recompute all paths from this leader to other nodes
            self.path_cache = {}
            for node in self.nodes:
                if node["id"] != self.node_id:
                    path = find_path(self.adj, self.node_id, node["id"])
                    if path:
                        self.path_cache[(self.node_id, node["id"])] = path
            
            # Cancel election timer and start sending heartbeats
            if self.election_timer:
                self.election_timer.cancel()
            self.send_heartbeat()


    def GetLeader(self, request, context):
        with self.lock:
            is_leader = self.state == "leader"
            leader_id = self.leader_id if not is_leader else self.node_id
            
            # Create a list of known server endpoints from our adjacency list
            server_endpoints = []
            for node_id in self.adj.get(self.node_id, []):
                node = next((n for n in self.nodes if n["id"] == node_id), None)
                if node:
                    server_endpoints.append(crash_pb2.ServerEndpoint(
                        server_id=node["id"],
                        address=node["address"],
                        port=node["port"]
                    ))
            
            # Add ourselves to the endpoints
            my_node = next((n for n in self.nodes if n["id"] == self.node_id), None)
            if my_node:
                server_endpoints.append(crash_pb2.ServerEndpoint(
                    server_id=self.node_id,
                    address=my_node["address"],
                    port=my_node["port"]
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
            
        with self.lock:
            term = self.current_term
        
        current_time = time.time()
        
        for nbr_id in self.adj.get(self.node_id, []):
            # Skip nodes in backoff period
            if nbr_id in self.backoff_until and current_time < self.backoff_until[nbr_id]:
                continue
                
            try:
                request = crash_pb2.AppendEntriesRequest(
                    term=term,
                    leader_id=self.node_id
                )
                
                # Initialize seen_nodes with just the leader
                seen_nodes = self.node_id
                
                response = self.stubs[nbr_id].AppendEntries(
                    request, 
                    metadata=[("seen_nodes", seen_nodes), ("failed_nodes", ",".join([n for n in self.node_health if not self.node_health[n]]))]
                )
                
                # Successful heartbeat, reset failure count
                self.heartbeat_failures[nbr_id] = 0
                if nbr_id in self.backoff_until:
                    del self.backoff_until[nbr_id]
                
                # Mark node as healthy
                if not self.node_health.get(nbr_id, True):
                    print(f"[{self.node_id}] Node {nbr_id} is back online")
                    self.node_health[nbr_id] = True
                    self.propagate_node_status(nbr_id, True)
                
                with self.lock:
                    if response.term > self.current_term:
                        self.current_term = response.term
                        self.state = "follower"
                        self.is_leader = False
                        self.voted_for = None
                        self.reset_election_timer()
                        return
            except Exception as e:
                # Implement exponential backoff
                failure_count = self.heartbeat_failures.get(nbr_id, 0) + 1
                self.heartbeat_failures[nbr_id] = failure_count
                
                # Calculate backoff time with exponential increase
                backoff_seconds = min(
                    self.initial_backoff * (self.backoff_factor ** (failure_count - 1)),
                    self.max_backoff
                )
                
                # Add jitter to prevent synchronized retries
                jitter = random.uniform(0.8, 1.2)
                backoff_with_jitter = backoff_seconds * jitter
                
                # Set timestamp for next retry attempt
                self.backoff_until[nbr_id] = current_time + backoff_with_jitter
                
                print(f"[{self.node_id}] Error sending heartbeat to {nbr_id}: {e}")
                print(f"[{self.node_id}] Backing off for {backoff_with_jitter:.2f}s before retrying {nbr_id} (attempt {failure_count})")
                
                # Mark node as failed after threshold
                if failure_count >= self.failure_threshold and self.node_health.get(nbr_id, True):
                    print(f"[{self.node_id}] Marking node {nbr_id} as failed after {failure_count} attempts")
                    self.node_health[nbr_id] = False
                    self.propagate_node_status(nbr_id, False)
                    self.update_topology()
        
        # Schedule next heartbeat
        self.reset_heartbeat_timer()

    def propagate_node_status(self, failed_node_id, is_healthy):
        """Propagate node status changes to all other nodes"""
        for nbr_id in self.adj.get(self.node_id, []):
            if nbr_id != failed_node_id and self.node_health.get(nbr_id, False):
                try:
                    request = crash_pb2.NodeStatusUpdate(
                        node_id=failed_node_id,
                        is_healthy=is_healthy,
                        reporter_id=self.node_id
                    )
                    self.stubs[nbr_id].UpdateNodeStatus(request)
                except Exception as e:
                    print(f"[{self.node_id}] Error propagating status of {failed_node_id} to {nbr_id}: {e}")

    def UpdateNodeStatus(self, request, context):
        """Handle node status updates from other nodes"""
        node_id = request.node_id
        is_healthy = request.is_healthy
        reporter_id = request.reporter_id
        
        # Update our view of node health
        if self.node_health.get(node_id, True) != is_healthy:
            self.node_health[node_id] = is_healthy
            print(f"[{self.node_id}] Received status update: Node {node_id} is {'healthy' if is_healthy else 'failed'} (reported by {reporter_id})")
            
            # Update topology if node is down
            if not is_healthy:
                self.update_topology()
                
            # Propagate to other neighbors (except reporter)
            for nbr_id in self.adj.get(self.node_id, []):
                if nbr_id != reporter_id and nbr_id != node_id and self.node_health.get(nbr_id, False):
                    try:
                        self.stubs[nbr_id].UpdateNodeStatus(request)
                    except Exception:
                        pass
        
        return crash_pb2.Ack(success=True)

    def update_topology(self):
        """Update routing tables when nodes fail"""
        # Invalidate cached paths containing failed nodes
        invalid_paths = []
        for key, path in self.path_cache.items():
            if any(node in path and not self.node_health.get(node, True) for node in path):
                invalid_paths.append(key)
        
        for key in invalid_paths:
            del self.path_cache[key]
            
        print(f"[{self.node_id}] Updated topology after node failures, invalidated {len(invalid_paths)} cached paths")


    def AppendEntries(self, request, context):
        meta = dict(context.invocation_metadata())
        seen_nodes = meta.get("seen_nodes", "")
        seen_list = seen_nodes.split(",") if seen_nodes else []

         # Get failed nodes information
        failed_nodes = meta.get("failed_nodes", "")
        failed_list = failed_nodes.split(",") if failed_nodes else []

        # Update our view of node health based on leader's information
        for node_id in failed_list:
            if node_id and self.node_health.get(node_id, False):
                self.node_health[node_id] = False
                print(f"[{self.node_id}] Learned that node {node_id} is down from heartbeat metadata")
                self.update_topology()
        
        # Get original leader from metadata or use the request's leader_id
        original_leader = meta.get("original_leader", request.leader_id)
        
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
            
            # Forward heartbeat to neighbors who haven't seen it yet
            seen_list.append(self.node_id)
            new_seen = ",".join(seen_list)
            
            current_time = time.time()
            
            for nbr_id in self.adj.get(self.node_id, []):
                if nbr_id not in seen_list:
                    # Skip nodes in backoff period
                    if nbr_id in self.backoff_until and current_time < self.backoff_until[nbr_id]:
                        continue
                        
                    try:
                        # Include original_leader in metadata when forwarding
                        self.stubs[nbr_id].AppendEntries(
                            request,
                            metadata=[
                                ("seen_nodes", new_seen),
                                ("original_leader", original_leader)
                            ]
                        )
                        
                        # Successful forward, reset failure count
                        self.heartbeat_failures[nbr_id] = 0
                        if nbr_id in self.backoff_until:
                            del self.backoff_until[nbr_id]
                            
                    except Exception as e:
                        # Implement exponential backoff
                        failure_count = self.heartbeat_failures.get(nbr_id, 0) + 1
                        self.heartbeat_failures[nbr_id] = failure_count
                        
                        # Calculate backoff time with exponential increase
                        backoff_seconds = min(
                            self.initial_backoff * (self.backoff_factor ** (failure_count - 1)),
                            self.max_backoff
                        )
                        
                        # Add jitter to prevent synchronized retries (±20%)
                        jitter = random.uniform(0.8, 1.2)
                        backoff_with_jitter = backoff_seconds * jitter
                        
                        # Set timestamp for next retry attempt
                        self.backoff_until[nbr_id] = current_time + backoff_with_jitter
                        
                        print(f"[{self.node_id}] Error forwarding heartbeat to {nbr_id}: {e}")
                        print(f"[{self.node_id}] Backing off for {backoff_with_jitter:.2f}s before retrying {nbr_id}")
        
        # Send ack back to the original leader with backoff handling
        if original_leader != self.node_id and original_leader != request.leader_id:
            # Skip if in backoff period
            if original_leader in self.backoff_until and current_time < self.backoff_until[original_leader]:
                return crash_pb2.AppendEntriesResponse(term=self.current_term, success=True)
                
            try:
                # Use cached stub or create a new one safely
                if original_leader not in self.stubs:
                    leader_node = next((n for n in self.nodes if n["id"] == original_leader), None)
                    if leader_node:
                        addr = f"{leader_node['address']}:{leader_node['port']}"
                        try:
                            channel = grpc.insecure_channel(addr)
                            self.stubs[original_leader] = crash_pb2_grpc.CrashReplicatorStub(channel)
                        except Exception as e:
                            print(f"[{self.node_id}] Error creating stub for leader {original_leader}: {e}")
                            return crash_pb2.AppendEntriesResponse(term=self.current_term, success=True)
                
                if original_leader in self.stubs:
                    self.stubs[original_leader].HeartbeatAck(
                        crash_pb2.HeartbeatAckRequest(
                            term=self.current_term,
                            follower_id=self.node_id,
                            success=True
                        )
                    )
                    
                    # Successful ack, reset failure count
                    self.heartbeat_failures[original_leader] = 0
                    if original_leader in self.backoff_until:
                        del self.backoff_until[original_leader]
                        
            except Exception as e:
                # Implement exponential backoff
                failure_count = self.heartbeat_failures.get(original_leader, 0) + 1
                self.heartbeat_failures[original_leader] = failure_count
                
                # Calculate backoff time with exponential increase
                backoff_seconds = min(
                    self.initial_backoff * (self.backoff_factor ** (failure_count - 1)),
                    self.max_backoff
                )
                
                # Add jitter to prevent synchronized retries
                jitter = random.uniform(0.8, 1.2)
                backoff_with_jitter = backoff_seconds * jitter
                
                # Set timestamp for next retry attempt
                self.backoff_until[original_leader] = current_time + backoff_with_jitter
                
                print(f"[{self.node_id}] Error sending heartbeat ack to leader {original_leader}: {e}")
                print(f"[{self.node_id}] Backing off for {backoff_with_jitter:.2f}s before retrying {original_leader}")
        
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
        is_repop = meta.get("repopulate") == "1"
        processed = 0

        # Parse headers for follower‐side routing
        if not self.is_leader:
            vector_clock = int(meta.get("vector_clock", "0"))
            node_set = meta.get("node_set", "")
            paths_meta = meta.get("paths") or ""
            pos_meta = int(meta.get("pos", "1"))

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
                    self.local_records[rid] = record
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
                        ("repopulate", "1"),
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

        return crash_pb2.Ack(success=True, message=f"Processed {count} records")

    def HeartbeatAck(self, request, context):
        with self.lock:
            if self.state != "leader":
                return crash_pb2.HeartbeatAckResponse(received=False)
                
            # print(f"[{self.node_id}] Received heartbeat ack from {request.follower_id} for term {request.term}")
            
            # You could track which followers have acknowledged heartbeats
            # This is useful for monitoring cluster health
            
            return crash_pb2.HeartbeatAckResponse(received=True)

    def QueryRow(self, request, context):
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

        RF = 2  # Replication factor

        # 4) If no successes, return NOT_FOUND
        if not successes:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"row_id {rid} not found on any node")
            return crash_pb2.QueryResponse()
            
        # 5) Repair if fewer than RF successes
        if len(successes) < RF:
            print(f"[{self.node_id}] Detected under-replicated row {rid}. Found {len(successes)} replicas, expected {RF}")
            best_node, best_rec = successes[0]
            
            # Get the intended replica set
            orig = self.node_sets.get(rid, set())
            if not orig:
                # If we don't have a record of where this should be, create a new assignment
                orig = set(node["id"] for node in random.sample(self.nodes, RF))
                self.node_sets[rid] = orig
                
            # Identify which nodes have the data and which are missing it
            got_ids = {nid for nid, _ in successes}
            missing = orig - got_ids
            
            # Check if leader itself is missing the data
            if self.node_id in missing:
                print(f"[{self.node_id}] Leader is missing row {rid}, storing locally first")
                raw = best_rec.SerializeToString()
                self._store(raw)
                self.local_records[rid] = best_rec
                missing.remove(self.node_id)
                got_ids.add(self.node_id)
            
            # Now replicate to other missing nodes
            for missing_node in missing:
                try:
                    self._replicate_to(missing_node, best_rec)
                    got_ids.add(missing_node)
                except Exception as e:
                    # Handle failure and try alternative node
                    print(f"[{self.node_id}] Failed to update primary replica {best_node}: {e}")
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
            ("repopulate", "1"),
        ]

        def one():
            yield record

        ack = stub.SendCrashes(one(), metadata=meta)
        if not ack.success:
            raise RuntimeError(f"replication to {nid} failed: {ack.message}")
        else:
            print(f"[{self.node_id}] Replication to {nid} succeeded: {ack.message}")

    # Private function meant to handle when a node leaves or comes back
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
                self._just_recovered = True
                addr = f"{new_node['address']}:{new_node['port']}"
                channel = grpc.insecure_channel(addr)
                self.stubs[new_node["id"]] = crash_pb2_grpc.CrashReplicatorStub(channel)
                print(
                    f"[{self.node_id}] Created connection to new peer {new_node['id']}"
                )

            # leader should push all missing data to the rejoined node
            if self.is_leader:
                print(f"[{self.node_id}] Leader recovering data for {new_node['id']}")
                self.handle_node_rejoin(new_node["id"])

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

    def handle_node_rejoin(self, recovered_id):
        if not self.is_leader:
            return

        # Collect all (rid, record) pairs we need to push
        to_push = []

        for rid, nodeset in list(self.node_sets.items()):
            if recovered_id not in nodeset:
                continue

            # And only if we are currently under-replicated or the recovered node is missing
            # (we assume node_sets wasn't updated when it went down)
            # if recovered_id == self.node_id, local_records might be empty
            needs_repop = recovered_id != self.node_id or rid not in self.local_records
            if not needs_repop:
                continue

            # fetch from any other live replica
            source = next((n for n in nodeset if n != recovered_id), None)
            if source is None:
                # no-one else has it
                continue
            try:
                rec = (
                    self.stubs[source]
                    .QueryRow(crash_pb2.QueryRequest(row_id=rid))
                    .record
                )
            except grpc.RpcError:
                continue

            to_push.append((rid, rec))

        if not to_push:
            print(f"[{self.node_id}] Nothing to re-send to {recovered_id}")
            return

        if recovered_id == self.node_id:
            for rid, rec in to_push:
                raw = rec.SerializeToString()
                self._store(raw)
                self.local_records[rid] = rec
            print(f"[{self.node_id}] Restored {len(to_push)} rows into local store")
        else:
            # Stream them to the recovered follower
            stub = self.stubs.get(recovered_id)
            if stub is None:
                print(f"[{self.node_id}] No stub for {recovered_id}, cannot re-send")
                return

            def stream():
                for _, rec in to_push:
                    yield rec

            try:
                ack = stub.SendCrashes(stream())
                if ack.success:
                    print(
                        f"[{self.node_id}] Re-sent {len(to_push)} rows → {recovered_id}"
                    )
                else:
                    print(
                        f"[{self.node_id}] Repopulation to {recovered_id} failed: {ack.message}"
                    )
            except Exception as e:
                print(f"[{self.node_id}] Error streaming to {recovered_id}: {e}")

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
                        self.stubs[nbr_id].GetLeader(
                            crash_pb2.LeaderRequest(), timeout=1
                        )
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
            print(
                f"[{self.node_id}] Network partition detected! Reachable: {reachable}"
            )

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
                        print(
                            f"[{self.node_id}] Attempting to bridge partition by connecting {r_id} to {u_id}"
                        )

                        # Update adjacency lists
                        self.adj.setdefault(r_id, []).append(u_id)
                        self.adj.setdefault(u_id, []).append(r_id)

                        # If we're creating a connection for ourselves, establish the stub
                        if r_id == self.node_id:
                            # Create our stub to the unreachable node
                            addr = f"{u_node['address']}:{u_node['port']}"
                            channel = grpc.insecure_channel(addr)
                            self.stubs[u_id] = crash_pb2_grpc.CrashReplicatorStub(
                                channel
                            )

                            # Now tell the unreachable node to create a stub back to us
                            try:
                                # Create a topology update request
                                request = crash_pb2.TopologyUpdateRequest(
                                    update_type="add",
                                    node_id=self.node_id,
                                    address=next(
                                        (
                                            n["address"]
                                            for n in self.nodes
                                            if n["id"] == self.node_id
                                        ),
                                        "",
                                    ),
                                    port=next(
                                        (
                                            n["port"]
                                            for n in self.nodes
                                            if n["id"] == self.node_id
                                        ),
                                        0,
                                    ),
                                )

                                # Send the request to create a connection back to us
                                self.stubs[u_id].UpdateTopology(request)
                                print(
                                    f"[{self.node_id}] Notified {u_id} to create connection back to us"
                                )
                            except Exception as e:
                                print(
                                    f"[{self.node_id}] Failed to notify {u_id} to create connection: {e}"
                                )
                                continue
                        # Clear path cache to force recalculation of routes
                        self.path_cache = {}
                        return True

            return False
        return True

    def monitor_node_health(self):
        """
        Periodically ping every node in self.nodes (except ourselves),
        track which ones we think are offline, and fire
        handle_node_rejoin/recovery when they come back.
        """
        while True:
            time.sleep(5)
            # print(self.node_sets)
            for node in list(self.nodes):
                nid = node["id"]
                if nid == self.node_id:
                    continue

                try:
                    stub = self.stubs.get(nid)
                    # print(f"Checking if stub exists for {nid}. {stub}")
                    if stub is None:
                        addr = f"{node['address']}:{node['port']}"
                        stub = crash_pb2_grpc.CrashReplicatorStub(
                            grpc.insecure_channel(addr)
                        )
                        self.stubs[nid] = stub

                    # --- ping it ---
                    alive = True
                    try:
                        # print("Attempting to get stub for node")
                        stub.GetLeader(crash_pb2.LeaderRequest(), timeout=2)
                        # print("Got stub for node")
                    except grpc.RpcError:
                        alive = False

                    # print(f"Checking {nid} for liveness. Alive = {alive}. In offline = {nid in self.offline}")

                    # --- recovery: it was offline, now it's alive ---
                    if alive and nid in self.offline:
                        print(f"[{self.node_id}] RECOVERY detected for {nid}")
                        # re-add to adjacency so heartbeats resume
                        if nid not in self.adj.get(self.node_id, []):
                            self.adj.setdefault(self.node_id, []).append(nid)
                        # only the leader pumps the missing data
                        if self.is_leader:
                            self.handle_node_rejoin(nid)
                        self.offline.remove(nid)
                        continue

                    # --- failure: it was online, now it's dead ---
                    if not alive and nid not in self.offline:
                        print(f"[{self.node_id}] FAILURE detected for {nid}")
                        # prune it out of our adjacency
                        if nid in self.adj.get(self.node_id, []):
                            self.adj[self.node_id].remove(nid)
                        self.offline.add(nid)
                        continue

                    # --- otherwise, nothing to do for this node ---
                except Exception as e:
                    # catch *any* bug in our health code so the thread never dies
                    print(f"[{self.node_id}] monitor_node_health error for {nid}: {e}")
                    continue

                # Only maintain heartbeats to direct peers
                if nid not in self.adj.get(self.node_id, []):
                    continue

                # Fire a heartbeat just to keep the channel warm
                try:
                    stub.GetLeader(crash_pb2.LeaderRequest(), timeout=2)
                except grpc.RpcError:
                    pass

            # Try to stitch up any network partitions
            self.detect_and_repair_network_partition()

    def RegisterNode(self, request, context):
        """
        Allow new nodes to register with the network.
        """
        new_node = {
            "id": request.node_id,
            "address": request.address,
            "port": request.port,
        }

        # Update our topology with the new node
        self.update_topology(new_node=new_node)

        # If we're the leader, broadcast the updated topology
        if self.is_leader:
            self.broadcast_topology_update(new_node=new_node)

        return crash_pb2.RegisterNodeResponse(
            success=True,
            current_leader=self.leader_id or "",
            nodes=[n["id"] for n in self.nodes],
        )

    def GetAllNodes(self, request, context):
        """
        Return information about all nodes in the network.
        Used by clients when leader changes.
        """
        node_infos = []
        for node in self.nodes:
            node_infos.append(
                crash_pb2.NodeInfo(
                    node_id=node["id"], address=node["address"], port=node["port"]
                )
            )

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

            # If we're the leader, recalculate paths
            if self.is_leader:
                print(
                    f"[{self.node_id}] Leader recalculating paths after topology change"
                )
                if hasattr(self, "cached_targets"):
                    del self.cached_targets
            # If we're not the leader, forward the request to the leader
            elif self.leader_id and self.leader_id != self.node_id:
                try:
                    # Get leader stub if needed
                    if self.leader_id not in self.stubs:
                        leader_node = next(
                            (n for n in self.nodes if n["id"] == self.leader_id), None
                        )
                        if leader_node:
                            leader_addr = (
                                f"{leader_node['address']}:{leader_node['port']}"
                            )
                            leader_channel = grpc.insecure_channel(leader_addr)
                            self.stubs[self.leader_id] = (
                                crash_pb2_grpc.CrashReplicatorStub(leader_channel)
                            )

                    # Forward the same request to the leader
                    print(
                        f"[{self.node_id}] Forwarding topology update to leader {self.leader_id}"
                    )
                    self.stubs[self.leader_id].UpdateTopology(request)
                except Exception as e:
                    print(
                        f"[{self.node_id}] Failed to forward topology update to leader: {e}"
                    )
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
                        port=new_node["port"],
                    )
                else:
                    request = crash_pb2.TopologyUpdateRequest(
                        update_type="remove", node_id=removed_node
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
