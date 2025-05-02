import argparse
import json
import random
from concurrent import futures
import grpc
from proto import crash_pb2
from proto import crash_pb2_grpc


class CrashReplicatorServicer(crash_pb2_grpc.CrashReplicatorServicer):
    def __init__(self, node_id, peers, is_leader):
        self.node_id = node_id
        self.peers = peers
        self.is_leader = is_leader
        self.peer_stubs = {}
        self.last_sent = {}
        self.seen = {}
        for p in peers:
            peer_id = p["id"]
            addr = f"{p['address']}:{p['port']}"
            channel = grpc.insecure_channel(addr)
            stub = crash_pb2_grpc.CrashReplicatorStub(channel)
            self.peer_stubs[peer_id] = stub
            self.last_sent[peer_id] = 0

    def SendCrashes(self, request_iterator, context):
        # Check for sender from gRPC metadata to not send anything back to the source
        md = dict(context.invocation_metadata())
        sender = md.get("sender")
        count = 0
        for record in request_iterator:
            count += 1
            rid = record.row_id
            self.seen.setdefault(rid, set())
            if sender:
                self.seen[rid].add(sender)
            self.seen[rid].add(self.node_id)
            print(
                f"[{self.node_id}] Received {rid} from {sender or 'client'}: "
                f"collision_id={record.collision_id}, date={record.crash_date}, time={record.crash_time}"
            )
            unseen = [pid for pid in self.peer_stubs if pid not in self.seen[rid]]
            if unseen:
                dest = random.choice(unseen)
                try:
                    self.seen[rid].add(dest)

                    def _one():
                        yield record

                    self.peer_stubs[dest].SendCrashes(
                        _one(), metadata=(("sender", self.node_id),)
                    )
                    print(f"[{self.node_id}] Forwarded {rid} to {dest}")
                except Exception as e:
                    print(f"[{self.node_id}] Error forwarding to {dest}: {e}")
        return crash_pb2.Ack(success=True, message=f"Processed {count}")


def serve(config):
    node_id = config["node_id"]
    address = config["address"]
    port = config["port"]
    peers = config.get("peers", [])
    # Default Leader is A for now
    # Insert leader election here
    is_leader = node_id == "A"

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=3))
    servicer = CrashReplicatorServicer(node_id, peers, is_leader)
    crash_pb2_grpc.add_CrashReplicatorServicer_to_server(servicer, server)

    listen_addr = f"{address}:{port}"
    server.add_insecure_port(listen_addr)
    server.start()

    print(
        f"[{node_id}] gRPC server running on {listen_addr}, leader={is_leader}, peers={[p['id'] for p in peers]}"
    )
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down server...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Start CrashReplicator node with topology config"
    )
    parser.add_argument("config", help="Path to node config JSON")
    args = parser.parse_args()

    with open(args.config, "r", encoding="utf-8") as f:
        config = json.load(f)

    serve(config)
