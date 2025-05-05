#!/usr/bin/env python3
import os
import sys
import argparse
import csv
import logging
import grpc
import time

from proto import crash_pb2, crash_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("csv_parser")


def parse_int(value, field_name, default=0):
    if value == "" or value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def parse_float(value, field_name, default=0.0):
    if value == "" or value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def load_crash_records(csv_path):
    """Parse CSV and yields CrashRecord messages"""
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            try:
                safe = lambda k: (row.get(k) or "").strip()
                crash_date = safe("CRASH DATE")
                crash_time = safe("CRASH TIME")
                borough = safe("BOROUGH")
                zip_code_s = safe("ZIP CODE")
                collision_id_s = safe("COLLISION_ID")

                if not crash_date or not crash_time:
                    continue
                if not zip_code_s:
                    zip_code_s = "-1"
                if not borough:
                    borough = "UNDEFINED"

                collision_id = int(collision_id_s) if collision_id_s else -1

                record = crash_pb2.CrashRecord(
                    row_id=idx,
                    crash_date=crash_date,
                    crash_time=crash_time,
                    borough=borough,
                    zip_code=parse_int(zip_code_s, "ZIP CODE"),
                    latitude=parse_float(safe("LATITUDE"), "LATITUDE"),
                    longitude=parse_float(safe("LONGITUDE"), "LONGITUDE"),
                    location=safe("LOCATION"),
                    on_street_name=safe("ON STREET NAME"),
                    cross_street_name=safe("CROSS STREET NAME"),
                    off_street_name=safe("OFF STREET NAME"),
                    num_persons_injured=parse_int(
                        safe("NUMBER OF PERSONS INJURED"), "NUMBER OF PERSONS INJURED"
                    ),
                    num_persons_killed=parse_int(
                        safe("NUMBER OF PERSONS KILLED"), "NUMBER OF PERSONS KILLED"
                    ),
                    num_pedestrians_injured=parse_int(
                        safe("NUMBER OF PEDESTRIANS INJURED"),
                        "NUMBER OF PEDESTRIANS INJURED",
                    ),
                    num_pedestrians_killed=parse_int(
                        safe("NUMBER OF PEDESTRIANS KILLED"),
                        "NUMBER OF PEDESTRIANS KILLED",
                    ),
                    num_cyclist_injured=parse_int(
                        safe("NUMBER OF CYCLIST INJURED"), "NUMBER OF CYCLIST INJURED"
                    ),
                    num_cyclist_killed=parse_int(
                        safe("NUMBER OF CYCLIST KILLED"), "NUMBER OF CYCLIST KILLED"
                    ),
                    num_motorist_injured=parse_int(
                        safe("NUMBER OF MOTORIST INJURED"), "NUMBER OF MOTORIST INJURED"
                    ),
                    num_motorist_killed=parse_int(
                        safe("NUMBER OF MOTORIST KILLED"), "NUMBER OF MOTORIST KILLED"
                    ),
                    collision_id=collision_id,
                )

                for i in range(1, 6):
                    cf = safe(f"CONTRIBUTING FACTOR VEHICLE {i}")
                    if cf:
                        record.contributing_factors.append(cf)
                for i in range(1, 6):
                    vt = safe(f"VEHICLE TYPE CODE {i}")
                    if vt:
                        record.vehicle_types.append(vt)

                yield record

            except Exception:
                logger.exception(f"Error processing row {idx}, skipping.")


def get_leader_address(initial_server="localhost:50056", known_servers=None):
    """Connect to any available server and get the leader address"""
    if known_servers is None:
        known_servers = [initial_server]
    elif initial_server not in known_servers:
        known_servers.append(initial_server)
    
    # Try each server until we find one that responds
    all_discovered_servers = set()
    leader_addr = None
    
    for server in known_servers:
        try:
            channel = grpc.insecure_channel(server)
            stub = crash_pb2_grpc.CrashReplicatorStub(channel)
            
            # Call the GetLeader RPC
            response = stub.GetLeader(crash_pb2.LeaderRequest())
            
            # Add this server and any discovered servers to our set
            all_discovered_servers.add(server)
            for endpoint in response.server_endpoints:
                server_addr = f"{endpoint.address}:{endpoint.port}"
                all_discovered_servers.add(server_addr)
            
            if response.is_leader:
                logger.info(f"Server {server} is the leader")
                leader_addr = server
                break
            elif response.leader_address:
                leader_addr = f"{response.leader_address}:{response.leader_port}"
                logger.info(f"Redirecting to leader at {leader_addr}")
                break
        except Exception as e:
            logger.debug(f"Error connecting to server {server}: {e}")
            continue
    
    if not leader_addr:
        logger.warning(f"No leader found, using initial server {initial_server}")
        leader_addr = initial_server
    
    return leader_addr, list(all_discovered_servers)


def run(csv_file, initial_server="localhost:50056"):
    MAX_ROWS = 2_000_000

    # Count rows (minus header and final newline), then cap to MAX_ROWS
    with open(csv_file, newline="", encoding="utf-8") as f:
        raw_count = sum(1 for _ in f) - 2
    total_rows = min(raw_count, MAX_ROWS)

    # Get the leader address and known servers
    leader_address, known_servers = get_leader_address(initial_server)
    
    sent = 0
    max_retries = 10
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # Connect to the leader
            leader_address, known_servers = get_leader_address(initial_server, known_servers=known_servers)
            channel = grpc.insecure_channel(leader_address)
            stub = crash_pb2_grpc.CrashReplicatorStub(channel)
            
            logger.info(f"Streaming up to {total_rows} crash records from {csv_file} to {leader_address}...")
            
            def generator():
                nonlocal sent
                for rec in load_crash_records(csv_file):
                    if sent >= total_rows:
                        break
                    sent += 1
                    if sent % 1000 == 0:
                        logger.info(f"Sent {sent} records so far...")
                    yield rec
            
            ack = stub.SendCrashes(generator())
            
            skipped = total_rows - sent
            logger.info(f"Total rows intended: {total_rows}, Sent: {sent}, Skipped: {skipped}")

            if ack.success:
                logger.info(f"Server received: {ack.message}")
                
                # # Only query if we actually sent something
                if sent > 0:
                    # Instead of random, iterate row_id 1000–2000 (within what we sent)
                    start_id = 550_000
                    end_id = min(sent, 600_000)
                    if end_id >= start_id:
                        for query_id in range(start_id, end_id + 1):
                            logger.info(f"Querying row_id={query_id}…")
                            try:
                                resp = stub.QueryRow(crash_pb2.QueryRequest(row_id=query_id))
                                rec = resp.record
                                print(f"Got row {rec.row_id}: {rec.location} @ {rec.crash_date} {rec.crash_time}")
                            except grpc.RpcError as e:
                                if e.code() == grpc.StatusCode.NOT_FOUND:
                                    print(f"Row {query_id} not found")
                                else:
                                    raise
                    else:
                        logger.warning(
                            f"Only {sent} records sent, which is less than start_id {start_id}; skipping range query."
                        )
                
                break  # Success, exit the retry loop
            else:
                logger.error(f"Server reported failure: {ack.message}")
                # Check if the error message contains leader information
                if "Try " in ack.message:
                    new_leader = ack.message.split("Try ")[1].strip()
                    logger.info(f"Redirecting to new leader at {new_leader}")
                    leader_address = new_leader
                    retry_count -= 1  # Don't count this as a retry
                else:
                    retry_count += 1
                
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                logger.error(f"Leader unavailable. Attempting to find new leader.")
                # Try to find a new leader from known servers
                found_new_leader = False
                for server in known_servers:
                    if server != leader_address:  # Don't retry the failed server
                        try:
                            new_leader, new_servers = get_leader_address(server)
                            leader_address = new_leader
                            known_servers = list(set(known_servers + new_servers))
                            logger.info(f"Found new leader at {leader_address}")
                            found_new_leader = True
                            break
                        except:
                            continue
                
                if not found_new_leader:
                    logger.error("Could not find a new leader from known servers.")
                
                retry_count += 1
                backoff_time = 2 ** retry_count  # Exponential backoff
                logger.info(f"Retrying in {backoff_time} seconds...")
                time.sleep(backoff_time)
            else:
                logger.error(f"RPC error: {e}")
                retry_count += 1
        except Exception as e:
            logger.error(f"Error streaming data: {e}")
            retry_count += 1
            
    if retry_count >= max_retries:
        logger.error("Max retries exceeded. Failed to stream data.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream crash CSV to gRPC server")
    parser.add_argument("csv_file", help="Path to the crash CSV file")
    parser.add_argument(
        "--server",
        default="localhost:50056",
        help="Initial server address (default: localhost:50056)",
    )
    args = parser.parse_args()

    if not args.csv_file or not os.path.isfile(args.csv_file):
        parser.error(f"CSV file not found: {args.csv_file!r}")

    try:
        start_time = time.time()
        run(args.csv_file, args.server)
        end_time = time.time()
        print(f"Data streaming completed in {end_time - start_time:.2f} seconds")
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting.")
        sys.exit(0)
