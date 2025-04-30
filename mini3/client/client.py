#!/usr/bin/env python3
import os
import sys
import argparse
import csv
import logging
import grpc

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
                # Bad but can't see another way of doing this
                safe = lambda k: (row.get(k) or "").strip()
                crash_date = safe("CRASH DATE")
                crash_time = safe("CRASH TIME")
                borough = safe("BOROUGH")
                zip_code_s = safe("ZIP CODE")
                collision_id_s = safe("COLLISION_ID")

                # Should never enter because every row in the CSV has these columns
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


def run(csv_file):
    # -2 because of new line at the end of the CSV and the header line
    with open(csv_file, newline="", encoding="utf-8") as f:
        total_rows = sum(1 for _ in f) - 2

    sent = 0
    def generator():
        nonlocal sent
        for rec in load_crash_records(csv_file):
            sent += 1
            yield rec

    channel = grpc.insecure_channel("localhost:50051")
    stub = crash_pb2_grpc.CrashReplicatorStub(channel)

    logger.info(f"Streaming crash records from {csv_file}...")
    ack = stub.SendCrashes(generator())

    skipped = total_rows - sent
    logger.info(f"Total rows: {total_rows}, Sent: {sent}, Skipped: {skipped}")

    if ack.success:
        logger.info(f"Server received: {ack.message}")
    else:
        logger.error(f"Server reported failure: {ack.message}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stream crash CSV to gRPC server")
    parser.add_argument("csv_file", help="Path to the crash CSV file")
    args = parser.parse_args()

    if not args.csv_file or not os.path.isfile(args.csv_file):
        parser.error(f"CSV file not found: {args.csv_file!r}")

    try:
        run(args.csv_file)
    except KeyboardInterrupt:
        logger.info("Interrupted by user, exiting.")
        sys.exit(0)
