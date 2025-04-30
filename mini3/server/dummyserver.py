from concurrent import futures
import grpc
from proto import crash_pb2
from proto import crash_pb2_grpc


class CrashReplicatorServicer(crash_pb2_grpc.CrashReplicatorServicer):
    def SendCrashes(self, request_iterator, context):
        count = 0
        for record in request_iterator:
            count += 1
            print(
                f"Received record {count}: collision_id={record.collision_id}, date={record.crash_date}, time={record.crash_time}"
            )
        return crash_pb2.Ack(success=True, message=f"Received {count} records")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    crash_pb2_grpc.add_CrashReplicatorServicer_to_server(
        CrashReplicatorServicer(), server
    )
    port = "[::]:50051"
    server.add_insecure_port(port)
    server.start()
    print(f"gRPC server is running on {port}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("Shutting down server...")


if __name__ == "__main__":
    serve()
