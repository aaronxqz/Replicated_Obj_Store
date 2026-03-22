"""
The server program for object storing
"""

import grpc, threading, argparse
from concurrent import futures
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc


class ObjectStoreSevice:
    def put(self, request, context):
        pass

def main():
    parser = argparse.ArgumentParser(description="--Listen and --Cluster flag implement")
    parser.add_argument("--Listen")
    parser.add_argument("--Cluster")
    args = parser._get_args
    
    server = grpc.server(futures.ThreadPoolExecutor())
    pb_grpc.add_ObjectStoreServicer_to_server(ObjectStoreServicer(), server)
    server.add_insecure_port(args.listen)
    server.start()
    server.wait_for_termination()