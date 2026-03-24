"""
The server program for object storing
"""

import grpc, threading, argparse
from concurrent import futures
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc


class ObjectStoreService:
    def Put(self, request, context):
        pass
    def Get(self, request, context):
        pass
    def Delete(self, request, context):
        pass
    def Update(self, request, context):
        pass
    def List(self, request, context):
        pass
    def Reset(self, request, context):
        pass
    def Stats(self, request, context):
        pass
    def ApplyWrite(self, request, context):
        pass

def main():
    print("check point1")
    parser = argparse.ArgumentParser(description="--Listen and --Cluster flag implement")
    parser.add_argument("--listen")
    parser.add_argument("--cluster")
    print("check point2")
    args = parser.parse_args()
    
    server = grpc.server(futures.ThreadPoolExecutor())
    pb_grpc.add_ObjectStoreServicer_to_server(ObjectStoreService(), server)
    print("check point3")
    server.add_insecure_port(args.listen)
    server.start()
    server.wait_for_termination()

main()