"""
The server program for object storing
"""

import sys
import argparse
import threading
import logging
from concurrent import futures
import re

import grpc
import objectstore_pb2 as pb
import objectstore_pb2_grpc as pb_grpc
from google.protobuf import empty_pb2

# -------------------------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------------------------

import validate_host_port as vhp

# Error Log
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------------------
# End of Helper Functions
# --------------------------------------------------------------------------------------


class ObjectStoreServicer:
    '''
    ObjectStoreServicer class contains:
    1. listen_addr : str
        The address server needs to bind to (--listen)
    2. cluster_list : str
        A string contains a list of ports that in the cluster seperates by comma
    '''
    
    def __init__(self, listen_addr : str, cluster_list : str):
        # 1. Normalize the arguments from CLI
        self.listen = listen_addr.lower().strip()
        
        self.cluster = sorted(
            ports.lower().strip(',') for ports in cluster_list.split(',')
        )
        
        # 2. Validates the form "localhost:port"
        
        is_valid, message = vhp.validate_host_port(self.listen)
        if not is_valid : 
            log.error(message, )
            sys.exit(1)
        
        for ip in self.cluster:
            is_valid, message = vhp.validate_host_port(ip)
            if not is_valid : 
                log.error(message, )
                sys.exit(1)
        
        if self.listen not in self.cluster:
            log.error("--listen address '%s' is not in --cluster list %s", self.listen, self.cluster, )
            sys.exit(1)
            
        # Set the primary node from the sorted list self.cluster
        self.primary = self.cluster[0]
        self.is_primary = (self.listen == self.primary)
        self.replicas = [i for i in self.cluster if i != self.listen]
        log.info(
            "This is a node in the cluster! Now listening: %s, Role: %s, Primary: %s, Replicas list: %s",
            self.listen,
            "PRIMARY" if self.is_primary else "REPLICA",
            self.primary,
            self.replicas,
        )
            
    
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
    parser.add_argument("--listen", required= True)
    parser.add_argument("--cluster", required= True)
    print("check point2")
    args = parser.parse_args()
    
    servicer = ObjectStoreServicer(
        listen_addr = args.listen,
        cluster_list = args.cluster,
    )
    
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=None), options=[
            # Allow the OS to reuse the port quickly after restart
            ("grpc.so_reuseport", 1),
        ],)
    pb_grpc.add_ObjectStoreServicer_to_server(servicer, server)
    bound_port = server.add_insecure_port(args.listen)
    if bound_port == 0 : 
        log.error("Failed to bind to %s — port may be in use.", args.listen)
        sys.exit(1)
        
    print("check point3")
    
    server.start()
    log.info("Server listening on %s  (press Ctrl+C to stop)", args.listen)  
      
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        server.stop(grace=5)   # 5-second grace period for in-flight RPCs
 
if __name__ == "__main__":
    main()