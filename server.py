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

MAX_KEY_LEN   = 128
MAX_VALUE_LEN = 1 * 1024 * 1024   # 1 MiB

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
            
        # 3. Set the primary node from the sorted list self.cluster and print summary info
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
        
        # 4. Generate stub dictionary for sending data from primary to replicas
        self.replica_stubs: dict[str, pb_grpc.ObjectStoreStub] = {}
        for addr in self.replicas:
            channel = grpc.insecure_channel(addr)
            self.replica_stubs[addr] = pb_grpc.ObjectStoreStub(channel)
        
        # 5. k-v pair for dictionary storage
        self.stores: dict[str, bytes] = {}
        
        # 6. Mutex lock
        self.lock = threading.Lock()
        
        # --- 7. Stats counters ---------------------------------------------
        # Only successful operations increment these.
        # Reset() zeroes them all.
        self.stat_puts    = 0
        self.stat_gets    = 0
        self.stat_deletes = 0
        self.stat_updates = 0
            
    def _reject_replica_service(self, context: grpc.ServicerContext) -> bool:
        if not self.is_primary:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(
                "This node is a replica. Send write requests to the primary: {self.primary}"
            )
            return True
        return False
    
    def _validate_key(self, key: str):
        """
        Return an error string if the key violates the spec rules, else None.
 
        Rules (from spec):
          • Printable ASCII only: codepoints 0x21 ('!') through 0x7E ('~').
            This excludes space (0x20), DEL (0x7F), and all control chars.
          • Max 128 characters.
          • Case-sensitive (no case-folding needed here; just pass through).
        """
        if not key:
            return "Key must not be empty"
        if len(key) > MAX_KEY_LEN:
            return f"Key exceeds {MAX_KEY_LEN} characters (got {len(key)})"
        for ch in key:
            if not (0x21 <= ord(ch) <= 0x7E):
                return (
                    f"Key contains invalid character {ch!r} "
                    f"(only printable ASCII 0x21-0x7E allowed)"
                )
        return None   # valid
 
    def _reject_if_replica(self, context: grpc.ServicerContext) -> bool:
        """
        If this node is NOT the primary, set FAILED_PRECONDITION on context
        and return True so the caller can immediately return an empty reply.
 
        Called at the top of every client-facing write handler.
        ApplyWrite must NOT call this — replicas must accept ApplyWrite.
        """
        if not self.is_primary:
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(
                f"This node is a replica. Send write requests to the "
                f"primary: {self.primary}"
            )
            return True
        return False
    
    # _fan_out sends WriteOp to all the replicas and collects the number of acknowldgement
    def _fan_out(self, write_op: pb.WriteOp):
        # for single node:
        if not self.replica_stubs:
            return 0
        # for replicas
        ack = 0
        with futures.ThreadPoolExecutor(max_workers=len(self.replica_stubs)) as executor:
            future_dict = {}
            for addr, stub in self.replica_stubs.items():
                future_dict.update({executor.submit(stub.ApplyWrite, write_op): addr})
            for future in futures.as_completed(future_dict):
                addr = future_dict[future]
                try:
                    future.result()
                    ack += 1
                    log.debug("Apply write is executed by replica %s", addr)
                except Exception as exc:
                    log.warning("Apply write to replica %s failed with %s", addr, exc)        
        return ack
    
    def _majority_commit(self, context: grpc.ServicerContext, write_op: pb.WriteOp) -> bool:
        majority_requirement = len(self.replicas) // 2 + 1
        replica_ack = self._fan_out(write_op) 
        total_ack = replica_ack + 1
        
        log.info(
            "Requirement majority acknowledgements from all node: %d, got replicas: %d, got total: %d", majority_requirement, replica_ack, total_ack,
        )
        
        if total_ack < majority_requirement:
            context.set_code(grpc.StatusCode.UNAVAILABLE)
            context.set_details(
                f"Total acknowledgement doesn't meet the requirement for majority: {majority_requirement}, "
                f"got {total_ack} acknowledgements" 
            )
            return False
        
        return True
    
        
    def Put(self, request: pb.PutRequest, context: grpc.ServicerContext):
        # Guard: replicas must not accept client writes
        if self._reject_if_replica(context):
            return empty_pb2.Empty()
 
        # Validate key format
        key_err = self._validate_key(request.key)
        if key_err:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(key_err)
            return empty_pb2.Empty()
 
        # Validate value size
        if len(request.value) > MAX_VALUE_LEN:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Value size {len(request.value)} bytes exceeds "
                f"maximum of {MAX_VALUE_LEN} bytes (1 MiB)"
            )
            return empty_pb2.Empty()
        
                # Replicate to other nodes and check majority
        write_op = pb.WriteOp(
            type  = pb.PUT,
            key   = request.key,
            value = request.value,
        )
        
        if not self._majority_commit(context, write_op):
            # The local apply stays; the client gets UNAVAILABLE.
            return empty_pb2.Empty()
 
        # Apply locally under the lock
        with self.lock:
            if request.key in self.stores:
                context.set_code(grpc.StatusCode.ALREADY_EXISTS)
                context.set_details(
                    f"Key '{request.key}' already exists. "
                    f"Use Update to change an existing value."
                )
                return empty_pb2.Empty()
 
            self.stores[request.key] = request.value
            self.stat_puts += 1
 
 
        return empty_pb2.Empty()
        
    def Get(self, request: pb.GetRequest, context: grpc.ServicerContext):
        with self.lock:
            if request.key not in self.stores:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(
                    f"Key: {request.key} was not found!"
                )
                return pb.GetResponse()
            self.stat_gets += 1
            return pb.GetResponse(value = self.stores[request.key])
            
    def Delete(self, request: pb.DeleteRequest, context: grpc.ServicerContext):
        if self._reject_if_replica(context):
            return empty_pb2.Empty()
 
        with self.lock:
            if request.key not in self.stores:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Key '{request.key}' does not exist")
                return empty_pb2.Empty()
 
            del self.stores[request.key]
            self.stat_deletes += 1
 
        write_op = pb.WriteOp(
            type = pb.DELETE,
            key  = request.key,
        )
        if not self._majority_commit(context, write_op):
            return empty_pb2.Empty()
 
        return empty_pb2.Empty()
    
    def Update(self, request: pb.UpdateRequest, context: grpc.ServicerContext):
        if self._reject_if_replica(context):
            return empty_pb2.Empty()
 
        if len(request.value) > MAX_VALUE_LEN:
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(
                f"Value size {len(request.value)} bytes exceeds "
                f"maximum of {MAX_VALUE_LEN} bytes (1 MiB)"
            )
            return empty_pb2.Empty()
 
        with self.lock:
            if request.key not in self.stores:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details(f"Key '{request.key}' does not exist")
                return empty_pb2.Empty()
 
            self.stores[request.key] = request.value
            self.stat_updates += 1
 
        write_op = pb.WriteOp(
            type  = pb.UPDATE,
            key   = request.key,
            value = request.value,
        )
        if not self._majority_commit(context, write_op):
            return empty_pb2.Empty()
 
        return empty_pb2.Empty()
    
    def List(self, request, context: grpc.ServicerContext):
        with self.lock:
            entries = [
                pb.ListEntry(key=k, size_bytes=len(v))
                for k, v in self.stores.items()
            ]
        return pb.ListResponse(entries=entries)
    
    def Reset(self, request, context: grpc.ServicerContext):
        if self._reject_if_replica(context):
            return empty_pb2.Empty()
 
        with self.lock:
            self.stores.clear()
            self.stat_puts    = 0
            self.stat_gets    = 0
            self.stat_deletes = 0
            self.stat_updates = 0
 
        write_op = pb.WriteOp(type=pb.RESET)
        if not self._majority_commit(context, write_op):
            return empty_pb2.Empty()
 
        return empty_pb2.Empty()
    
    def Stats(self, request, context: grpc.ServicerContext):
        with self.lock:
            live_objects = len(self.stores)
            total_bytes  = sum(len(v) for v in self.stores.values())
            return pb.StatsResponse(
                live_objects = live_objects,
                total_bytes  = total_bytes,
                puts         = self.stat_puts,
                gets         = self.stat_gets,
                deletes      = self.stat_deletes,
                updates      = self.stat_updates,
            )
            
    def ApplyWrite(self, request, context: grpc.ServicerContext):
        with self.lock:
            op_type = request.type
 
            if op_type == pb.PUT:
                # A replica applying PUT should just overwrite if somehow the
                # key already exists (possible after crash/restart edge cases).
                self.stores[request.key] = request.value
 
            elif op_type == pb.DELETE:
                # pop() with a default avoids KeyError if the replica somehow
                # missed a prior PUT (e.g. it was down and just restarted).
                self.stores.pop(request.key, None)
 
            elif op_type == pb.UPDATE:
                self.stores[request.key] = request.value
 
            elif op_type == pb.RESET:
                self.stores.clear()
                # Reset counters on the replica too so Stats is consistent.
                self.stat_puts    = 0
                self.stat_gets    = 0
                self.stat_deletes = 0
                self.stat_updates = 0
 
        return empty_pb2.Empty()

def main():
    parser = argparse.ArgumentParser(description="--Listen and --Cluster flag implement")
    parser.add_argument("--listen", required= True)
    parser.add_argument("--cluster", required= True)
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
        
    server.start()
    log.info("Server listening on %s  (press Ctrl+C to stop)", args.listen)  
      
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        log.info("Shutting down...")
        server.stop(grace=5)   # 5-second grace period for in-flight RPCs
 
if __name__ == "__main__":
    main()