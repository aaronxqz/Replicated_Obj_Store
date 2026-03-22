
"""import argparse

def main():
    parser = argparse.ArgumentParser(description="This is a description")   #define rules 
    parser.add_argument("file", help="Input file")
    parser.add_argument("-v", "--verbose", action="store_true", help="enable verbose")
    
    args = parser.parse_args()                                              #Read from sys.argv and apply the rule
    
    print("test msg")
    print(f"File:{args.file}, Verbose={args.verbose}")
    
main()"""

try:
    import grpc
    print("gRPC library (grpcio package) is installed.")
    # You can also check the version if needed
    # from grpc import _grpcio_metadata
    # print(f"Installed version: {_grpcio_metadata.__version__}")
except ImportError:
    print("gRPC library is not installed.")
    print("You can install it using: pip install grpcio")

