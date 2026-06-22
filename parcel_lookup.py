import sys
import grpc
import property_pb2
import property_pb2_grpc

def main():
    if len(sys.argv) != 4:
        print(f"Usage: {sys.argv[0]} <host> <port> <parcel>")
        sys.exit(1)

    host = sys.argv[1]
    port = sys.argv[2]
    parcel = sys.argv[3]

    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = property_pb2_grpc.PropertyLookupStub(channel)

    response = stub.AddressByParcel(property_pb2.ParcelRequest(parcel=parcel))
    for addr in response.addresses:
        print(addr)

if __name__ == "__main__":
    main()
