import grpc
import gzip
import csv
from collections import defaultdict
import os

# Assuming property_pb2 and property_pb2_grpc are generated from property.proto
# You would typically generate these using:
# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. property.proto
import property_pb2
import property_pb2_grpc

class DatasetServer(property_pb2_grpc.PropertyLookupServicer):
    """
    Implements the PropertyLookup gRPC service, loading data from a CSV file.
    """
    def __init__(self, csv_path):
        self.parcel_index = defaultdict(list)
        self._load_data(csv_path)
        print(f"Loaded {len(self.parcel_index)} parcels")

    def _load_data(self, csv_path):
        """
        Loads parcel and address data from a gzipped CSV file.
        Assumes 'ParcelNumber' is at index 3 and 'MailingAddress' is at index 9.
        """
        try:
            with gzip.open(csv_path, 'rt', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader) # Skip header
                
                # We need Parcel (index 3) and Address (index 9)
                # Note: These indices are hardcoded, similar to the Java implementation.
                PARCEL_INDEX = 3
                ADDRESS_INDEX = 9

                for i, row in enumerate(reader):
                    if len(row) > max(PARCEL_INDEX, ADDRESS_INDEX):
                        parcel = row[PARCEL_INDEX]
                        address = row[ADDRESS_INDEX]
                        self.parcel_index[parcel].append(address)
                    else:
                        # Optionally log skipped malformed lines
                        # print(f"Skipped malformed line {i+2}: {row}")
                        pass
            
            # Sort each address list, similar to the Java implementation
            for addrs in self.parcel_index.values():
                addrs.sort()

        except FileNotFoundError:
            print(f"Error: CSV file not found at {csv_path}")
            raise
        except Exception as e:
            print(f"Error loading data from {csv_path}: {e}")
            raise

    def AddressByParcel(self, request, context):
        """
        gRPC method to look up addresses by parcel number.
        """
        parcel = request.parcel
        addrs = self.parcel_index.get(parcel)
        
        response_builder = property_pb2.AddressResponse()
        if addrs is None:
            response_builder.failed = True
        else:
            response_builder.addresses.extend(addrs)
            
        return response_builder

def serve(csv_path):
    server = grpc.server(java.util.concurrent.Executors.newFixedThreadPool(1)) # Matching Java thread pool size
    property_pb2_grpc.add_PropertyLookupServicer_to_server(DatasetServer(csv_path), server)
    server.add_insecure_port('[::]:5000')
    print("Server started on port 5000")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    # Default CSV path, matching the Java implementation
    CSV_FILE_PATH = "/data/addresses.csv.gz" 
    serve(CSV_FILE_PATH)
