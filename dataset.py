import grpc
import gzip
import csv
from collections import defaultdict
import os
import concurrent.futures # Import for ThreadPoolExecutor

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
        Uses csv.DictReader and assumes column names "Parcel" and "Address".
        """
        try:
            with gzip.open(csv_path, 'rt', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                # Use specified column names
                PARCEL_COLUMN_NAME = "Parcel"
                ADDRESS_COLUMN_NAME = "Address"

                for row in reader:
                    # Check if the required columns exist in the row
                    if PARCEL_COLUMN_NAME in row and ADDRESS_COLUMN_NAME in row:
                        parcel = row[PARCEL_COLUMN_NAME]
                        address = row[ADDRESS_COLUMN_NAME]
                        self.parcel_index[parcel].append(address)
                    else:
                        # Optionally log skipped malformed lines or rows missing columns
                        # print(f"Skipped row due to missing '{PARCEL_COLUMN_NAME}' or '{ADDRESS_COLUMN_NAME}' column: {row}")
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
    # Matching Java thread pool size by using max_workers=1
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=1))
    property_pb2_grpc.add_PropertyLookupServicer_to_server(DatasetServer(csv_path), server)
    # Listen on 0.0.0.0:5000 as required
    server.add_insecure_port('0.0.0.0:5000')
    print("Server started on 0.0.0.0:5000")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    # Read addresses.csv.gz from the current working directory by default,
    # or use path from ADDRESSES_CSV environment variable.
    CSV_FILE_PATH = os.getenv('ADDRESSES_CSV', 'addresses.csv.gz')
    serve(CSV_FILE_PATH)
