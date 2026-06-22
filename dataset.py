import concurrent.futures
import csv
import gzip
import os
from collections import defaultdict

import grpc
import property_pb2
import property_pb2_grpc


class DatasetServer(property_pb2_grpc.PropertyLookupServicer):
    def __init__(self, csv_path):
        self.parcel_index = defaultdict(list)
        self.zip_index = defaultdict(list)
        self._load_data(csv_path)
        print(f"Loaded {len(self.parcel_index)} parcels")

    def _load_data(self, csv_path):
        with gzip.open(csv_path, "rt", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                parcel = row["Parcel"]
                zipcode = row["ZipCode"]
                address = row["Address"]

                self.parcel_index[parcel].append(address)
                self.zip_index[zipcode].append(address)

        for addrs in self.parcel_index.values():
            addrs.sort()

        for addrs in self.zip_index.values():
            addrs.sort()

    def AddressByParcel(self, request, context):
        parcel = request.parcel
        addrs = self.parcel_index.get(parcel)

        response = property_pb2.AddressResponse()
        if addrs is None:
            response.failed = True
        else:
            response.addresses.extend(addrs)

        return response

    def AddressByZip(self, request, context):
        zipcode = request.zip
        addrs = self.zip_index.get(zipcode)

        response = property_pb2.AddressResponse()
        if addrs is None:
            response.failed = True
        else:
            response.addresses.extend(addrs)

        return response


def serve(csv_path):
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=1))
    property_pb2_grpc.add_PropertyLookupServicer_to_server(
        DatasetServer(csv_path), server
    )
    server.add_insecure_port("0.0.0.0:5000")
    print("Server started on 0.0.0.0:5000")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    csv_path = os.getenv("ADDRESSES_CSV", "addresses.csv.gz")
    serve(csv_path)
