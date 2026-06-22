import flask
from flask import Flask
import grpc
import property_pb2
import property_pb2_grpc
import os

app = Flask("p2")

project = os.environ.get("PROJECT", "p2")
channel = grpc.insecure_channel(f"{project}-java-dataset-1:5000")
stub = property_pb2_grpc.PropertyLookupStub(channel)

@app.route("/parcelnum/<parcel>")
def parcel_lookup(parcel):
    source = "1"
    addrs = []
    error = ""

    response = stub.AddressByParcel(property_pb2.ParcelRequest(parcel=parcel), timeout=1)
    addrs = list(response.addresses)
    if response.failed:
        error = "unknown backend error"

    return flask.jsonify({"source": source, "addrs": addrs, "error": error})

def main():
    app.run("0.0.0.0", port=5000, debug=False, threaded=False)

if __name__ == "__main__":
    main()
