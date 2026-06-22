import flask
from flask import Flask
import grpc
import property_pb2
import property_pb2_grpc
import os

app = Flask("p2")

project = os.environ.get("PROJECT", "p2")
channel1 = grpc.insecure_channel(f"{project}-java-dataset-1:5000")
stub1 = property_pb2_grpc.PropertyLookupStub(channel1)

channel2 = grpc.insecure_channel(f"{project}-java-dataset-2:5000")
stub2 = property_pb2_grpc.PropertyLookupStub(channel2)

last_source = "2"
@app.route("/parcelnum/<parcel>")
def parcel_lookup(parcel):
    global last_source

    addrs = []
    error = ""

    if last_source == "1":
        primary_stub = stub2
        primary_source = "2"
        backup_stub = stub1
        backup_source = "1"
    else:
        primary_stub = stub1
        primary_source = "1"
        backup_stub = stub2
        backup_source = "2"

    try:
        source = primary_source
        response = primary_stub.AddressByParcel(
            property_pb2.ParcelRequest(parcel=parcel), timeout=1
        )
        last_source = source
    except grpc.RpcError:
        try:
            source = backup_source
            response = backup_stub.AddressByParcel(
                property_pb2.ParcelRequest(parcel=parcel), timeout=1
            )
            last_source = source
        except grpc.RpcError:
            return flask.jsonify({"source": backup_source, "addrs": addrs, "error": "grpc error"})
        except Exception as e:
            return flask.jsonify({"source": backup_source, "addrs": addrs, "error": str(e)})
    except Exception as e:
        return flask.jsonify({"source": primary_source, "addrs": addrs, "error": str(e)})

    addrs = list(response.addresses)
    if response.failed:
        error = "unknown backend error"

    return flask.jsonify({"source": source, "addrs": addrs, "error": error})
def main():
    app.run("0.0.0.0", port=5000, debug=False, threaded=False)

if __name__ == "__main__":
    main()
