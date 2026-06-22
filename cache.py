import flask
from flask import Flask
import grpc
import property_pb2
import property_pb2_grpc
import os
from collections import OrderedDict

app = Flask("p2")

project = os.environ.get("PROJECT", "p2")
dataset_implementation = os.environ.get("DATASET_IMPLEMENTATION", "JAVA")

if dataset_implementation == "PYTHON":
    dataset_service = "python-dataset"
else:
    dataset_service = "java-dataset"

channel1 = grpc.insecure_channel(f"{project}-{dataset_service}-1:5000")
stub1 = property_pb2_grpc.PropertyLookupStub(channel1)

channel2 = grpc.insecure_channel(f"{project}-{dataset_service}-2:5000")
stub2 = property_pb2_grpc.PropertyLookupStub(channel2)
last_source = "2"

cache = OrderedDict()
CACHE_SIZE = 6


@app.route("/parcelnum/<parcel>")
def parcel_lookup(parcel):
    global last_source
    global cache

    if parcel in cache:
        addrs = cache.pop(parcel)
        cache[parcel] = addrs
        return flask.jsonify({"source": "cache", "addrs": addrs, "error": ""})

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
            return flask.jsonify({
                "source": backup_source,
                "addrs": addrs,
                "error": "grpc error"
            })

        except Exception as e:
            return flask.jsonify({
                "source": backup_source,
                "addrs": addrs,
                "error": str(e)
            })

    except Exception as e:
        return flask.jsonify({
            "source": primary_source,
            "addrs": addrs,
            "error": str(e)
        })

    addrs = list(response.addresses)

    if response.failed:
        error = "unknown backend error"
    else:
        cache[parcel] = addrs
        if len(cache) > CACHE_SIZE:
            cache.popitem(last=False)

    return flask.jsonify({"source": source, "addrs": addrs, "error": error})


@app.route("/zip/<zipcode>")
def zip_lookup(zipcode):
    global last_source

    addrs = []

    if last_source == "2":
        source = "1"
        stub = stub1
    else:
        source = "2"
        stub = stub2

    try:
        response = stub.AddressByZip(
            property_pb2.ZipRequest(zip=zipcode), timeout=1
        )
    except grpc.RpcError:
        if source == "1":
            source = "2"
            stub = stub2
        else:
            source = "1"
            stub = stub1

        try:
            response = stub.AddressByZip(
                property_pb2.ZipRequest(zip=zipcode), timeout=1
            )
        except grpc.RpcError:
            return flask.jsonify({"source": source, "addrs": addrs, "error": "grpc error"})
        except Exception as e:
            return flask.jsonify({"source": source, "addrs": addrs, "error": str(e)})
    except Exception as e:
        return flask.jsonify({"source": source, "addrs": addrs, "error": str(e)})

    last_source = source

    addrs = list(response.addresses)
    error = ""
    if response.failed:
        error = "unknown backend error"

    return flask.jsonify({"source": source, "addrs": addrs, "error": error})


def main():
    app.run("0.0.0.0", port=5000, debug=False, threaded=False)


if __name__ == "__main__":
    main()
