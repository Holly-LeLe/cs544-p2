import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import property.Property.ParcelRequest;
import property.Property.AddressResponse;
import property.PropertyLookupGrpc;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class DatasetServer extends PropertyLookupGrpc.PropertyLookupImplBase {
    // parcel string -> sorted list of addresses
    private final Map<String, List<String>> parcelIndex = new HashMap<>();

    public DatasetServer(String csvPath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new GZIPInputStream(new FileInputStream(csvPath))))) {
            String header = reader.readLine();

            // We need Parcel (index 3) and Address (index 9)
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",", -1);
                if (fields.length < 10) continue;
                String parcel = fields[3];
                String address = fields[9];
                parcelIndex.computeIfAbsent(parcel, k -> new ArrayList<>()).add(address);
            }
        }
        // sort each address list
        for (List<String> addrs : parcelIndex.values()) {
            Collections.sort(addrs);
        }
        System.out.println("Loaded " + parcelIndex.size() + " parcels");
    }

    @Override
    public void addressByParcel(ParcelRequest request, StreamObserver<AddressResponse> responseObserver) {
        String parcel = request.getParcel();
        List<String> addrs = parcelIndex.getOrDefault(parcel, null);
        AddressResponse.Builder builder = AddressResponse.newBuilder();
        if (addrs == null) {
            builder.setFailed(true);
        } else {
            builder.addAllAddresses(addrs);
        }
        AddressResponse response = builder.build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public static void main(String[] args) throws Exception {
        DatasetServer service = new DatasetServer("/data/addresses.csv.gz");
        Server server = ServerBuilder.forPort(5000)
                .addService(service)
                .executor(java.util.concurrent.Executors.newFixedThreadPool(1))
                .build()
                .start();
        System.out.println("Server started on port 5000");
        server.awaitTermination();
    }
}
