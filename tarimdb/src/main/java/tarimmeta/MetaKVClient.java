package tarimmeta;

import com.deepexi.rpc.TarimKVProto;
import io.grpc.stub.StreamObserver;

import java.util.List;

public interface MetaKVClient {

    public void put(String key, String value);

    /**
     */
    public String get(String key) ;
    /**
     */
    public List<TarimKVProto.KeyValue> prefixSeek(String cfName, String keyPrefix );
    /**
     */
    public void delete(TarimKVProto.DeleteRequest request, StreamObserver<TarimKVProto.StatusResponse> responseObserver);
}
