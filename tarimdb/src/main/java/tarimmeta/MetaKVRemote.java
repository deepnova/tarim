package tarimmeta;

import com.deepexi.rpc.TarimKVMetaGrpc;
import com.deepexi.rpc.TarimKVProto;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class MetaKVRemote implements MetaKVClient{

    private final static Logger logger = LogManager.getLogger(MetaKVRemote.class);
    private String host;
    private int port;

    public MetaKVRemote(String host, int port){
        this.host = host;
        this.port = port;
    }

    @Override
    public void put(String key, String value) {
        //todo
        ManagedChannel channel;
        TarimKVMetaGrpc.TarimKVMetaBlockingStub blockStub;
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        blockStub = TarimKVMetaGrpc.newBlockingStub(channel);
        //send the msg

    }

    @Override
    public String get(String key) {
        //todo
        return null;
    }

    @Override
    public List<TarimKVProto.KeyValue> prefixSeek(String cfName, String keyPrefix) {
        return null;
    }

    @Override
    public void delete(TarimKVProto.DeleteRequest request, StreamObserver<TarimKVProto.StatusResponse> responseObserver) {
        //todo
    }
}
