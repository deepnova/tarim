package tarimmeta;

import com.deepexi.rpc.TarimKVProto;
import com.deepexi.tarimdb.tarimkv.Slot;
import com.deepexi.tarimdb.util.TarimKVException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;

import java.util.List;

public class MetaKVLocal implements MetaKVClient{
    Slot slot; //for local mode
    private final static Logger logger = LogManager.getLogger(MetaKVLocal.class);

    public MetaKVLocal(KVMetadata metadata){
        this.slot = new Slot(metadata.metaSlotConf);
        try {
            slot.open();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void put(String key, String value) {
        slot.put(key, value);
    }

    @Override
    public String get(String key)  {
        return null;
    }

    @Override
    public List<TarimKVProto.KeyValue> prefixSeek(String cfName, String keyPrefix ) {
        ReadOptions readOpt = new ReadOptions();
        ColumnFamilyHandle cfh;

        try {
            cfh = slot.getColumnFamilyHandle(cfName);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } catch (TarimKVException e) {
            throw new RuntimeException(e);
        }
        List<TarimKVProto.KeyValue> values = null;
        try {
            values = slot.prefixSeekForSchema(readOpt, cfh, keyPrefix);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        } catch (TarimKVException e) {
            throw new RuntimeException(e);
        }
        return values;
    }

    @Override
    public void delete(TarimKVProto.DeleteRequest request, StreamObserver<TarimKVProto.StatusResponse> responseObserver) {

    }
}
