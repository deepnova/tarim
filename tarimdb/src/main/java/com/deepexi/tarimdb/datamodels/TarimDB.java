package com.deepexi.tarimdb.datamodels;

import com.deepexi.rpc.TarimKVProto;
import com.deepexi.rpc.TarimProto;
import com.deepexi.tarimdb.util.Common;
import com.deepexi.tarimdb.util.TarimKVException;
import com.google.protobuf.ByteString;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.deepexi.tarimdb.util.Status;
import com.deepexi.tarimdb.tarimkv.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.deepexi.tarimdb.util.Common.toChunkID;

/**
 * TarimDB
 *
 */
public class TarimDB extends AbstractDataModel {

    public final static Logger logger = LogManager.getLogger(TarimDB.class);

    public TarimDB() {
    }

    @Override
    public Status init(TarimKVClient kv) {
        super.init(kv);
        logger.info( "This is TarimDB!" );
        return Status.OK;
    }

    @Override
    public void run() {
        //TODO
        logger.info( "Do some work in TarimDB!" );
    }

    public int insertMsgProc(int tableID, String partitionID, String primaryKey, byte[] records) {

        TarimKVProto.PutRequest.Builder requestBuilder = TarimKVProto.PutRequest.newBuilder();
        TarimKVProto.KeyValueByte.Builder kvBuiler = TarimKVProto.KeyValueByte.newBuilder();

        requestBuilder.setChunkID(toChunkID(partitionID));
        requestBuilder.setTableID(tableID);

        ByteArrayInputStream bytesIS = new ByteArrayInputStream(records);
        BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(bytesIS, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        //todo schemaJson should be create from the metaNode
        String schemaJson = Common.loadTableMeta("tablemeta.json");
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaJson);

        reader.setSchema(schema);

        ByteArrayOutputStream bytesOS = new ByteArrayOutputStream();
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(bytesOS, null); // or binaryEncoder() to create BufferedBinaryEncoder
        DatumWriter writer = new GenericDatumWriter(schema);
        int readSize = 0;
        int index = 0;
        while(true)
        {
            try {
                GenericRecord datum = reader.read(null, decoder);
                if(datum == null) break;
                //todo we will support to add some hidden schema, so we have to decode the data

                writer.write(datum, encoder);

                readSize++;
                kvBuiler.setKey(datum.get(primaryKey).toString());
                kvBuiler.setValue(ByteString.copyFrom(bytesOS.toByteArray()));
                requestBuilder.addValues(index, kvBuiler.build());
                bytesOS.reset();
                index++;
                logger.info(readSize + ": " + datum.get("userID") + ", " + datum.get("age") + ", " + datum.get("class")
                        + ". Datum: " + datum.toString());

            }catch(EOFException e){
                logger.info("read datum complete.");
                break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        TarimKVProto.PutRequest request = requestBuilder.build();

        try{
            getTarimKVClient().put(request);
        }catch (TarimKVException e){
            logger.error("put data err!");
            return 1;
        }

        return 0;
    }

    public TarimProto.PrepareScanResponse preScan(int tableID, List<String> partitionIDs) throws TarimKVException {

        TarimProto.PrepareScanResponse.Builder respBuilder = TarimProto.PrepareScanResponse.newBuilder();
        TarimProto.ScanInfo.Builder scanBuilder = TarimProto.ScanInfo.newBuilder();
        respBuilder.setMsg("OK");
        respBuilder.setCode(0);

        TarimProto.Partition.Builder partitionBuilder = TarimProto.Partition.newBuilder();

        List<TarimProto.Partition> partitions = new ArrayList<>();

        int size = partitionIDs.size();
        if (size == 0){
            //return ok, but the partition list is null
            return respBuilder.build();
        }


        long[] chunks = new long[size];

        for (int i = 0; i < size; i++){
            chunks[i] = toChunkID(partitionIDs.get(i));
        }
        TarimKVClient client = getTarimKVClient();
        KVSchema.PrepareScanInfo result = client.prepareChunkScan(tableID, chunks);

        for (int i = 0; i < result.chunkDetails.size(); i++){
            partitionBuilder.setPartitionID(partitionIDs.get(i));
            partitionBuilder.setScanHandler(result.chunkDetails.get(i).scanHandler);
            partitionBuilder.setMergePolicy(result.chunkDetails.get(i).mergePolicy);
            partitionBuilder.addAllMainPaths(result.chunkDetails.get(i).mainPaths);
            partitionBuilder.setPort(client.getKVLocalMetadata().port);
            partitionBuilder.setHost(client.getKVLocalMetadata().address);
            partitions.add(partitionBuilder.build());
        }

        scanBuilder.addAllPartitions(partitions);
        respBuilder.setScanInfo(scanBuilder.build());
        return respBuilder.build();
    }

    public TarimProto.ScanResponse scanMsgProc(int tableID, long scanHandle, String partitionID) throws TarimKVException {
        KVSchema.DeltaScanParam DeltaScanParam = new KVSchema.DeltaScanParam();
        DeltaScanParam.tableID = tableID;
        DeltaScanParam.scanHandler = scanHandle;
        DeltaScanParam.chunkID = toChunkID(partitionID);

        TarimKVClient client = getTarimKVClient();
        List<TarimKVProto.KeyValueOp> result = client.deltaChunkScan(DeltaScanParam, true);
        //the op all are 1 now

        //todo schemaJson should be create from the metaNode
        String schemaJson = Common.loadTableMeta("tablemeta.json");
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaJson);

        TarimProto.ScanResponse.Builder respBuilder = TarimProto.ScanResponse.newBuilder();
        ByteArrayOutputStream bytesOS = new ByteArrayOutputStream();
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(bytesOS, null); // or binaryEncoder() to create BufferedBinaryEncoder
        DatumWriter writer = new GenericDatumWriter(schema);

        for(TarimKVProto.KeyValueOp record : result) {
            try {
                writer.write(record.getValue(), encoder);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        //test all insert first
        respBuilder.setCode(0);
        respBuilder.setMsg("OK");
        respBuilder.setRecords(ByteString.copyFrom(bytesOS.toByteArray()));

        return respBuilder.build();
    }
}
