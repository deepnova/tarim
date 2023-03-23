package com.deepexi.tarimdb.datamodels;

import com.deepexi.rpc.TarimKVProto;
import com.deepexi.tarimdb.util.TarimKVException;
import com.google.protobuf.ByteString;
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

        requestBuilder.setChunkID(partitionID.hashCode());
        requestBuilder.setTableID(tableID);

        ByteArrayInputStream bytesIS = new ByteArrayInputStream(records);
        BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(bytesIS, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        //todo schemaJson should be create from the metaNode
        String schemaJson = "{\"namespace\": \"org.apache.arrow.avro\","
                + "\"type\": \"record\","
                + "\"name\": \"TarimRecord\","
                + "\"fields\": ["
                + "{\"name\": \"userID\", \"type\": \"int\"},"
                + "    {\"name\": \"age\", \"type\": \"int\"},"
                + "    {\"name\": \"class\", \"type\": \"string\"}"
                + "]}";
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
}
