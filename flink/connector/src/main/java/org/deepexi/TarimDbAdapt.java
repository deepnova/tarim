package org.deepexi;

import com.deepexi.rpc.TarimProto;
import com.google.protobuf.ByteString;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Table;
import org.deepexi.source.DeltaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.formats.avro.AvroToRowDataConverters.createRowConverter;

public class TarimDbAdapt implements Serializable {
    //temp members
    private static final Logger logger = LoggerFactory.getLogger(TarimDbAdapt.class);
    private transient int subTaskId;
    private transient int attemptId;

    private transient Table table;


    public TarimDbAdapt(int subTaskId, int attemptId, Table table){
        this.subTaskId = subTaskId;
        this.attemptId = attemptId;
        this.table = table;
    }

    public <T> void writeData(List<String> element) {
        //todo, send data to trimDB
        logger.info("writeData to tarimDB");
        return;
    }

    /*
    public StreamRecord<WResult> complete() {

        //todo , return to committed files operator
        return;
    }
    */
    public void endData(){
        //todo nodify the ending, send to tarimDB
        logger.info("send endData to trimDB");
        return;
    };

    public boolean doCheckponit(long checkpointId){
        //todo ,send to tarimDB
        logger.info("send checkponit to trimDB");
        return true;
    }

    public Long getLastommittedCheckpointId(){
        //todo, send to tarimDB
        logger.info("send getLastommittedCheckpointId to trimDB");
        return -1L;
    }

    public List<DeltaData> getDeltaData(int tableID, RowType rowType, String partitionID, String schemaJson, long handle, String host, int port) throws InterruptedException {
        //todo, get delta data from tarimDB
        logger.info("getDeltaData to tarimDB");
        List<DeltaData> datalist = new ArrayList<>();

        TarimDbClient client = new TarimDbClient(host, port);

        TarimProto.ScanResponse response = client.scanRequest(tableID, handle, partitionID);
        ByteArrayInputStream bytesIS = new ByteArrayInputStream(response.getRecords().toByteArray());

        AvroToRowDataConverters.AvroToRowDataConverter converter = createRowConverter(rowType);

        BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(bytesIS, null);
        org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaJson);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        reader.setSchema(schema);

        int readSize = 0;

        while(true) {
            try {
                GenericRecord datum = reader.read(null, decoder);
                RowData data = (RowData)converter.convert(datum);
                DeltaData deltaData = new DeltaData(data);
                datalist.add(deltaData);
                readSize++;

                logger.info(readSize + ": " + datum.get("userID") + ", " + datum.get("age") + ", " + datum.get("class")
                        + ". Datum: " + datum.toString());

            }catch(EOFException e){
                logger.info("read datum complete.");
                break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        //simulate some data
        /*
        if (partitionID.equals("d1")){
            datalist.add(new DeltaData(GenericRowData.of(fromString("d1222"), 2)));

            datalist.add(new DeltaData(GenericRowData.of(fromString("d1"), 10000)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d1"), 10001)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d1"), 10002)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d1"), 10003)));

        }else if(partitionID.equals("d3")){
            datalist.add(new DeltaData(GenericRowData.of(fromString("33333"), 3)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d3"), 30000)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d3"), 30001)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d3"), 30002)));
            datalist.add(new DeltaData(GenericRowData.of(fromString("d3"), 30003)));
        }else{
            return datalist;
        }

        Thread.sleep(3000);
         */
        return datalist;
    }
}
