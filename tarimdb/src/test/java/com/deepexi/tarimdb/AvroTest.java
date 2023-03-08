package com.deepexi.tarimdb;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
//import org.junit.Assert;
import org.junit.jupiter.api.Assertions;

import java.util.List;
import java.util.ArrayList;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.EOFException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@SpringBootTest
class AvroTest {

    private final static Logger logger = LogManager.getLogger(AvroTest.class);

    private final String schema_json = "{\"namespace\": \"org.apache.arrow.avro\","
                                     + "\"type\": \"record\","
                                     + "\"name\": \"TarimRecord\","
                                     + "\"fields\": ["
                                     + "    {\"name\": \"f0\", \"type\": \"string\"},"
                                     + "    {\"name\": \"f1\", \"type\": \"int\"},"
                                     + "    {\"name\": \"f2\", \"type\": \"boolean\"}"
                                     + "]}";

    @Test
    void contextLoads() {
    }

    @Test
    public void testSchema() throws Exception {
      logger.info("schema define in json: " + schema_json);
      Schema schema = new Schema.Parser().parse(schema_json);
      logger.info("schema in avro: " + schema.toString(true));
      logger.info("full name: " + schema.getFullName() + ", fields count: " + schema.getFields().size());
    }

    @Test
    public void testRecords() throws Exception 
    {
        Schema schema = new Schema.Parser().parse(schema_json);
        ArrayList<GenericRecord> data = new ArrayList<>();
        int writeSize = 0;
        for (int i = 0; i < 5; i++) 
        {
            GenericRecord record = new GenericData.Record(schema);
            record.put(0, "test" + i);
            record.put(1, i);
            record.put(2, i % 2 == 0);
            data.add(record);
            writeSize++;
        }

        logger.info("schema in avro: " + schema.toString(true));
        logger.info("testSchema() data size: " + data.size());
        Assertions.assertEquals(writeSize, data.size());

        ByteArrayOutputStream bytesOS = new ByteArrayOutputStream();
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(bytesOS, null); // or binaryEncoder() to create BufferedBinaryEncoder
        DatumWriter writer = new GenericDatumWriter(schema);

        for(GenericRecord record : data)
        {
            writer.write(record, encoder);
        }

        byte[] recordBuf = bytesOS.toByteArray();
        logger.info("testSchema() data size after encode: " + recordBuf.length);

        ByteArrayInputStream bytesIS = new ByteArrayInputStream(recordBuf);
        BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(bytesIS, null);
        GenericDatumReader<Object> reader = new GenericDatumReader<>();
        reader.setSchema(schema);

        int readSize = 0;
        while(true)
        {
            try {
                Object datum = reader.read(null, decoder);
                if(datum == null) break;
                readSize++;
                logger.info("datum: " + datum.toString());
            }catch(EOFException e){
                logger.info("read datum complete.");
                break;
            }
        }

        Assertions.assertEquals(writeSize, readSize);
    }
}
