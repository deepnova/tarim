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
import java.lang.ArrayIndexOutOfBoundsException;

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

    private final String schema2_json = "{\"namespace\": \"org.apache.arrow.avro\","
                                      + "\"type\": \"record\","
                                      + "\"name\": \"TarimRecord\","
                                      + "\"fields\": ["
                                      + "    {\"name\": \"f0\", \"type\": \"string\"},"
                                      + "    {\"name\": \"f1\", \"type\": \"int\"},"
                                      + "    {\"name\": \"f2\", \"type\": \"boolean\"},"
                                      + "    {\"name\": \"_fv\", \"type\": [\"int\", \"null\"], \"default\": 0}"
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
        for (int i = 8; i < 15; i++) 
        {
            GenericRecord record = new GenericData.Record(schema);
            record.put(0, "test" + i);
            record.put(1, i);
            record.put(2, i % 2 == 0);
            data.add(record);
            writeSize++;
        }

        logger.info("schema in avro: " + schema.toString(true));
        logger.info("data size: " + data.size());
        Assertions.assertEquals(writeSize, data.size());

        ByteArrayOutputStream bytesOS = new ByteArrayOutputStream();
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(bytesOS, null); // or binaryEncoder() to create BufferedBinaryEncoder
        DatumWriter writer = new GenericDatumWriter(schema);

        int lastSize = 0;
        //ArrayList<byte[]> dataList = new ArrayList<>();
        for(GenericRecord record : data)
        {
            writer.write(record, encoder);
            logger.info("bytes buffer size: " + bytesOS.size() + ", add size: " + (bytesOS.size() - lastSize));
            lastSize = bytesOS.size();
            //byte[] bb = bytesOS.toByteArray();
            //dataList.add(bb);
            //logger.info("every bytes buffer size: " + );
        }

        byte[] recordBuf = bytesOS.toByteArray();
        logger.info("data size after encode: " + recordBuf.length);

        ByteArrayInputStream bytesIS = new ByteArrayInputStream(recordBuf);
        BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(bytesIS, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        reader.setSchema(schema);

        int readSize = 0;
        while(true)
        {
            try {
                GenericRecord datum = reader.read(null, decoder);
                if(datum == null) break;
                readSize++;
                logger.info(readSize + ": " + datum.get("f0") + ", " + datum.get("f1") + ", " + datum.get("f2")
                                     + ". Datum: " + datum.toString());
            }catch(EOFException e){
                logger.info("read datum complete.");
                break;
            }
        }

        Assertions.assertEquals(writeSize, readSize);
    }

    @Test
    public void testRecords2() throws Exception 
    {
        Schema schema = new Schema.Parser().parse(schema_json);
        ArrayList<GenericRecord> data = new ArrayList<>();
        int writeSize = 0;
        for (int i = 8; i < 15; i++) 
        {
            GenericRecord record = new GenericData.Record(schema);
            record.put(0, "test" + i);
            record.put(1, i);
            record.put(2, i % 2 == 0);
            data.add(record);
            writeSize++;
        }

        logger.info("schema in avro: " + schema.toString(true));
        logger.info("data size: " + data.size());
        Assertions.assertEquals(writeSize, data.size());

        ByteArrayOutputStream bytesOS = new ByteArrayOutputStream();
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(bytesOS, null); // or binaryEncoder() to create BufferedBinaryEncoder
        DatumWriter writer = new GenericDatumWriter(schema);

        int lastSize = 0;
        ArrayList<byte[]> dataList = new ArrayList<>();
        for(GenericRecord record : data)
        {
            writer.write(record, encoder);
            logger.info("bytes buffer size: " + bytesOS.size() + ", add size: " + (bytesOS.size() - lastSize));
            lastSize = bytesOS.size();
            byte[] bb = bytesOS.toByteArray();
            dataList.add(bb);
            logger.info("every bytes buffer size: " + bb.length);
            bytesOS.reset();
        }

        //byte[] recordBuf = bytesOS.toByteArray();
        //logger.info("data size after encode: " + recordBuf.length);

        //ByteArrayInputStream bytesIS = new ByteArrayInputStream(recordBuf);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        reader.setSchema(schema);

        int readSize = 0;
        BinaryDecoder decoder = new DecoderFactory().binaryDecoder(dataList.get(readSize), 0, dataList.get(readSize).length, null);
        while(true)
        {
            try {
                //decoder = new DecoderFactory().directBinaryDecoder(bytesIS, decoder);
                if(readSize > 0) decoder = new DecoderFactory().binaryDecoder(dataList.get(readSize), 0, dataList.get(readSize).length, decoder);
                GenericRecord datum = reader.read(null, decoder);
                if(datum == null) break;
                readSize++;
                logger.info(readSize + ": " + datum.get("f0") + ", " + datum.get("f1") + ", " + datum.get("f2")
                                     + ". Datum: " + datum.toString());
                if(readSize >= dataList.size()) break;
            }catch(EOFException e){
                logger.info("read datum complete.");
                break;
            }
        }

        Assertions.assertEquals(writeSize, readSize);
    }

    @Test
    public void testSchemaDiff() throws Exception 
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
        logger.info("data size: " + data.size());
        Assertions.assertEquals(writeSize, data.size());

        ByteArrayOutputStream bytesOS = new ByteArrayOutputStream();
        BinaryEncoder encoder = new EncoderFactory().directBinaryEncoder(bytesOS, null); // or binaryEncoder() to create BufferedBinaryEncoder
        DatumWriter writer = new GenericDatumWriter(schema);

        for(GenericRecord record : data)
        {
            writer.write(record, encoder);
            logger.info("bytes buffer size: " + bytesOS.size());
        }

        byte[] recordBuf = bytesOS.toByteArray();
        logger.info("data size after encode: " + recordBuf.length);

        ByteArrayInputStream bytesIS = new ByteArrayInputStream(recordBuf);
        BinaryDecoder decoder = new DecoderFactory().directBinaryDecoder(bytesIS, null);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>();
        Schema schema2 = new Schema.Parser().parse(schema2_json);
        reader.setSchema(schema2);

        int readSize = 0;
        while(true)
        {
            try {
                GenericRecord datum = reader.read(null, decoder);
                if(datum == null) break;
                readSize++;
                logger.info(readSize + ": " + datum.get("f0") + ", " + datum.get("f1") + ", " + datum.get("f2")
                                     + ". Datum: " + datum.toString());
            }catch(EOFException e){
                logger.info("read datum complete.");
                break;
            }catch(ArrayIndexOutOfBoundsException e){
                logger.info("schema parse error.");
                Assertions.assertFalse(false);
                break;
            }
            Assertions.assertFalse(true);
        }
    }
}
