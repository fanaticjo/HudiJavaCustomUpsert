package com.hudiUpsert;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;


public class hudiCustomUpsert extends OverwriteWithLatestAvroPayload {

    private static final Logger logger=LoggerFactory.getLogger(hudiCustomUpsert.class.getName());
    public hudiCustomUpsert(GenericRecord record,Comparable comparable){
        super(record,comparable);
    }

    public List<String> splitKeys(String keys) throws Exception {
        if (keys==null){
            throw new Exception("Keys cant be null");
        }
        else{
            return Arrays.stream(keys.split(",")).collect(Collectors.toList());
        }
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
        GenericRecord existingRecord= (GenericRecord) currentValue;
        GenericRecord incomingRecord= (GenericRecord) getInsertValue(schema).get();
        String update_key=properties.getProperty("hoodie.update.keys");
        logger.info("the update keys are -"+update_key);
        try {
            List<String> keys=splitKeys(properties.getProperty("hoodie.update.keys"));
            keys.forEach((key)->{
                Object value=incomingRecord.get(key);
                existingRecord.put(key,value);
            });
            return Option.of(existingRecord);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
