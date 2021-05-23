package com.hudiUpsert;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
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

    public boolean checkColumnExists(List<String> keys, Schema schema){
        List<Field> field=schema.getFields();
        List<Field> common=field.stream()
                                      .filter(columns->keys.contains(columns.name()))
                                      .collect(Collectors.toList());
        return common.size()==keys.size()?true:false;
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
        GenericRecord existingRecord= (GenericRecord) currentValue;
        GenericRecord incomingRecord= (GenericRecord) getInsertValue(schema).get();
        try {
            List<String> keys=splitKeys(properties.getProperty("hoodie.update.keys"));
            if (checkColumnExists(keys,schema)) {
                keys.forEach((key) -> {
                    Object value = incomingRecord.get(key);
                    existingRecord.put(key, value);
                });
                return Option.of(existingRecord);
            }
            else{
                throw new Exception("Update key not present please check the names");
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
