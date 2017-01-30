package de.tuberlin.serialization;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.UnsupportedEncodingException;
import java.util.Map;

/**
 * Created by patrick on 26.12.16.
 */
public class SparkStringTsDeserializer implements Deserializer<String> {


        private String encoding = "UTF8";

        public SparkStringTsDeserializer() {
        }

        public void configure(Map<String, ?> configs, boolean isKey) {
            String propertyName = isKey?"key.deserializer.encoding":"value.deserializer.encoding";
            Object encodingValue = configs.get(propertyName);
            if(encodingValue == null) {
                encodingValue = configs.get("deserializer.encoding");
            }

            if(encodingValue != null && encodingValue instanceof String) {
                this.encoding = (String)encodingValue;
            }

        }

        public String deserialize(String topic, byte[] data) {
            try {
               // System.out.println(new String(data, this.encoding)+","+System.currentTimeMillis());
                return data == null?null:new String(data, this.encoding)+","+System.currentTimeMillis();
            } catch (UnsupportedEncodingException var4) {
                throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.encoding);
            }
        }

        public void close() {
        }
    }
