package io.github.lb.kafka.examples.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class CustomGenericRecord {

  public static void main(String[] args) {
    String SCHEMA = "{" +
            "\"type\":\"record\"," +
            "\"name\":\"User\"," +
            "\"fields\":[" +
              "{\"name\":\"id\",\"type\":\"int\"}," +
              "{\"name\":\"name\", \"type\": \"string\"}, " +
              "{\"name\":\"age\", \"type\": \"int\"}" +
            "]" +
            "}";
    Schema schema = new Schema.Parser().parse(SCHEMA);
    GenericRecord genericRecord = new GenericData.Record(schema);
    genericRecord.put("id", 1);
    genericRecord.put("name", "John");
    genericRecord.put("age", 21);

    Injection<GenericRecord, byte[]> injection = GenericAvroCodecs.toBinary(schema);
    long start = System.currentTimeMillis();
    byte[] array = null;
    for (int i = 0; i < 1000; ++i) {
      array = injection.apply(genericRecord);
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start);
    for (byte b : array) {
      System.out.print(b);
    }
    System.out.println();
    System.out.println(array.length);

    GenericRecord record = injection.invert(array).get();
    System.out.println(record.get("id"));
    System.out.println(record.get("name"));
    System.out.println(record.get("age"));
  }
}
