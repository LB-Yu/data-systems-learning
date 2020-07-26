package io.github.lb.kafka.examples.avro;

import com.twitter.bijection.Injection;
import com.twitter.bijection.avro.GenericAvroCodecs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class UserGenericRecord {
  private static final String SCHEMA = "{" +
            "\"type\":\"record\"," +
            "\"name\":\"User\"," +
            "\"fields\":[" +
              "{\"name\":\"id\",\"type\":\"int\"}," +
              "{\"name\":\"name\", \"type\": \"string\"}, " +
              "{\"name\":\"age\", \"type\": \"int\"}" +
            "]" +
          "}";
  private final Schema schema = new Schema.Parser().parse(SCHEMA);
  private final GenericRecord genericRecord = new GenericData.Record(schema);
  private final Injection<GenericRecord, byte[]> injection = GenericAvroCodecs.toBinary(schema);

  public UserGenericRecord() { }

  public byte[] serialize(User user) {
    genericRecord.put("id", user.getId());
    genericRecord.put("name", user.getName());
    genericRecord.put("age", user.getAge());
    return injection.apply(genericRecord);
  }

  public User deserialize(byte[] bytes) {
    GenericRecord record = injection.invert(bytes).get();
    int id = (int) record.get("id");
    String name = record.get("name").toString();
    int age = (int) record.get("age");
    return new User(id, name, age);
  }
}
