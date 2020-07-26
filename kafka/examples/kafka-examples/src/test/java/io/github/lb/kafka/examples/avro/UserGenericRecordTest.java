package io.github.lb.kafka.examples.avro;

public class UserGenericRecordTest {

  @org.junit.Test
  public void serializeDeserialize() {
    UserGenericRecord genericRecord = new UserGenericRecord();

    User user1 = new User(1, "John", 21);
    User user2 = new User(2, "Li", 22);

    long start = System.currentTimeMillis();
    for (int i = 0; i < 1000; i++) {
      byte[] bytes1 = genericRecord.serialize(user1);
      byte[] bytes2 = genericRecord.serialize(user2);

      User invertUser1 = genericRecord.deserialize(bytes1);
      User invertUser2 = genericRecord.deserialize(bytes2);
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start);

  }
}