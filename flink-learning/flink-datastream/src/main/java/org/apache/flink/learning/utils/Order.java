package org.apache.flink.learning.utils;

public class Order {

  private String orderId;
  private String userName;
  private String item;
  private long timestamp;

  public Order() { }

  public Order(String orderId, String userName, String item, long timestamp) {
    this.orderId = orderId;
    this.userName = userName;
    this.item = item;
    this.timestamp = timestamp;
  }

  public String getOrderId() {
    return orderId;
  }

  public void setOrderId(String orderId) {
    this.orderId = orderId;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public String getItem() {
    return item;
  }

  public void setItem(String item) {
    this.item = item;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }
}
