package org.apache.flink.learning.table.utils;

public class Shipment {

    private String shipId;
    private String orderId;
    private String company;
    private long timestamp;

    public Shipment(String shipId, String orderId, String company, long timestamp) {
        this.shipId = shipId;
        this.orderId = orderId;
        this.company = company;
        this.timestamp = timestamp;
    }

    public String getShipId() {
        return shipId;
    }

    public void setShipId(String shipId) {
        this.shipId = shipId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
