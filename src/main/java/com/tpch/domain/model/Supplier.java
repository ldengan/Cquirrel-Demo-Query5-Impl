package com.tpch.domain.model;

public class Supplier {
    public Boolean update;
    public int suppKey;
    public String name;
    public String address;
    public int nationKey;
    public String phone;
    public float acctbal;
    public String comment;

    public Supplier(String[] fields) {
        if (fields.length < 9) {
            return;
        }
        this.update = fields[0].equals("+");
        this.suppKey = Integer.parseInt(fields[2]);
        this.name = fields[3];
        this.address = fields[4];
        this.nationKey = Integer.parseInt(fields[5]);
        this.phone = fields[6];
        this.acctbal = Float.parseFloat(fields[7]);
        this.comment = fields[8];
    }
}
