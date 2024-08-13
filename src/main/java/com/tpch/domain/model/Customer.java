package com.tpch.domain.model;

public class Customer {
    public Boolean update;
    public int custKey;

    public String name;
    public String address;
    public int nationKey;
    public String phone;
    public float acctbal;
    public String mktSegment;
    public String comment;

    public Customer(String[] fields) {
        if (fields.length < 10) {
            return;
        }
        this.update = fields[0].equals("+");
        this.custKey = Integer.parseInt(fields[2]);
        this.name = fields[3];
        this.address = fields[4];
        this.nationKey = Integer.parseInt(fields[5]);
        this.phone = fields[6];
        this.acctbal = Float.parseFloat(fields[7]);
        this.mktSegment = fields[8];
        this.comment = fields[9];
    }
}