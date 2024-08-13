package com.tpch.domain.model;

public class Nation {
    public Boolean update;
    public int nationKey;

    public String name; //reserved column
    public int regionKey;
    public String comment;
    public Nation(String[] fields) {
        if (fields.length < 6) {
            return;
        }
        this.update = fields[0].equals("+");
        this.nationKey = Integer.parseInt(fields[2]);
        this.name = fields[3];
        this.regionKey = Integer.parseInt(fields[4]);
        this.comment = fields[5];
    }
}
