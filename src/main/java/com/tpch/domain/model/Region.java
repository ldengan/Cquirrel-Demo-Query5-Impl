package com.tpch.domain.model;

public class Region {
    public Boolean update;
    public int regionKey;
    public String name;
    public String comment;

    public Region(String[] fields) {
        if (fields.length < 5) {
            return;
        }
        this.update = fields[0].equals("+");
        this.regionKey = Integer.parseInt(fields[2]);
        this.name = fields[3];
        this.comment = fields[4];
    }
    public static enum RegionKey
    {
        ASIA,
        AMERICA;
    }
}
