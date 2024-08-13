package com.tpch.domain.model;

import com.tpch.utils.TimeFormatConvert;

import java.sql.Date;
import java.text.ParseException;

public class Orders {
    public Boolean update;
    public int orderKey;
    public int custKey;
    public String orderStatus;
    public double totalPrice;
    public Date orderDate;
    public String orderPriority;
    public String clerk;
    public String shipPriority;
    public String comment;

    public Orders(String[] fields) {
        if (fields.length < 11) {
            return;
        }
        this.update = fields[0].equals("+");
        this.orderKey = Integer.parseInt(fields[2]);
        this.custKey = Integer.parseInt(fields[3]);
        this.orderStatus = fields[4];
        this.totalPrice = Float.parseFloat(fields[5]);
        try {
            this.orderDate = TimeFormatConvert.getFormatDate(fields[6]);
        } catch (ParseException e) {
            System.out.println("Error fields[6] in Orders:" + fields[6]);
//            throw new RuntimeException(e);
        }
        this.orderPriority = fields[7];
        this.clerk = fields[8];
        this.shipPriority = fields[9];
        this.comment = fields[10];
    }
}
