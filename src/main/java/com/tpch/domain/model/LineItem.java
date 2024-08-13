package com.tpch.domain.model;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class LineItem {
    public Boolean update;
    public int orderKey;
    public int partKey;
    public int suppKey;
    public int lineNumber;
    public double quantity;
    public double extendedPrice; //reserved column
    public double discount; //reserved column
    public double tax;
    public String returnFlag;
    public String lineStatus;
    public Date shipDate;
    public Date commitDate;
    public Date receiptDate;
    public String shiPinStruct;
    public String shipMode;
    public String comment;

    public LineItem(String[] fields) {
        if (fields.length < 18) {
            return;
        }
        this.update = fields[0].equals("+");
        this.orderKey = Integer.parseInt(fields[2]);
        this.partKey = Integer.parseInt(fields[3]);
        this.suppKey = Integer.parseInt(fields[4]);
        this.lineNumber = Integer.parseInt(fields[5]);
        this.quantity = Double.parseDouble(fields[6]);
        this.extendedPrice = Double.parseDouble(fields[7]);
        this.discount = Double.parseDouble(fields[8]);
        this.tax = Double.parseDouble(fields[9]);
        this.returnFlag = fields[10];
        this.lineStatus = fields[11];
        //SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy",  Locale.US);// 26/7/1992
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd",  Locale.US);// "1992-06-27"
        try {
            this.shipDate = new Date(simpleDateFormat.parse(fields[12]).getTime());
            this.commitDate = new Date(simpleDateFormat.parse(fields[13]).getTime());
            this.receiptDate = new Date(simpleDateFormat.parse(fields[14]).getTime());
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        this.shiPinStruct = fields[15];
        this.shipMode = fields[16];
        this.comment = fields[17];
    }
}
