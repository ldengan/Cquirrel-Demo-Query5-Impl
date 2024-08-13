package com.tpch.utils;

import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Locale;

public class TimeFormatConvert {
    private static final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd",  Locale.US);
    public static Date getFormatDate(String source) throws ParseException {
        String s = "1990-01-01";
        if (StringUtils.isBlank(source)) {
            source = s;
        }
        try {
            return new Date(simpleDateFormat.parse(source).getTime());
        } catch (Exception e) {
            System.out.println("Error Source=" + source);
        }
        return  new Date(simpleDateFormat.parse("1971-01-01").getTime());
    }
}
