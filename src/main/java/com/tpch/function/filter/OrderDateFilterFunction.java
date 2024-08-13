package com.tpch.function.filter;

import com.tpch.domain.model.Orders;
import com.tpch.utils.TimeFormatConvert;
import org.apache.flink.api.common.functions.FilterFunction;

import java.sql.Date;
import java.text.ParseException;

public class OrderDateFilterFunction implements FilterFunction<Orders> {
    @Override
    public boolean filter(Orders order) throws ParseException {
        try {
            Date startDate = TimeFormatConvert.getFormatDate("1994-01-01");
            Date endDate = TimeFormatConvert.getFormatDate("1995-01-01");
            // 检查日期是否在范围内
            return order.orderDate.compareTo(startDate) >= 0 && order.orderDate.compareTo(endDate) < 0;
        } catch (ParseException exception) {
            System.out.println("Error order filter, orderKey=" + order.orderKey + ", date=" + order.orderDate);
        }
        return false;
    }
}