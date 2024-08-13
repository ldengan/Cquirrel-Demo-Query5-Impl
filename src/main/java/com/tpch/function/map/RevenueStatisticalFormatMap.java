package com.tpch.function.map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class RevenueStatisticalFormatMap extends RichMapFunction<Tuple3<String, Double, Integer>, String> {
    @Override
    public String map(Tuple3<String, Double, Integer> tuple) throws Exception {
        return String.format("nationName: %s, sumRevenue: %.2f, totalRecords: %d",
                tuple.f0, tuple.f1, tuple.f2);
    }
}