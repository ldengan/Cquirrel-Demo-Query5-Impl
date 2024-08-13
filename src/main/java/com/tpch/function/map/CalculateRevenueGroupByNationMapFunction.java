package com.tpch.function.map;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

public class CalculateRevenueGroupByNationMapFunction extends RichMapFunction<Tuple2<String, List<Double>>, Tuple2<String, Double>> {
    @Override
    public Tuple2<String, Double> map(Tuple2<String, List<Double>> tuple) throws Exception {
        double sum = 0.0;
        for (Double d : tuple.f1) {
            sum += d;
        }
        double revenue = sum / tuple.f1.size();
        BigDecimal bigDecimal = new BigDecimal(revenue);
        bigDecimal = bigDecimal.setScale(2, RoundingMode.HALF_UP);
        return Tuple2.of(tuple.f0, bigDecimal.doubleValue());
    }
}
