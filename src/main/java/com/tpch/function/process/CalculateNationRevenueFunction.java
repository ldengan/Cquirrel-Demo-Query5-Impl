package com.tpch.function.process;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Input: update,  nation.name, supplier.suppKey, lineItem.extendedPrice, lineItem.discount, lineItem.suppKey
 * OutPut: nation.name, revenue
 */
public class CalculateNationRevenueFunction extends KeyedProcessFunction<String, Tuple6<Boolean, String, Integer, Double, Double, Integer>, Tuple3<String, Double, Integer>> {
    private transient MapState<String, List<Double>> nationRevenueState;

    private static final long TIME_TRIGGER_MILLIS = 1000;

    private ValueState<Long> lastUpdateTimeState;

    @Override
    public void open(Configuration parameters) throws Exception {
//        revenueState = getRuntimeContext().getState(new ValueStateDescriptor<>("revenueState", Types.DOUBLE));
        nationRevenueState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("nationRevenueState", Types.STRING, Types.LIST(Types.DOUBLE)));

        lastUpdateTimeState =
                getRuntimeContext().getState(new ValueStateDescriptor<>("lastUpdateTimeState", Long.class));
    }

    @Override
    public void processElement(Tuple6<Boolean, String, Integer, Double, Double, Integer> tuple,
                               Context context, Collector<Tuple3<String, Double, Integer>> collector) throws Exception {
        List<Double> tmp = new ArrayList<>();
        double lineItemRevenue = tuple.f3 * (1 - tuple.f4);
        if (!nationRevenueState.contains(tuple.f1)) { //第一条数据
            tmp.add(lineItemRevenue);
            nationRevenueState.put(tuple.f1, tmp);
            System.out.println("第一条数据: tuple=" + tuple);
        } else {
            tmp = nationRevenueState.get(tuple.f1);
            tmp.add(lineItemRevenue);
            nationRevenueState.put(tuple.f1, tmp);
            System.out.println("更新数据: tuple=" + tuple);
        }
        lastUpdateTimeState.update(Instant.now().toEpochMilli());
        context.timerService().registerEventTimeTimer(lastUpdateTimeState.value() + TIME_TRIGGER_MILLIS);
    }
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<String, Double, Integer>> out) throws Exception {
        for (Map.Entry<String, List<Double>> entry : nationRevenueState.entries()) {
            String nationName = entry.getKey();
            List<Double> revenues = entry.getValue();
            double sum = 0.0;
            for (Double d :revenues) {
                sum += d;
            }
            BigDecimal bigDecimal = BigDecimal.valueOf(sum);
            bigDecimal = bigDecimal.setScale(2, RoundingMode.HALF_UP);
            out.collect(Tuple3.of(nationName, bigDecimal.doubleValue(), revenues.size()));
        }
        // 清空缓冲区
        nationRevenueState.clear();
        lastUpdateTimeState.clear();
    }
}