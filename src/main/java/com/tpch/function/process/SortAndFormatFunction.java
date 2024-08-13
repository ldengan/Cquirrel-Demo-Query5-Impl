package com.tpch.function.process;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.Set;

public class SortAndFormatFunction extends KeyedProcessFunction<String, Tuple2<String, Double>, String> {
    private transient Set<Tuple2<String, Double>> sortedBuffer;
    private static final long TIME_TRIGGER_MILLIS = 500;
    private ValueState<Long> lastUpdateTimeState;
    @Override
    public void open(Configuration parameters) throws Exception {
        sortedBuffer = new LinkedHashSet<>();
        lastUpdateTimeState =
                getRuntimeContext().getState(new ValueStateDescriptor<>("lastUpdateTimeState", Long.class));
    }
    @Override
    public void processElement(Tuple2<String, Double> value, Context ctx, Collector<String> out) throws Exception {
        sortedBuffer.add(value);
        lastUpdateTimeState.update(Instant.now().toEpochMilli());
        System.out.println("====== Add Value: " + value + " Last update time: " + lastUpdateTimeState.value());
        ctx.timerService().registerProcessingTimeTimer(lastUpdateTimeState.value() + TIME_TRIGGER_MILLIS);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        sortedBuffer.stream().sorted((a, b) -> {
            // 降序排序
            return Double.compare(b.f1, a.f1);
        });

        // 格式化并输出排序后的数据
        for (Tuple2<String, Double> sortedValue : sortedBuffer) {
            String formatted = String.format("%s, %.2f", sortedValue.f0, sortedValue.f1);
            System.out.println("====== Collect Value: " + sortedValue);
            out.collect(formatted);
        }
        // 清空缓冲区
        sortedBuffer.clear();
        lastUpdateTimeState.clear();
    }
}