package com.tpch.function.process;

import com.tpch.domain.model.Supplier;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Input: update, nation.name, nation.nationKey
 * OutPut: update, nation.name, nation.nationKey, supplier.suppKey
 */
public class SupplierJoinNationAndRegionFunction extends CoProcessFunction<
        Tuple3<Boolean, String, Integer>,
        Supplier,
        Tuple4<Boolean, String, Integer, Integer>> {
    private ValueState<Tuple3<Boolean, String, Integer>> leftState;
    private ListState<Supplier> supplierListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        leftState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("leftState",
                        Types.TUPLE(Types.BOOLEAN, Types.STRING, Types.INT))
        );
        supplierListState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("supplierState", Supplier.class)
        );
    }

    @Override
    public void processElement1(Tuple3<Boolean, String, Integer> left, CoProcessFunction<Tuple3<Boolean, String, Integer>, Supplier, Tuple4<Boolean, String, Integer, Integer>>.Context context, Collector<Tuple4<Boolean, String, Integer, Integer>> collector) throws Exception {
        Tuple3<Boolean, String, Integer> oldleft= leftState.value();

        if ((oldleft == null && left.f0) || (oldleft != null && !left.f0)) {
            leftState.update(left);
            for (Supplier supplier : supplierListState.get()) {
                boolean update = left.f0 && supplier.update;
                Tuple4<Boolean, String, Integer, Integer> tuple = Tuple4.of(update, left.f1, left.f2, supplier.suppKey);
                if (left.f0 || supplier.update) {
                    collector.collect(tuple);
                }
            }
        }
    }

    @Override
    public void processElement2(Supplier supplier, CoProcessFunction<Tuple3<Boolean, String, Integer>, Supplier, Tuple4<Boolean, String, Integer, Integer>>.Context context, Collector<Tuple4<Boolean, String, Integer, Integer>> collector) throws Exception {
        boolean canAddToList = supplier.update;
        if (!canAddToList) {
            int insertCount = 0;
            int deleteCount = 0;
            for (Supplier s : supplierListState.get()) {
                if (s.update) {
                    insertCount += 1;
                } else {
                    deleteCount += 1;
                }
            }
            if (insertCount > deleteCount) {
                canAddToList = true;
            }
        }
        if (!canAddToList) {
            return;
        }
        supplierListState.add(supplier);
        Tuple3<Boolean, String, Integer> left = leftState.value();
        if (left != null) {
            boolean update = left.f0 && supplier.update;
            Tuple4<Boolean, String, Integer, Integer>tuple = Tuple4.of(update, left.f1, left.f2, supplier.suppKey);
            if (left.f0 || supplier.update) {
                collector.collect(tuple);
            }
        }
    }
}