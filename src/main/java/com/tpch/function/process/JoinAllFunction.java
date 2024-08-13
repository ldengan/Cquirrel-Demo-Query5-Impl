package com.tpch.function.process;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Input1: update, nation.name, nation.nationKey, supplier.suppKey
 * --- SupplierJoinNationAndRegionFunction: Tuple4<Boolean, String, Integer, Integer>
 * Input2: update, customer.nationKey, lineItem.extendedPrice, lineItem.discount, lineItem.suppKey
 * --- CustomerAndOrderJoinLineItemFunction: Tuple5<Boolean, Integer, Double, Double, Integer>
 * OutPut: update,  nation.name, supplier.suppKey, lineItem.extendedPrice, lineItem.discount, lineItem.suppKey
 * --- Tuple6<Boolean, String, Integer, Double, Double, Integer>
 */
public class JoinAllFunction extends CoProcessFunction<
        Tuple4<Boolean, String, Integer, Integer>,
        Tuple5<Boolean, Integer, Double, Double, Integer>,
        Tuple6<Boolean, String, Integer, Double, Double, Integer>> {

    private ListState<Tuple4<Boolean, String, Integer, Integer>> leftListState;
    private ListState<Tuple5<Boolean, Integer, Double, Double, Integer>> rightListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        leftListState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("leftList",
                        Types.TUPLE(Types.BOOLEAN, Types.STRING, Types.INT, Types.INT))
        );

        rightListState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("rightList",
                        Types.TUPLE(Types.BOOLEAN, Types.INT, Types.DOUBLE, Types.DOUBLE, Types.INT))
        );
    }

    @Override
    public void processElement1(Tuple4<Boolean, String, Integer, Integer> left, CoProcessFunction<Tuple4<Boolean, String, Integer, Integer>,
            Tuple5<Boolean, Integer, Double, Double, Integer>, Tuple6<Boolean, String, Integer, Double, Double, Integer>>.Context context,
                                Collector<Tuple6<Boolean, String, Integer, Double, Double, Integer>> collector) throws Exception {
        leftListState.add(left);
        for (Tuple5<Boolean, Integer, Double, Double, Integer> right : rightListState.get()) {
            boolean update = left.f0 && right.f0;
            if (left.f0 || right.f0) {
                Tuple6<Boolean, String, Integer, Double, Double, Integer> tuple = Tuple6.of(update, left.f1, left.f3, right.f2, right.f3, right.f4);
                collector.collect(tuple);
            }
        }
    }

    @Override
    public void processElement2(Tuple5<Boolean, Integer, Double, Double, Integer> right, CoProcessFunction<Tuple4<Boolean, String, Integer, Integer>, Tuple5<Boolean, Integer, Double, Double, Integer>,
            Tuple6<Boolean, String, Integer, Double, Double, Integer>>.Context context, Collector<Tuple6<Boolean, String, Integer, Double, Double, Integer>> collector) throws Exception {
        rightListState.add(right);
        for (Tuple4<Boolean, String, Integer, Integer> left : leftListState.get()) {
            boolean update = left.f0 && right.f0;
            if (left.f0 || right.f0) {
                Tuple6<Boolean, String, Integer, Double, Double, Integer> tuple = Tuple6.of(update, left.f1, left.f3, right.f2, right.f3, right.f4);
                collector.collect(tuple);
            }
        }
    }
}