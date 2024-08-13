package com.tpch.function.process;

import com.tpch.domain.model.LineItem;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Input1: update, customer.custKey, customer.nationKey, order.orderKey
 * Input2: LineItem
 * OutPut: update, customer.nationKey, lineItem.extendedPrice, lineItem.discount, lineItem.suppKey
 */
public class CustomerAndOrderJoinLineItemFunction extends CoProcessFunction<
        Tuple4<Boolean, Integer, Integer, Integer>,
        LineItem,
        Tuple5<Boolean, Integer, Double, Double, Integer>> {

    private ValueState<Tuple4<Boolean, Integer, Integer, Integer>> customerAndOrderState;
    private ListState<LineItem> lineItemListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        customerAndOrderState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("customerAndOrderState",
                        Types.TUPLE(Types.BOOLEAN, Types.INT, Types.INT, Types.INT))
        );
        lineItemListState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("lineItemState", LineItem.class)
        );
    }

    @Override
    public void processElement1(Tuple4<Boolean, Integer, Integer, Integer> customerAndOrder, CoProcessFunction<Tuple4<Boolean, Integer, Integer, Integer>, LineItem, Tuple5<Boolean, Integer, Double, Double, Integer>>.Context context, Collector<Tuple5<Boolean, Integer, Double, Double, Integer>> collector) throws Exception {
        Tuple4<Boolean, Integer, Integer, Integer> oldLeft = customerAndOrderState.value();

        if ((oldLeft == null && customerAndOrder.f0) || (oldLeft != null && !customerAndOrder.f0)) {
            customerAndOrderState.update(customerAndOrder);
            for (LineItem lineItem : lineItemListState.get()) {
                boolean update = customerAndOrder.f0 && lineItem.update;
                Tuple5<Boolean, Integer, Double, Double, Integer> tuple = Tuple5.of(update, customerAndOrder.f1, lineItem.extendedPrice, lineItem.discount, lineItem.suppKey);
                if (customerAndOrder.f0 || lineItem.update) {
                    collector.collect(tuple);
                }
            }
        }
    }

    @Override
    public void processElement2(LineItem lineItem, CoProcessFunction<Tuple4<Boolean, Integer, Integer, Integer>, LineItem, Tuple5<Boolean, Integer, Double, Double, Integer>>.Context context, Collector<Tuple5<Boolean, Integer, Double, Double, Integer>> collector) throws Exception {
        boolean canAddToList = lineItem.update;
        if (!canAddToList) {
            int insertCount = 0;
            int deleteCount = 0;
            for (LineItem li : lineItemListState.get()) {
                if (li.update) {
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
        lineItemListState.add(lineItem);
        Tuple4<Boolean, Integer, Integer, Integer> customerAndOrder = customerAndOrderState.value();
        if (customerAndOrder != null) {
            boolean update = customerAndOrder.f0 && lineItem.update;
            Tuple5<Boolean, Integer, Double, Double, Integer> tuple = Tuple5.of(update, customerAndOrder.f1, lineItem.extendedPrice, lineItem.discount, lineItem.suppKey);
            if (customerAndOrder.f0 || lineItem.update) {
                collector.collect(tuple);
            }
        }
    }
}