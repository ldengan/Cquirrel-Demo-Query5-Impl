package com.tpch.function.process;

import com.tpch.domain.model.Customer;
import com.tpch.domain.model.Orders;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Input: Customer, Orders
 * OutPut: update, customer.custKey, customer.nationKey, order.orderKey，后面能用到
 */
public class CustomerJoinOrderFunction extends CoProcessFunction<Customer, Orders, Tuple4<Boolean, Integer, Integer, Integer>> {
    private ValueState<Customer> customerState;

    private ListState<Orders> orderListState;

    @Override
    public void open(Configuration parameters) throws Exception {
        customerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("customerState", Customer.class)
        );
        orderListState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("orderState", Orders.class)
        );
    }

    @Override
    public void processElement1(Customer customer, CoProcessFunction<Customer, Orders, Tuple4<Boolean, Integer, Integer, Integer>>.Context context, Collector<Tuple4<Boolean, Integer, Integer, Integer>> collector) throws Exception {
        Customer oldCustomer = customerState.value();

        if ((oldCustomer == null && customer.update) || (oldCustomer != null && !customer.update)) {
            customerState.update(customer);
            for (Orders order: orderListState.get()) {
                boolean update = customer.update && order.update;
                Tuple4<Boolean, Integer, Integer, Integer> tuple = Tuple4.of(update, customer.custKey, customer.nationKey, order.orderKey);
                if (customer.update || order.update) {
                    collector.collect(tuple);
                }
            }
        }
    }


    @Override
    public void processElement2(Orders orders, CoProcessFunction<Customer, Orders, Tuple4<Boolean, Integer, Integer, Integer>>.Context context, Collector<Tuple4<Boolean, Integer, Integer, Integer>> collector) throws Exception {
        boolean canAddToList = orders.update;
        if (!canAddToList) {
            int insertCount = 0;
            int deleteCount = 0;
            for (Orders o : orderListState.get()) {
                if (o.update) {
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
        orderListState.add(orders);
        Customer customer = customerState.value();
        if (customer != null) {
            boolean update = customer.update && orders.update;
            Tuple4<Boolean, Integer, Integer, Integer> tuple = Tuple4.of(update, customer.custKey, customer.nationKey, orders.orderKey);
            if (customer.update || orders.update) {
                collector.collect(tuple);
            }
        }
    }
}